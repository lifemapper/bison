"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, REGION, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH
)
from bison.common.log import logit
from bison.common.util import get_current_datadate_str
from bison.spnet.sparse_matrix import SparseMatrix
from bison.spnet.summary_matrix import SummaryMatrix

"""
Note:
    The analysis dimension should be geospatial, and fully cover the landscape with no
        overlaps.  Each species/occurrence count applies to one and only one record in
        the analysis dimension.
"""


# .............................................................................
def create_sparse_matrix_from_records(
        s3, stacked_table_type, mtx_table_type, datestr, logger=None):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dim.

    Args:
        s3 (bison.tools.aws_util.S3): authenticated boto3 client for S3 interactions.
        stacked_table_type (str): code from bison.common.constants.SUMMARY with
            predefined type of data to download, indicating type and contents.
        mtx_table_type (code from bison.common.constants.SUMMARY): predefined type
            of data to create from the stacked data.
        datestr (str): date of the current dataset, in YYYY_MM_DD format
        logger (bison.common.log.Logger): for writing messages to file and console

    Returns:
        sparse_mtx (bison.spnet.sparse_matrix.SparseMatrix): sparse matrix
            containing data separated into 2 dimensions
        y_fld (str): column header from stacked input records containing values for
            sparse matrix row headers
        x_fld (str): column header from stacked input records containing values for
            sparse matrix column headers
        val_fld (str): column header from stacked input records containing values
            for sparse matrix cells

    Raises:
        Exception: on failure to read records from parquet into a Dataframe.
        Exception: on failure to convert Dataframe to a sparse matrix.
    """
    _, dim0, dim1, _ = SUMMARY.parse_table_type(mtx_table_type)
    # Download stacked records from S3 into a dataframe
    try:
        y_fld, x_fld, val_fld, stk_df = \
            _read_stacked_data_records(s3, stacked_table_type, datestr)
    except Exception as e:
        logit(
            f"Failed to read {stacked_table_type} to dataframe. ({e})",
            logger=logger
        )
        raise
    logit(f"Read stacked data {stacked_table_type}.", logger=logger)

    # Create matrix from record data, then test consistency and upload.
    try:
        sparse_mtx = SparseMatrix.init_from_stacked_data(
            stk_df, y_fld, x_fld, val_fld, mtx_table_type, datestr)
    except Exception as e:
        logger.log(f"Failed to read {stacked_table_type} to sparse matrix. ({e})")
        raise(e)
    logit(f"Built {mtx_table_type} from stacked data.", logger=logger)

    return stk_df, sparse_mtx


# ...............................................
def test_stacked_vs_matrix(stack_df, sparse_mtx, test_count=5):
    """Test values in stacked dataframe against those in sparse matrix for consistency.

    Args:
        stack_df: dataframe containing stacked data records
        sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values

    Returns:
        success (bool): flag indicating success of all or failure of any tests.
    """
    success = True

    # Test raw counts
    for axis in (0, 1):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        this_success = _test_stacked_to_aggregate_sum(
            stack_df, sparse_mtx, axis=axis, test_count=test_count)
        success = success and this_success

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            this_success = _test_stacked_to_aggregate_extremes(
                stack_df, sparse_mtx, axis=axis, test_count=test_count, is_max=is_max)
            success = success and this_success

    return success


# ...............................................
def test_sums(sum1_mtx, sum2_mtx):
    """Test values in stacked dataframe against those in sparse matrix for consistency.

    Args:
        sum1_mtx: (bison.spnet.summary_matrix.SummaryMatrix) object containing
            a summary of occurrence and species counts by stacked data records
        sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values

    Returns:
        success (bool): flag indicating success of all or failure of any tests.
    """


# ...............................................
def _get_extreme_val_and_attrs_for_column_from_stacked_data(
        stacked_df, filter_fld, filter_value, attr_fld, val_fld, is_max=True):
    """Find the minimum or maximum value for rows where 'filter_fld' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_fld: column name for filtering.
        filter_value: column value for filtering.
        attr_fld: column name of attribute to return.
        val_fld: column name for attribute with min/max value.
        is_max (bool): flag indicating whether to get maximum (T) or minimum (F)

    Returns:
        target_val:  Minimum or maximum value for rows where
            'filter_fld' = 'filter_value'.
        attr_vals: values for attr_fld for rows with the minimum or maximum value.

    Raises:
        Exception: on min/max = 0.  Zeros should never be returned for min or max value.
    """
    # Create a dataframe of rows where column 'filter_fld' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_fld] == filter_value]
    # Find the min or max value for those rows
    if is_max is True:
        target_val = tmp_df[val_fld].max()
    else:
        target_val = tmp_df[val_fld].min()
        # There should be NO zeros in these aggregated records
    if target_val == 0:
        raise Exception(
            f"Found value 0 in column {val_fld} for rows where "
            f"{filter_fld} == {filter_value}")
    # Get the attribute(s) in the row(s) with the max value
    attrs_containing_max_df = tmp_df.loc[tmp_df[val_fld] == target_val]
    attr_vals = [rec for rec in attrs_containing_max_df[attr_fld]]
    return target_val, attr_vals


# ...............................................
def sum_stacked_data_vals_for_column(stacked_df, filter_fld, filter_value, val_fld):
    """Sum the values for rows where column 'filter_fld' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_fld: column name for filtering.
        filter_value: column value for filtering.
        val_fld: column name for summation.

    Returns:
        tmp_df: dataframe containing only rows with a value of filter_value in column
            filter_fld.
    """
    # Create a dataframe of rows where column 'filter_fld' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_fld] == filter_value]
    # Sum the values for those rows
    count = tmp_df[val_fld].sum()
    return count


# ...............................................
def _test_row_col_comparisons(sparse_mtx, test_count, logger):
    """Test row comparisons between 1 and all, and column comparisons between 1 and all.

    Args:
        sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    y_vals = sparse_mtx.get_random_labels(test_count, axis=0)
    x_vals = sparse_mtx.get_random_labels(test_count, axis=1)
    for y in y_vals:
        row_comps = sparse_mtx.compare_row_to_others(y)
        logit("Row comparisons:", logger=logger, print_obj=row_comps)
    for x in x_vals:
        col_comps = sparse_mtx.compare_column_to_others(x)
        logit("Column comparisons:", logger=logger, print_obj=col_comps)


# ...............................................
def _test_stacked_to_aggregate_sum(
        stk_df, sparse_mtx, axis=0, test_count=5, logger=None):
    """Test for equality of sums in stacked and aggregated dataframes.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
        axis (int): Axis 0 (row) or 1 (column) that corresponds with the column
            label (stk_axis_col_label) in the original stacked data.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Returns:
        success (bool): Flag indicating success of all or failure of any tests.

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    success = True
    val_fld = sparse_mtx.input_val_fld
    if axis == 0:
        col_fld = sparse_mtx.y_dimension["key_fld"]
    else:
        col_fld = sparse_mtx.x_dimension["key_fld"]
    sparse_labels = sparse_mtx.get_random_labels(test_count, axis=axis)
    # Test stacked column totals against aggregate x columns
    for sp_lbl in sparse_labels:
        stk_sum = sum_stacked_data_vals_for_column(stk_df, col_fld, sp_lbl, val_fld)
        agg_sum = sparse_mtx.sum_vector(sp_lbl, axis=axis)
        logit(f"Test axis {axis}: {sp_lbl}", logger=logger)
        if stk_sum == agg_sum:
            logit(
                f"  Total {stk_sum}: Stacked data for {col_fld} "
                f"== aggregate data in axis {axis}: {sp_lbl}", logger=logger
            )
        else:
            success = False
            logit(
                f"  !!! {stk_sum} != {agg_sum}: Stacked data for {col_fld} "
                f"!= aggregate data in axis {axis}: {sp_lbl}", logger=logger
            )
        logit("", logger=logger)
    logit("", logger=logger)
    return success


# ...............................................
def _test_stacked_to_aggregate_extremes(
        stk_df, sparse_mtx, axis=0, test_count=5, logger=None, is_max=True):
    """Test min/max counts for attributes in the sparse matrix vs. the stacked data.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
        axis (int): Axis 0 (row) or 1 (column) that corresponds with the column
            label (stk_axis_col_label) in the original stacked data.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages
        is_max (bool): flag indicating whether to test maximum (T) or minimum (F)

    Returns:
        success (bool): Flag indicating success of all or failure of any tests.

    Raises:
        Exception: on label does not exist in axis.

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    success = True
    sparse_labels = sparse_mtx.get_random_labels(test_count, axis=axis)
    val_fld = sparse_mtx.input_val_fld
    y_fld = sparse_mtx.y_dimension["key_fld"]
    x_fld = sparse_mtx.x_dimension["key_fld"]
    # for logging
    if is_max is True:
        extm = "Max"
    else:
        extm = "Min"

    # Get min/max of row (identified by filter_fld, attr_fld in axis 0)
    if axis == 0:
        filter_fld, attr_fld = y_fld, x_fld
    # Get min/max of column (identified by label in axis 1)
    elif axis == 1:
        filter_fld, attr_fld = x_fld, y_fld

    # Test dataset - get species with largest count and compare
    for sp_lbl in sparse_labels:
        # Get stacked data results
        (stk_target_val,
         stk_attr_vals) = _get_extreme_val_and_attrs_for_column_from_stacked_data(
            stk_df, filter_fld, sp_lbl, attr_fld, val_fld, is_max=is_max)
        # Get sparse matrix results
        try:
            # Get row/column (sparse array), and its index
            vector, vct_idx = sparse_mtx.get_vector_from_label(sp_lbl, axis=axis)
        except Exception:
            raise
        agg_target_val, agg_labels = sparse_mtx.get_extreme_val_labels_for_vector(
            vector, axis=axis, is_max=is_max)
        logit(f"Test vector {sp_lbl} on axis {axis}", logger=logger)
        if stk_target_val == agg_target_val:
            logit(f"  {extm} values equal {stk_target_val}", logger=logger)
            if set(stk_attr_vals) == set(agg_labels):
                logit(
                    f"  {extm} value labels equal; len={len(stk_attr_vals)}",
                    logger=logger)
            else:
                success = False
                logit(
                    f"  !!! {extm} value labels NOT equal; stacked labels "
                    f"{stk_attr_vals} != agg labels {agg_labels}", logger=logger
                )
        else:
            logit(
                f"!!! {extm} stacked value {stk_target_val} != {agg_target_val} "
                f"agg value", logger=logger)
        logit("", logger=logger)
    logit("", logger=logger)
    return success


# ...............................................
def _read_stacked_data_records(s3, stacked_data_table_type, datestr):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dataset.

    Args:
        s3 (bison.aws_util.S3): client connection for reading/writing data to AWS S3.
        stacked_data_table_type (str): table type for parquet data containing records of
            species lists for another dimension (i.e. region) with occurrence and
            species counts.
        datestr (str): date of the current dataset, in YYYY_MM_DD format

    Returns:
        sparse_mtx (sppy.tools.s2n.sparse_matrix.SparseMatrix): sparse matrix
            containing data separated into 2 dimensions
    """
    # Species in columns/x/axis1
    stacked_record_table = SUMMARY.get_table(stacked_data_table_type, datestr)

    axis0_fld = stacked_record_table["key_fld"]
    axis1_fld = stacked_record_table["species_fld"]
    val_fld = stacked_record_table["value_fld"]

    pqt_fname = f"{stacked_record_table['fname']}.parquet"
    # Read stacked (record) data directly into DataFrame
    stk_df = s3.get_dataframe_from_parquet(
        S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)
    return (axis0_fld, axis1_fld, val_fld, stk_df)


# .............................................................................
# .............................................................................
def download_dataframe(s3, table_type, datestr, bucket, bucket_dir):
    """Download a table written by Redshift to S3 in parquet, return dataframe.

    Args:
        s3 (bison.aws_util.S3): client connection for reading/writing data to AWS S3.
        table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
        datestr (str): date string in format YYYY_MM_DD
        bucket (str): S3 bucket for project.
        bucket_dir (str): Folder in S3 bucket for datafile.

    Returns:
        df (pandas.DataFrame): dataframe containing Redshift "counts" table data.

    Raises:
        Exception: on failure to download data from S3.
    """
    tbl = SUMMARY.get_table(table_type, datestr=datestr)
    pqt_fname = f"{tbl['fname']}.parquet"
    # Read stacked (record) data directly into DataFrame
    try:
        df = s3.get_dataframe_from_parquet(bucket, bucket_dir, pqt_fname)
    except Exception as e:
        print(f"Failed to read s3 parquet {pqt_fname} to dataframe. ({e})")
        raise(e)
    return df


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    """Main script creates a SPECIES_DATASET_MATRIX from county/species list."""
    datestr = get_current_datadate_str()
    s3 = S3(region=REGION)
    logger = None

    dim_region = ANALYSIS_DIM.COUNTY["code"]
    dim_species = ANALYSIS_DIM.species_code()
    stacked_data_table_type = SUMMARY.get_table_type("list", dim_region, dim_species)
    # Species are always columns (for PAM)
    mtx_table_type = SUMMARY.get_table_type("matrix", dim_region, dim_species)

    # .................................
    # Create, test, save a sparse matrix from stacked data
    # .................................
    stack_df, sparse_mtx = create_sparse_matrix_from_records(
        s3, stacked_data_table_type, mtx_table_type, datestr)

    success = test_stacked_vs_matrix(stack_df, sparse_mtx)
    if success is False:
        raise Exception(
            "Failed tests comparing matrix created from stacked data to stacked data"
        )

    out_filename = sparse_mtx.compress_to_file(local_path=TMP_PATH)
    s3_mtx_key = f"{S3_SUMMARY_DIR}/{os.path.basename(out_filename)}"
    s3.upload(out_filename, S3_BUCKET, s3_mtx_key, overwrite=True)

    # .................................
    # Create, test a sparse matrix from saved file
    # .................................
    table = SUMMARY.get_table(mtx_table_type, datestr)
    zip_fname = f"{table['fname']}.zip"
    zip_filename = s3.download(
        S3_BUCKET, S3_SUMMARY_DIR, zip_fname, TMP_PATH, overwrite=True)

    sparse_mtx2 = SparseMatrix.init_from_compressed_file(
        zip_filename, local_path=TMP_PATH, overwrite=True)

    success = test_stacked_vs_matrix(stack_df, sparse_mtx2)
    if success is False:
        raise Exception(
            "Failed tests comparing matrix created from compressed file to stacked data"
        )

    # .................................
    # Create a summary matrix for each dimension of sparse matrix and upload
    # .................................
    sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(sparse_mtx, axis=0)
    spsum_table_type = sp_sum_mtx.table_type
    sp_sum_filename = sp_sum_mtx.compress_to_file()
    s3_spsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(sp_sum_filename)}"
    s3.upload(sp_sum_filename, S3_BUCKET, s3_spsum_key, overwrite=True)

    od_sum_mtx = SummaryMatrix.init_from_sparse_matrix(sparse_mtx, axis=1)
    odsum_table_type = od_sum_mtx.table_type
    od_sum_filename = od_sum_mtx.compress_to_file()
    s3_odsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(od_sum_filename)}"
    s3.upload(od_sum_filename, S3_BUCKET, s3_odsum_key, overwrite=True)

    # .................................
    # Download summary matrix files and recreate 2 summary matrices to test for corruption
    # .................................
    sp_table = SUMMARY.get_table(spsum_table_type, datestr=datestr)
    _, _, sp_zip_fname = SummaryMatrix.get_matrix_meta_zip_filenames(table)
    sp_zip_filename = s3.download(
        S3_BUCKET, S3_SUMMARY_DIR, sp_zip_fname, local_path=TMP_PATH, overwrite=True)
    sp_sum_mtx2 = \
        SummaryMatrix.init_from_compressed_file(
            sp_zip_filename, local_path=TMP_PATH, overwrite=True)

    # Other Dimension Summary
    od_table = SUMMARY.get_table(odsum_table_type, datestr=datestr)
    _, _, od_zip_fname = SummaryMatrix.get_matrix_meta_zip_filenames(od_table)
    od_zip_filename = s3.download(
        S3_BUCKET, S3_SUMMARY_DIR, od_zip_fname, local_path=TMP_PATH, overwrite=True)
    od_sum_mtx2 = \
        SummaryMatrix.init_from_compressed_file(
            od_zip_filename, local_path=TMP_PATH, overwrite=True)

    # # for count_field in (OCCURRENCE_COUNT_FLD, SPECIES_COUNT_FLD):
    # count_field = OCCURRENCE_COUNT_FLD
    # count_table_type = SUMMARY.get_table_type("counts", dim0, None)
    #
    # count_df = download_dataframe(count_table_type, datestr, S3_BUCKET, S3_SUMMARY_DIR)
    # sum_mtx = SummaryMatrix(count_df, count_table_type, datestr)
"""
from bison.task.build_matrices import *
from bison.common.constants import *
from pprint import pp
local_path = TMP_PATH
overwrite = True

datestr = get_current_datadate_str()
s3 = S3(region=REGION)
logger = None

dim_region = ANALYSIS_DIM.COUNTY["code"]
dim_species = ANALYSIS_DIM.species_code()
stacked_data_table_type = SUMMARY.get_table_type("list", dim_region, dim_species)
# Species are always columns (for PAM)
mtx_table_type = SUMMARY.get_table_type("matrix", dim_region, dim_species)

# .................................
# Create, test, save a sparse matrix from stacked data
# .................................
stack_df, sparse_mtx = create_sparse_matrix_from_records(
    s3, stacked_data_table_type, mtx_table_type, datestr)

success = test_stacked_vs_matrix(stack_df, sparse_mtx, test_count=3)
if success is False:
    raise Exception(
        "Failed tests comparing matrix created from stacked data to stacked data"
    )

out_filename = sparse_mtx.compress_to_file(local_path=TMP_PATH)
s3_mtx_key = f"{S3_SUMMARY_DIR}/{os.path.basename(out_filename)}"
uploaded_fname = s3.upload(out_filename, S3_BUCKET, s3_mtx_key, overwrite=True)

# .................................
# Create, test a sparse matrix from saved file
# .................................
table = SUMMARY.get_table(mtx_table_type, datestr)
zip_fname = f"{table['fname']}.zip"
zip_filename = s3.download(
    S3_BUCKET, S3_SUMMARY_DIR, zip_fname, TMP_PATH, overwrite=True)

sparse_mtx2 = SparseMatrix.init_from_compressed_file(
    zip_filename, local_path=TMP_PATH, overwrite=True)

success = test_stacked_vs_matrix(stack_df, sparse_mtx2)
if success is False:
    raise Exception(
        "Failed tests comparing matrix created from compressed file to stacked data"
    )

# .................................
# Create a summary matrix for each dimension of sparse matrix and upload
# .................................

# Fail with compress summary matrix to CSV: created as NPZ and contents are binary

sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(sparse_mtx, axis=0)
spsum_table_type = sp_sum_mtx.table_type
sp_sum_filename = sp_sum_mtx.compress_to_file()
s3_spsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(sp_sum_filename)}"
s3.upload(sp_sum_filename, S3_BUCKET, s3_spsum_key, overwrite=True)

"""
