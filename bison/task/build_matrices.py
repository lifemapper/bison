"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, OCCURRENCE_COUNT_FLD, REGION, S3_BUCKET, S3_SUMMARY_DIR,
    SPECIES_COUNT_FLD, SUMMARY, TMP_PATH
)
from bison.common.log import logit, Logger
from bison.common.util import get_current_datadate_str, get_today_str
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
        agg_sparse_mtx (bison.spnet.sparse_matrix.SparseMatrix): sparse matrix
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
    # Download stacked records from S3 into a dataframe
    try:
        y_fld, x_fld, val_fld, stk_df = \
            _read_stacked_data_records(s3, stacked_table_type, datestr)
    except Exception as e:
        logit(f"Failed to read {stacked_table_type} to dataframe. ({e})", logger=logger)
        raise
    logit(f"Read stacked data {stacked_table_type}.", logger=logger)

    # Create matrix from record data, then test consistency and upload.
    try:
        agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
            stk_df, y_fld, x_fld, val_fld, mtx_table_type, datestr,
            logger=logger)
    except Exception as e:
        logger.log(f"Failed to read {stacked_table_type} to sparse matrix. ({e})")
        raise(e)
    logit(f"Built {mtx_table_type} from stacked data.", logger=logger)

    return stk_df, agg_sparse_mtx


# ...............................................
def test_stacked_vs_matrix(stack_df, sparse_matrix):
    y_fld = sparse_matrix.input_y_fld
    x_fld = sparse_matrix.input_x_fld
    val_fld = sparse_matrix.input_val_fld
    # .................................
    # Test consistency between stacked data and sparse matrix
    # .................................
    # Test raw counts
    for stk_lbl, axis in ((y_fld, 0), (x_fld, 1)):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        _test_stacked_to_aggregate_sum(
            stack_df, stk_lbl, y_fld, sparse_matrix, agg_axis=axis, test_count=5)

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            _test_stacked_to_aggregate_extremes(
                stack_df, y_fld, x_fld, val_fld, sparse_matrix, agg_axis=axis,
                test_count=5, is_max=is_max)


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
def _test_row_col_comparisons(agg_sparse_mtx, test_count, logger):
    """Test row comparisons between 1 and all, and column comparisons between 1 and all.

    Args:
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    y_vals = agg_sparse_mtx.get_random_labels(test_count, axis=0)
    x_vals = agg_sparse_mtx.get_random_labels(test_count, axis=1)
    for y in y_vals:
        row_comps = agg_sparse_mtx.compare_row_to_others(y)
        logit("Row comparisons:", logger=logger, print_obj=row_comps)
    for x in x_vals:
        col_comps = agg_sparse_mtx.compare_column_to_others(x)
        logit("Column comparisons:", logger=logger, print_obj=col_comps)


# ...............................................
def _test_stacked_to_aggregate_sum(
        stk_df, stk_axis_col_label, stk_val_col_label, agg_sparse_mtx, agg_axis=0,
        test_count=5, logger=None):
    """Test for equality of sums in stacked and aggregated dataframes.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        stk_axis_col_label: column label in stacked dataframe to be used as the column
            labels of the axis in the aggregate sparse matrix.
        stk_val_col_label: column label in stacked dataframe for data to be used as
            value in the aggregate sparse matrix.
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
        agg_axis (int): Axis 0 (row) or 1 (column) that corresponds with the column
            label (stk_axis_col_label) in the original stacked data.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    labels = agg_sparse_mtx.get_random_labels(test_count, axis=agg_axis)
    # Test stacked column totals against aggregate x columns
    for lbl in labels:
        stk_sum = sum_stacked_data_vals_for_column(
            stk_df, stk_axis_col_label, lbl, stk_val_col_label)
        agg_sum = agg_sparse_mtx.sum_vector(lbl, axis=agg_axis)
        logit(f"Test axis {agg_axis}: {lbl}", logger=logger)
        if stk_sum == agg_sum:
            logit(
                f"  Total {stk_sum}: Stacked data for {stk_axis_col_label} "
                f"== aggregate data in axis {agg_axis}: {lbl}", logger=logger
            )
        else:
            logit(
                f"  !!! {stk_sum} != {agg_sum}: Stacked data for {stk_axis_col_label} "
                f"!= aggregate data in axis {agg_axis}: {lbl}", logger=logger
            )
        logit("", logger=logger)
    logit("", logger=logger)


# ...............................................
def _test_stacked_to_aggregate_extremes(
        stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val,
        agg_sparse_mtx, agg_axis=0, test_count=5, logger=None, is_max=True):
    """Test min/max counts for attributes in the sparse matrix vs. the stacked data.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        stk_col_label_for_axis0: column label in stacked dataframe to be used as the
            row (axis 0) labels of the axis in the aggregate sparse matrix.
        stk_col_label_for_axis1: column label in stacked dataframe to be used as the
            column (axis 1) labels of the axis in the aggregate sparse matrix.
        stk_col_label_for_val: column label in stacked dataframe for data to be used as
            value in the aggregate sparse matrix.
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
        agg_axis (int): Axis 0 (row) or 1 (column) that corresponds with the column
            label (stk_axis_col_label) in the original stacked data.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages
        is_max (bool): flag indicating whether to test maximum (T) or minimum (F)

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    labels = agg_sparse_mtx.get_random_labels(test_count, axis=agg_axis)
    # for logging
    if is_max is True:
        extm = "Max"
    else:
        extm = "Min"

    # Get min/max of row (identified by filter_fld, attr_fld in axis 0)
    if agg_axis == 0:
        filter_fld, attr_fld = stk_col_label_for_axis0, stk_col_label_for_axis1
    # Get min/max of column (identified by label in axis 1)
    elif agg_axis == 1:
        filter_fld, attr_fld = stk_col_label_for_axis1, stk_col_label_for_axis0

    # Test dataset - get species with largest count and compare
    for lbl in labels:
        # Get stacked data results
        (stk_target_val,
         stk_attr_vals) = _get_extreme_val_and_attrs_for_column_from_stacked_data(
            stk_df, filter_fld, lbl, attr_fld, stk_col_label_for_val, is_max=is_max)
        # Get sparse matrix results
        try:
            # Get row/column (sparse array), and its index
            vector, vct_idx = agg_sparse_mtx.get_vector_from_label(lbl, axis=agg_axis)
        except IndexError:
            raise
        agg_target_val, agg_labels = agg_sparse_mtx.get_extreme_val_labels_for_vector(
            vector, axis=agg_axis, is_max=is_max)
        logit(f"Test vector {lbl} on axis {agg_axis}", logger=logger)
        if stk_target_val == agg_target_val:
            logit(f"  {extm} values equal {stk_target_val}", logger=logger)
            if set(stk_attr_vals) == set(agg_labels):
                logit(
                    f"  {extm} value labels equal; len={len(stk_attr_vals)}",
                    logger=logger)
            else:
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
        agg_sparse_mtx (sppy.tools.s2n.sparse_matrix.SparseMatrix): sparse matrix
            containing data separated into 2 dimensions
    """
    # Species in columns/x/axis1
    stacked_record_table = SUMMARY.get_table(stacked_data_table_type, datestr)

    stk_col_label_for_axis0 = stacked_record_table["key_fld"]
    stk_col_label_for_axis1 = stacked_record_table["species_fld"]
    stk_col_label_for_val = stacked_record_table["value_fld"]

    pqt_fname = f"{stacked_record_table['fname']}.parquet"
    # Read stacked (record) data directly into DataFrame
    stk_df = s3.get_dataframe_from_parquet(
        S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)
    return (
        stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val, stk_df
    )


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
    # region/axis0/row x species/axis1/column
    mtx_table_type = SUMMARY.get_table_type("matrix", dim_region, dim_species)

    stack_df, agg_sparse_mtx = create_sparse_matrix_from_records(s3, stacked_data_table_type, mtx_table_type, datestr)
    test_stacked_vs_matrix(stack_df, agg_sparse_mtx)
    # # .................................
    # # Create SparseMatrix from stacked data records
    # # .................................
    # stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val, stk_df = \
    #     _read_stacked_data_records(s3, stacked_data_table_type, datestr)
    #
    # agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
    #     stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val,
    #     mtx_table_type, datestr)
    #
    # # .................................
    # # Test consistency between stacked data and sparse matrix
    # # .................................
    # # Test raw counts
    # for stk_lbl, axis in ((stk_col_label_for_axis0, 0), (stk_col_label_for_axis1, 1)):
    #     # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
    #     test_stacked_to_aggregate_sum(
    #         stk_df, stk_lbl, stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis,
    #         test_count=5, logger=logger)
    #
    # # Test min/max values for rows/columns
    # for is_max in (False, True):
    #     for axis in (0, 1):
    #         test_stacked_to_aggregate_extremes(
    #             stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1,
    #             stk_col_label_for_val, agg_sparse_mtx, agg_axis=axis, test_count=5,
    #             logger=logger, is_max=is_max)

    # .................................
    # Create a summary matrix for each dimension of sparse matrix and upload
    # .................................
    sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=0, logger=logger)
    spsum_table_type = sp_sum_mtx.table_type
    sp_sum_filename = sp_sum_mtx.compress_to_file()

    ds_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=1, logger=logger)
    dssum_table_type = ds_sum_mtx.table_type
    ds_sum_filename = ds_sum_mtx.compress_to_file()


    # # .................................
    # # Save sparse matrix to S3 then clear
    # # .................................
    # out_filename = agg_sparse_mtx.compress_to_file()
    # upload_to_s3(out_filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    # # Copy logfile to S3
    # upload_to_s3(logger.filename, PROJ_BUCKET, SUMMARY_FOLDER, REGION)
    # agg_sparse_mtx = None



    # # for count_field in (OCCURRENCE_COUNT_FLD, SPECIES_COUNT_FLD):
    # count_field = OCCURRENCE_COUNT_FLD
    # count_table_type = SUMMARY.get_table_type("counts", dim0, None)
    #
    # count_df = download_dataframe(count_table_type, datestr, S3_BUCKET, S3_SUMMARY_DIR)
    # sum_mtx = SummaryMatrix(count_df, count_table_type, datestr)

"""
from bison.task.build_matrices import *
from bison.common.constants import *

datestr = get_current_datadate_str()
datatype = "list"
dim_region = ANALYSIS_DIM.COUNTY["code"]
dim_species = ANALYSIS_DIM.species_code()
stacked_data_table_type = SUMMARY.get_table_type("list", dim_region, dim_species)
# species/axis0/row x region/axis1/column
mtx_table_type = SUMMARY.get_table_type("matrix", dim_species, dim_region) 

logger = None
s3 = S3(region=REGION)

# .................................
# Create a summary matrix for each dimension of sparse matrix and upload
# .................................
sp_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=0, logger=logger)
spsum_table_type = sp_sum_mtx.table_type
sp_sum_filename = sp_sum_mtx.com    

ds_sum_mtx = SummaryMatrix.init_from_sparse_matrix(agg_sparse_mtx, axis=1, logger=logger)
dssum_table_type = ds_sum_mtx.table_type
ds_sum_filename = ds_sum_mtx.compress_to_file()
"""
