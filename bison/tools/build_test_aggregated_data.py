"""Tools to compute dataset x species statistics from S3 data."""
from logging import INFO
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    PROJECT, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH, WORKFLOW_ROLE
)
from bison.common.log import Logger
from bison.common.util import get_current_datadate_str
from bison.spnet.sparse_matrix import SparseMatrix
from bison.spnet.summary_matrix import SummaryMatrix


# .............................................................................
def _get_extreme_val_and_attrs_for_column_from_stacked_data(
        stacked_df, filter_label, filter_value, attr_label, val_label, is_max=True):
    """Find the minimum or maximum value for rows where 'filter_label' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_label: column name for filtering.
        filter_value: column value for filtering.
        attr_label: column name of attribute to return.
        val_label: column name for attribute with min/max value.
        is_max (bool): flag indicating whether to get maximum (T) or minimum (F)

    Returns:
        target_val:  Minimum or maximum value for rows where
            'filter_label' = 'filter_value'.
        attr_vals: values for attr_label for rows with the minimum or maximum value.

    Raises:
        Exception: on min/max = 0.  Zeros should never be returned for min or max value.
    """
    # Create a dataframe of rows where column 'filter_label' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_label] == filter_value]
    # Find the min or max value for those rows
    if is_max is True:
        target_val = tmp_df[val_label].max()
    else:
        target_val = tmp_df[val_label].min()
        # There should be NO zeros in these aggregated records
    if target_val == 0:
        raise Exception(
            f"Found value 0 in column {val_label} for rows where "
            f"{filter_label} == {filter_value}")
    # Get the attribute(s) in the row(s) with the max value
    attrs_containing_max_df = tmp_df.loc[tmp_df[val_label] == target_val]
    attr_vals = [rec for rec in attrs_containing_max_df[attr_label]]
    return target_val, attr_vals


# .............................................................................
def _sum_stacked_data_vals_for_column(stacked_df, filter_label, filter_value, val_label):
    """Sum the values for rows where column 'filter_label' = 'filter_value'.

    Args:
        stacked_df: dataframe containing stacked data records
        filter_label: column name for filtering.
        filter_value: column value for filtering.
        val_label: column name for summation.

    Returns:
        tmp_df: dataframe containing only rows with a value of filter_value in column
            filter_label.
    """
    # Create a dataframe of rows where column 'filter_label' = 'filter_value'.
    tmp_df = stacked_df.loc[stacked_df[filter_label] == filter_value]
    # Sum the values for those rows
    count = tmp_df[val_label].sum()
    return count


# .............................................................................
def test_row_col_comparisons(agg_sparse_mtx, test_count=5):
    """Test row comparisons between 1 and all, and column comparisons between 1 and all.

    Args:
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        test_count (int): number of rows and columns to test.

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    y_vals = agg_sparse_mtx.get_random_labels(test_count, axis=0)
    x_vals = agg_sparse_mtx.get_random_labels(test_count, axis=1)
    for y in y_vals:
        row_comps = agg_sparse_mtx.compare_row_to_others(y)
        print("Row comparisons:", print_obj=row_comps)
    for x in x_vals:
        col_comps = agg_sparse_mtx.compare_column_to_others(x)
        print("Column comparisons:", print_obj=col_comps)


# .............................................................................
def _test_stacked_to_aggregate_sum(
        stk_df, stk_axis_col_label, stk_val_col_label, agg_sparse_mtx, agg_axis=0,
        test_count=5):
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

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    labels = agg_sparse_mtx.get_random_labels(test_count, axis=agg_axis)
    # Test stacked column totals against aggregate x columns
    for lbl in labels:
        stk_sum = _sum_stacked_data_vals_for_column(
            stk_df, stk_axis_col_label, lbl, stk_val_col_label)
        agg_sum = agg_sparse_mtx.sum_vector(lbl, axis=agg_axis)
        print(f"Test axis {agg_axis}: {lbl}")
        if stk_sum == agg_sum:
            print(
                f"  Total {stk_sum}: Stacked data for {stk_axis_col_label} == "
                f"aggregate data in axis {agg_axis}: {lbl}"
            )
        else:
            print(
                f"  !!! {stk_sum} != {agg_sum}: Stacked data for {stk_axis_col_label} "
                f"!= aggregate data in axis {agg_axis}: {lbl}"
            )
        print("")
    print("")


# .............................................................................
def _test_stacked_to_aggregate_extremes(
        stk_df, stk_col_label_for_axis0, stk_col_label_for_axis1, stk_col_label_for_val,
        agg_sparse_mtx, agg_axis=0, test_count=5, is_max=True):
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
        is_max (bool): flag indicating whether to test maximum (T) or minimum (F)

    Raises:
        IndexError: on failure to find row or column in matrix.

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

    # Get min/max of row (identified by filter_label, attr_label in axis 0)
    if agg_axis == 0:
        filter_label, attr_label = stk_col_label_for_axis0, stk_col_label_for_axis1
    # Get min/max of column (identified by label in axis 1)
    elif agg_axis == 1:
        filter_label, attr_label = stk_col_label_for_axis1, stk_col_label_for_axis0

    # Test dataset - get species with largest count and compare
    for lbl in labels:
        # Get stacked data results
        (stk_target_val,
         stk_attr_vals) = _get_extreme_val_and_attrs_for_column_from_stacked_data(
            stk_df, filter_label, lbl, attr_label, stk_col_label_for_val, is_max=is_max)
        # Get sparse matrix results
        try:
            # Get row/column (sparse array), and its index
            vector, vct_idx = agg_sparse_mtx.get_vector_from_label(lbl, axis=agg_axis)
        except IndexError:
            raise

        agg_target_val, agg_labels = agg_sparse_mtx.get_extreme_val_labels_for_vector(
            vector, axis=agg_axis, is_max=is_max)
        print(f"Test vector {lbl} on axis {agg_axis}")
        if stk_target_val == agg_target_val:
            print(f"  {extm} values equal {stk_target_val}")
            if set(stk_attr_vals) == set(agg_labels):
                print(
                    f"  {extm} value labels equal; len={len(stk_attr_vals)}")
            else:
                print(
                    f"  !!! {extm} value labels NOT equal; stacked labels "
                    f"{stk_attr_vals} != agg labels {agg_labels}"
                )
        else:
            print(
                f"!!! {extm} stacked value {stk_target_val} != "
                f"{agg_target_val} agg value")
        print("")
    print("")


# .............................................................................
def test_stacked_data(stk_df, axis0_label, axis1_label, val_label, agg_sparse_mtx):
    """Test consistency between stacked data and sparse matrix.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        axis0_label: column label in stacked dataframe to be used as the
            row (axis 0) labels of the axis in the aggregate sparse matrix.
        axis1_label: column label in stacked dataframe to be used as the
            column (axis 1) labels of the axis in the aggregate sparse matrix.
        val_label: column label in stacked dataframe for data to be used as
            value in the aggregate sparse matrix.
        agg_sparse_mtx (SparseMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values]
    """
    # Test raw counts
    for stk_lbl, axis in ((axis0_label, 0), (axis1_label, 1)):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        _test_stacked_to_aggregate_sum(
            stk_df, stk_lbl, val_label, agg_sparse_mtx, agg_axis=axis, test_count=5)

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            _test_stacked_to_aggregate_extremes(
                stk_df, axis0_label, axis1_label, val_label, agg_sparse_mtx,
                agg_axis=axis, test_count=5, is_max=is_max)


# .............................................................................
def read_stacked_data_records(s3, table_type, data_datestr):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dim.

    Args:
        s3 (bison.tools.aws_util.S3): authenticated boto3 client for S3 interactions.
        table_type (code from bison.common.constants.SUMMARY): predefined type of
            data indicating type and contents.
        data_datestr (str): date of the current dataset, in YYYY_MM_DD format

    Returns:
        agg_sparse_mtx (bison.spnet.sparse_matrix.SparseMatrix): sparse matrix
            containing data separated into 2 dimensions
    """
    # Analysis dimension (i.e. region) in rows/x/axis 1
    stk_tbl = SUMMARY.get_table(table_type, datestr=data_datestr)
    axis1_label = stk_tbl["key_fld"]
    val_label = stk_tbl["value_fld"]
    # Species (taxonKey + name) in columns/y/axis 0
    axis0_label = stk_tbl["species_fld"]
    pqt_fname = f"{stk_tbl['fname']}.parquet"
    # Read stacked (record) data directly into DataFrame
    stk_df = s3.get_dataframe_from_parquet(S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)

    return (axis0_label, axis1_label, val_label, stk_df)


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    """Main script creates a species-x-county_matrix from county-x-species_list."""
    datestr = get_current_datadate_str()
    overwrite = True
    dim1 = "county"
    dim2 = "species"
    # All lists are analysis_dim x species
    stacked_table_type = SUMMARY.get_table_type("list", dim1, dim2)
    # Create matrix with species as rows: species x analysis_dim
    mtx_table_type = SUMMARY.get_table_type("matrix", dim2, dim1)

    # .................................
    # Create a logger
    # .................................
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # Create logger with default INFO messages
    logger = Logger(
        script_name, log_path=TMP_PATH, log_console=True, log_level=INFO)

    # Get authenticated S3 client
    s3 = S3(PROJECT, WORKFLOW_ROLE)

    # .................................
    # Download stacked records from S3 and build sparse matrix.
    # Test, comparing contents of the two data structures.
    # .................................
    axis0_label, axis1_label, val_label, stk_df = \
        read_stacked_data_records(s3, stacked_table_type, datestr)

    # Create matrix from record data, then test consistency and upload.
    agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
        stk_df, axis0_label, axis1_label, val_label, mtx_table_type, datestr,
        logger=logger)

    test_stacked_data(stk_df, axis0_label, axis1_label, val_label, agg_sparse_mtx)

    # Save sparse matrix to S3 then clear
    out_filename = agg_sparse_mtx.compress_to_file(local_path=TMP_PATH)
    s3_sparse_data_path = s3.upload(out_filename, S3_BUCKET, S3_SUMMARY_DIR)
    agg_sparse_mtx = None

    # .................................
    # Download sparse matrix from S3 file; test contents to ensure consistency
    # .................................
    table = SUMMARY.get_table(mtx_table_type, datestr)
    zip_fname = f"{table['fname']}.zip"
    # Only download if file does not exist
    zip_filename = s3.download(
        S3_BUCKET, S3_SUMMARY_DIR, zip_fname, local_path=TMP_PATH, overwrite=overwrite)

    # Only extract if files do not exist
    sparse_coo, row_categ, col_categ, table_type, _datestr = \
        SparseMatrix.uncompress_zipped_data(
            zip_filename, local_path=TMP_PATH, overwrite=overwrite)

    # Create
    agg_sparse_mtx = SparseMatrix(
        sparse_coo, mtx_table_type, datestr, row_categ, col_categ,
        logger=logger)

    test_stacked_data(stk_df, axis0_label, axis1_label, val_label, agg_sparse_mtx)

    # .................................
    # Create a summary matrix for each dimension of sparse matrix and upload
    # .................................
    # dim1 = "county"
    # dim2 = "species"
    # axis0/rows
    # Species totals/non-zero count
    # Other dimension total/count of species, down axis 0, one value for every column
    otherdim_sum_mtx = SummaryMatrix.init_from_sparse_matrix(
        agg_sparse_mtx, axis=0, logger=logger)
    otherdim_sum_table_type = otherdim_sum_mtx.table_type
    otherdim_sum_filename = otherdim_sum_mtx.compress_to_file()
    s3.upload(otherdim_sum_filename, S3_BUCKET, S3_SUMMARY_DIR)

    # axis1/columns
    # Species total/count of other dimension, across axis 1, one val for every row
    species_sum_mtx = SummaryMatrix.init_from_sparse_matrix(
        agg_sparse_mtx, axis=1, logger=logger)
    species_sum_table_type = species_sum_mtx.table_type
    species_sum_filename = species_sum_mtx.compress_to_file()
    s3.upload(species_sum_filename, S3_BUCKET, S3_SUMMARY_DIR)

    # .................................
    # TODO: Test summary matrices against sparse matrix
    # .................................

    # .................................
    # Download summary matrix files and recreate 2 summary matrices to test for corruption
    # .................................
    # Other dimension summary
    odim_table = SUMMARY.get_table(otherdim_sum_table_type, datestr)
    odim_zip_fname = f"{odim_table['fname']}.zip"
    odim_zip_filename = s3.download(
        S3_BUCKET, S3_SUMMARY_DIR, odim_zip_fname, local_path=TMP_PATH,
        overwrite=overwrite)

    odim_dataframe, odim_meta_dict, tmp_table_type, datestr = \
        SummaryMatrix.uncompress_zipped_data(
            odim_zip_filename, local_path=TMP_PATH, overwrite=overwrite)

    # Species Summary
    spdim_table = SUMMARY.get_table(species_sum_table_type, datestr)
    spdim_zip_fname = f"{spdim_table['fname']}.zip"
    spdim_zip_filename = s3.download(
        S3_BUCKET, S3_SUMMARY_DIR, spdim_zip_fname, local_path=TMP_PATH,
        overwrite=overwrite)

    spdim_dataframe, spdim_meta_dict, spdim_table_type, datestr = \
        SummaryMatrix.uncompress_zipped_data(
            spdim_zip_filename, local_path=TMP_PATH, overwrite=overwrite)

    # .................................
    # TODO: Test summary matrices
    # .................................

    # .................................
    # TODO: Test dataset lookup for random labels in sparse matrix
    # .................................

"""
from logging import INFO
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    PROJECT, TMP_PATH, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, WORKFLOW_USER)
from bison.common.log import Logger
from bison.common.util import get_current_datadate_str
from bison.spnet.sparse_matrix import SparseMatrix
from bison.spnet.summary_matrix import SummaryMatrix

from bison.tools.build_test_aggregated_data import *
from bison.common.constants import *
import boto3

datestr = "2024_09_01"
overwrite = True
dim1 = "county"
dim2 = "species"

stacked_table_type = SUMMARY.get_table_type("list", dim1, dim2)
mtx_table_type = SUMMARY.get_table_type("matrix", dim2, dim1)

logger = Logger(
    "test", log_path=TMP_PATH, log_console=True, log_level=INFO)

s3 = S3(PROJECT, WORKFLOW_ROLE)

# .................................
# Download stacked records from S3 and build sparse matrix.
# Test, comparing contents of the two data structures.
# .................................
axis0_label, axis1_label, val_label, stk_df = \
    read_stacked_data_records(s3, stacked_table_type, datestr)

stacked_df, y_fld, x_fld, val_fld, table_type, data_datestr = (
    stk_df, axis0_label, axis1_label, val_label, mtx_table_type, datestr
)
# Create matrix from record data, then test consistency and upload.
agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
    stk_df, axis0_label, axis1_label, val_label, mtx_table_type, datestr,
    logger=logger)

test_stacked_data(stk_df, axis0_label, axis1_label, val_label, agg_sparse_mtx)

# .................................
# Must upload sparse matrix to S3 from an EC2 instance with permission to S3
# For local testing, do:
#      aws s3 cp local_file s3://bucket/folder
# .................................

listing = s3.list(S3_BUCKET, S3_SUMMARY_DIR)
if out_filename in listing:
    print("Yay")
    agg_sparse_mtx = None


# .................................
# Download sparse matrix from S3 file; test contents to ensure consistency
# .................................
table = SUMMARY.get_table(mtx_table_type, datestr)
zip_fname = f"{table['fname']}.zip"
# Only download if file does not exist
zip_filename = s3.download(
    S3_BUCKET, S3_SUMMARY_DIR, zip_fname, local_path=TMP_PATH, overwrite=overwrite)

# Only extract if files do not exist
sparse_coo, row_categ, col_categ, table_type, _datestr = \
    SparseMatrix.uncompress_zipped_data(
        zip_filename, local_path=TMP_PATH, overwrite=overwrite)

# Create
agg_sparse_mtx = SparseMatrix(
    sparse_coo, mtx_table_type, datestr, row_categ, col_categ,
    logger=logger)

test_stacked_data(stk_df, axis0_label, axis1_label, val_label, agg_sparse_mtx)

otherdim_sum_mtx = SummaryMatrix.init_from_sparse_matrix(
    agg_sparse_mtx, axis=0, logger=logger)
otherdim_sum_table_type = otherdim_sum_mtx.table_type
otherdim_sum_filename = otherdim_sum_mtx.compress_to_file()

# axis1/columns
# Species total/count of other dimension, across axis 1, one val for every row
species_sum_mtx = SummaryMatrix.init_from_sparse_matrix(
    agg_sparse_mtx, axis=1, logger=logger)
species_sum_table_type = species_sum_mtx.table_type
species_sum_filename = species_sum_mtx.compress_to_file()

# .................................
# Must upload sparse matrix to S3 from an EC2 instance with permission to S3
# For local testing, do:
#      aws s3 cp local_file s3://bucket/folder
# .................................
# s3://bison-321942852011-us-east-1/summary/county-x-species_summary_2024_09_01.zip

# .................................
# Download summary matrix files and recreate 2 summary matrices to test for corruption
# .................................
# Other dimension summary
odim_table = SUMMARY.get_table(otherdim_sum_table_type, datestr)
odim_zip_fname = f"{odim_table['fname']}.zip"
odim_zip_filename = s3.download(
    S3_BUCKET, S3_SUMMARY_DIR, odim_zip_fname, local_path=TMP_PATH,
    overwrite=overwrite)

bucket, bucket_path, filename, local_path = (
    S3_BUCKET, S3_SUMMARY_DIR, odim_zip_fname, TMP_PATH)

odim_dataframe, odim_meta_dict, tmp_table_type, datestr = \
    SummaryMatrix.uncompress_zipped_data(
        odim_zip_filename, local_path=TMP_PATH, overwrite=overwrite)

# Species Summary
spdim_table = SUMMARY.get_table(species_sum_table_type, datestr)
spdim_zip_fname = f"{spdim_table['fname']}.zip"
spdim_zip_filename = s3.download(
    S3_BUCKET, S3_SUMMARY_DIR, spdim_zip_fname, local_path=TMP_PATH,
    overwrite=overwrite)

spdim_dataframe, spdim_meta_dict, spdim_table_type, datestr = \
    SummaryMatrix.uncompress_zipped_data(
        spdim_zip_filename, local_path=TMP_PATH, overwrite=overwrite)


"""
