"""Test creation of species by site matrices, then summaries and statistics."""
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, REGION, SUMMARY, TMP_PATH, COUNT_FLD, TOTAL_FLD
)
from bison.common.log import logit
from bison.common.util import get_current_datadate_str
from bison.spnet.heatmap_matrix import HeatmapMatrix
from bison.spnet.pam_matrix import PAM
from bison.spnet.summary_matrix import SummaryMatrix
from bison.task.calc_stats import (create_heatmap_from_records)

"""
Note:
    The analysis dimension should be geospatial, and fully cover the landscape with no
        overlaps.  Each species/occurrence count applies to one and only one record in
        the analysis dimension.
"""


# ...............................................
def test_stacked_vs_heatmap(stack_df, heatmap, test_count=5, logger=None):
    """Test values in stacked dataframe against those in sparse matrix for consistency.

    Args:
        stack_df: dataframe containing stacked data records
        heatmap (HeatmapMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Returns:
        success (bool): flag indicating success of all or failure of any tests.
    """
    success = True

    # Test raw counts
    for axis in (0, 1):
        # Test stacked column used for axis 0/1 against sparse matrix axis 0/1
        this_success = _test_stacked_to_aggregate_sum(
            stack_df, heatmap, axis=axis, test_count=test_count, logger=logger)
        success = success and this_success

    # Test min/max values for rows/columns
    for is_max in (False, True):
        for axis in (0, 1):
            this_success = _test_stacked_to_aggregate_extremes(
                stack_df, heatmap, axis=axis, test_count=test_count, is_max=is_max,
                logger=logger
            )
            success = success and this_success

    return success


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
def _sum_stacked_data_vals_for_column(stacked_df, filter_fld, filter_value, val_fld):
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
def _test_row_col_comparisons(heatmap, test_count, logger):
    """Test row comparisons between 1 and all, and column comparisons between 1 and all.

    Args:
        heatmap (HeatmapMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    y_vals = heatmap.get_random_labels(test_count, axis=0)
    x_vals = heatmap.get_random_labels(test_count, axis=1)
    for y in y_vals:
        row_comps = heatmap.compare_row_to_others(y)
        logit("Row comparisons:", logger=logger, print_obj=row_comps)
    for x in x_vals:
        col_comps = heatmap.compare_column_to_others(x)
        logit("Column comparisons:", logger=logger, print_obj=col_comps)


# ...............................................
def test_heatmap_vs_summary(
        heatmap, summary_mtx_lst, test_count=5, logger=None):
    """Test for equality of sums and counts in summary and aggregated dataframes.

    Args:
        heatmap (bison.spnet.heatmap_matrix.HeatmapMatrix): object containing a
            2-D scipy.sparse.coo_array.
        summary_mtx_lst (list of bison.spnet.summary_matrix.SummaryMatrix): list of 2
            SummaryMatrices, each summarizing rows or columns of the heatmap
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Returns:
        success (bool): Flag indicating success of all or failure of any tests.

    Postcondition:
        Printed information for successful or failed tests.

    Note: The summary matrices must have been created from dimensions in the heatmap.
    """
    success = True
    for summ_mtx in summary_mtx_lst:
        # Count and total x_dim for each value in y_dim
        count_total_dim = summ_mtx.x_dimension
        count_total_code = count_total_dim["code"]
        # each_value_in_dim = summ_mtx.y_dimension
        if count_total_dim == ANALYSIS_DIM.SPECIES:
            axis = 0
        else:
            axis = 1
        summ_labels = heatmap.get_random_labels(test_count, axis=axis)
        logit("Total comparisons:", logger=logger)
        for lbl in summ_labels:
            summ_vals = summ_mtx.get_row_values(lbl)
            # Test totals
            sparse_sum = heatmap.sum_vector(lbl, axis=axis)
            if summ_vals[TOTAL_FLD] == sparse_sum:
                logit(
                    f"  Label {lbl}, Total {sparse_sum}: HeatmapMatrix == "
                    f"SummaryMatrix in {count_total_code} axis {axis}", logger=logger
                )
            else:
                logit(
                    f"  !!! Label {lbl}, HeatmapMatrix total {sparse_sum} != "
                    f"{summ_vals[TOTAL_FLD]} SummaryMatrix in {count_total_code} "
                    f"axis {axis}", logger=logger
                )
                success = False
            # Test counts
            sparse_count = heatmap.count_vector(lbl, axis=axis)
            if summ_vals[COUNT_FLD] == sparse_count:
                logit(
                    f"  Label {lbl}, Count {sparse_count}: HeatmapMatrix == "
                    f"SummaryMatrix in {count_total_code} axis {axis}", logger=logger
                )
            else:
                logit(
                    f"  !!! Label {lbl}, HeatmapMatrix count {sparse_count} != "
                    f"{summ_vals[COUNT_FLD]} SummaryMatrix in {count_total_code} "
                    f"axis {axis}", logger=logger
                )
                success = False
    return success


# ...............................................
def _test_stacked_to_aggregate_sum(
        stk_df, heatmap, axis=0, test_count=5, logger=None):
    """Test for equality of sums in stacked and aggregated dataframes.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        heatmap (HeatmapMatrix): object containing a scipy.sparse.coo_array
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
    val_fld = heatmap.input_val_fld
    if axis == 0:
        col_fld = heatmap.y_dimension["key_fld"]
    else:
        col_fld = heatmap.x_dimension["key_fld"]
    sparse_labels = heatmap.get_random_labels(test_count, axis=axis)
    # Test stacked column totals against aggregate x columns
    for sp_lbl in sparse_labels:
        stk_sum = _sum_stacked_data_vals_for_column(stk_df, col_fld, sp_lbl, val_fld)
        agg_sum = heatmap.sum_vector(sp_lbl, axis=axis)
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
def test_heatmap_vs_filtered_vs_pam(
        heatmap, min_count=3, test_count=5, logger=None):
    """Test for equality of sums in stacked and aggregated dataframes.

    Args:
        heatmap (HeatmapMatrix): object containing a scipy.sparse.coo_array
            with 3 columns from the stacked_df arranged as rows and columns with values
        min_count (int): minimum value to be included in filtered data or PAM.
        test_count (int): number of rows and columns to test.
        logger (object): logger for saving relevant processing messages

    Returns:
        success (bool): Flag indicating success of all or failure of any tests.

    Postcondition:
        Printed information for successful or failed tests.

    Note: The aggregate_df must have been created from the stacked_df.
    """
    success = True
    heatmap_flt = heatmap.filter(min_count=min_count)
    pam = PAM.init_from_heatmap(heatmap, min_count)

    axis = 0
    labels = heatmap_flt.get_random_labels(test_count, axis=axis)
    # Test stacked column totals against aggregate x columns
    for lbl in labels:
        sum_orig = heatmap.sum_vector_ge_than(lbl, min_count, axis=axis)
        sum_flt = heatmap_flt.sum_vector(lbl, axis=axis)
        count_orig = heatmap.count_vector_ge_than(lbl, min_count, axis=axis)
        count_flt = heatmap_flt.count_vector(lbl, axis=axis)
        count_pam = pam.count_vector(lbl, axis=axis)
        logit(f"Test axis {axis}: {lbl}", logger=logger)
        if sum_orig == sum_flt:
            logit(
                f"  Sum of vector {lbl} == {sum_orig}: original, filtered >= "
                f"{min_count}, axis {axis}", logger=logger
            )
        else:
            success = False
            logit(
                f"  !!! {sum_orig} != {sum_flt}: Sum of vector original != "
                f"filtered >= {min_count}, axis {axis}", logger=logger
            )
        if count_orig == count_flt == count_pam:
            logit(
                f"  Count of vector {lbl} == {count_orig}: original, filtered, "
                f"pam >= {min_count}, axis {axis}", logger=logger
            )
        else:
            success = False
            logit(
                f"  !!! {count_orig} != {count_flt} != {count_pam}: Count of "
                f"vector original != filtered != pam >= {min_count}, axis {axis}",
                logger=logger
            )
        logit("", logger=logger)
    logit("", logger=logger)
    return success


# ...............................................
def _test_stacked_to_aggregate_extremes(
        stk_df, heatmap, axis=0, test_count=5, logger=None, is_max=True):
    """Test min/max counts for attributes in the sparse matrix vs. the stacked data.

    Args:
        stk_df: dataframe of stacked data, containing records with columns of
            categorical values and counts.
        heatmap (HeatmapMatrix): object containing a scipy.sparse.coo_array
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
    sparse_labels = heatmap.get_random_labels(test_count, axis=axis)
    val_fld = heatmap.input_val_fld
    y_fld = heatmap.y_dimension["key_fld"]
    x_fld = heatmap.x_dimension["key_fld"]
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
            vector, vct_idx = heatmap.get_vector_from_label(sp_lbl, axis=axis)
        except Exception:
            raise
        agg_target_val, agg_labels = heatmap.get_extreme_val_labels_for_vector(
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


# .............................................................................
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
    # Create, test, save, uncompress, test a sparse matrix from stacked data
    # .................................
    stack_df, heatmap = create_heatmap_from_records(
        s3, stacked_data_table_type, mtx_table_type, datestr)

    success = test_stacked_vs_heatmap(stack_df, heatmap)
    if success is False:
        raise Exception(
            "Failed tests comparing matrix created from stacked data to stacked data"
        )

    # Compress locally, test filename construction
    heatmap_filename = heatmap.compress_to_file(local_path=TMP_PATH)
    # Create, test a sparse matrix from compressed file
    table = SUMMARY.get_table(mtx_table_type, datestr)
    zip_fname = SUMMARY.get_filename(mtx_table_type, datestr, is_compressed=True)
    local_filename = os.path.join(TMP_PATH, zip_fname)
    # Make sure filenames are created correctly
    if heatmap_filename != local_filename:
        raise Exception(
            f"Compressed filename {heatmap_filename} != table generated "
            f"filename {local_filename}")

    sparse_mtx2 = HeatmapMatrix.init_from_compressed_file(
        heatmap_filename, local_path=TMP_PATH, overwrite=True)

    success = test_stacked_vs_heatmap(stack_df, sparse_mtx2)
    if success is False:
        raise Exception(
            "Failed tests comparing matrix created from compressed file to stacked data"
        )

    # .................................
    # Create a summary matrix for each dimension of sparse matrix and test
    # .................................
    sp_sum_mtx = SummaryMatrix.init_from_heatmap(heatmap, axis=0)
    spsum_table_type = sp_sum_mtx.table_type
    sp_sum_filename = sp_sum_mtx.compress_to_file(local_path=TMP_PATH)

    od_sum_mtx = SummaryMatrix.init_from_heatmap(heatmap, axis=1)
    odsum_table_type = od_sum_mtx.table_type
    od_sum_filename = od_sum_mtx.compress_to_file(local_path=TMP_PATH)

    summary_mtx_lst = [sp_sum_mtx, od_sum_mtx]
    success = test_heatmap_vs_summary(heatmap, summary_mtx_lst)

    # .................................
    # Test filename construction
    # Species summary
    zip_fname = SUMMARY.get_filename(spsum_table_type, datestr, is_compressed=True)
    local_filename = os.path.join(TMP_PATH, zip_fname)
    # Make sure filenames are created correctly
    if sp_sum_filename != local_filename:
        raise Exception(
            f"Compressed filename {sp_sum_filename} != table generated "
            f"filename {local_filename}")

    # Other Dimension Summary
    zip_fname = SUMMARY.get_filename(spsum_table_type, datestr, is_compressed=True)
    local_filename = os.path.join(TMP_PATH, zip_fname)
    # Make sure filenames are created correctly
    if od_sum_filename != local_filename:
        raise Exception(
            f"Compressed filename {od_sum_filename} != table generated "
            f"filename {local_filename}")

    # Construct from compressed, then test
    sp_sum_mtx2 = \
        SummaryMatrix.init_from_compressed_file(
            sp_sum_filename, local_path=TMP_PATH, overwrite=True)

    od_sum_mtx2 = \
        SummaryMatrix.init_from_compressed_file(
            od_sum_filename, local_path=TMP_PATH, overwrite=True)

    summary_mtx_lst = [sp_sum_mtx2, od_sum_mtx2]
    success = test_heatmap_vs_summary(heatmap, summary_mtx_lst)

    # .................................
    # Create PAM from Heatmap
    # .................................
    min_count = 3
    test_count = 5
    print("Heatmap:")
    print(heatmap.dimensions)
    print(heatmap.shape)
    success = test_heatmap_vs_filtered_vs_pam(
        heatmap, min_count=min_count, test_count=test_count)
    print(f"Success is {success}")
