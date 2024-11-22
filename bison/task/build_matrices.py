"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, REGION, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH, COUNT_FLD,
    TOTAL_FLD
)
from bison.common.log import logit
from bison.common.util import get_current_datadate_str
from bison.spnet.heatmap_matrix import HeatmapMatrix
from bison.spnet.pam_matrix import PAM
from bison.spnet.summary_matrix import SummaryMatrix

"""
Note:
    The analysis dimension should be geospatial, and fully cover the landscape with no
        overlaps.  Each species/occurrence count applies to one and only one record in
        the analysis dimension.
"""


# .............................................................................
def create_heatmap_from_records(
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
        heatmap (bison.spnet.heatmap_matrix.HeatmapMatrix): sparse matrix
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
        heatmap = HeatmapMatrix.init_from_stacked_data(
            stk_df, y_fld, x_fld, val_fld, mtx_table_type, datestr)
    except Exception as e:
        logger.log(f"Failed to read {stacked_table_type} to sparse matrix. ({e})")
        raise(e)
    logit(f"Built {mtx_table_type} from stacked data.", logger=logger)

    return stk_df, heatmap


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
        heatmap (sppy.tools.s2n.heatmap_matrix.HeatmapMatrix): sparse matrix
            containing data separated into 2 dimensions
    """
    # Species in columns/x/axis1
    stacked_record_table = SUMMARY.get_table(stacked_data_table_type, datestr)
    pqt_fname = SUMMARY.get_filename(stacked_data_table_type, datestr)

    axis0_fld = stacked_record_table["key_fld"]
    axis1_fld = stacked_record_table["species_fld"]
    val_fld = stacked_record_table["value_fld"]

    # Read stacked (record) data directly into DataFrame
    stk_df = s3.get_dataframe_from_parquet(S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)
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
    # Build heatmap, upload
    # .................................
    stack_df, heatmap = create_heatmap_from_records(
        s3, stacked_data_table_type, mtx_table_type, datestr)

    # out_filename = heatmap.compress_to_file(local_path=TMP_PATH)
    # s3_mtx_key = f"{S3_SUMMARY_DIR}/{os.path.basename(out_filename)}"
    # s3.upload(out_filename, S3_BUCKET, s3_mtx_key, overwrite=True)
    #
    #
    # # .................................
    # # Create a summary matrix for each dimension of sparse matrix and upload
    # # .................................
    # sp_sum_mtx = SummaryMatrix.init_from_heatmap(heatmap, axis=0)
    # spsum_table_type = sp_sum_mtx.table_type
    # sp_sum_filename = sp_sum_mtx.compress_to_file()
    # s3_spsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(sp_sum_filename)}"
    # s3.upload(sp_sum_filename, S3_BUCKET, s3_spsum_key, overwrite=True)
    #
    # od_sum_mtx = SummaryMatrix.init_from_heatmap(heatmap, axis=1)
    # odsum_table_type = od_sum_mtx.table_type
    # od_sum_filename = od_sum_mtx.compress_to_file()
    # s3_odsum_key = f"{S3_SUMMARY_DIR}/{os.path.basename(od_sum_filename)}"
    # s3.upload(od_sum_filename, S3_BUCKET, s3_odsum_key, overwrite=True)

    # .................................
    # Create PAM from Heatmap
    # .................................
    min_count = 3
    pam = PAM.init_from_heatmap(heatmap, min_count)
    species_stats_mtx = pam.calc_species_stats()
    print(species_stats_mtx)

    # site_stats_mtx = pam.calc_site_stats()
    # print(site_stats_mtx)
    #
    # diversity_stats_mtx = pam.calc_diversity_stats()
    # print(diversity_stats_mtx)

"""
from bison.task.build_matrices import *
from bison.common.constants import *
from pprint import pp

local_path = TMP_PATH
overwrite = True
test_count = 5
datestr = get_current_datadate_str()
s3 = S3(region=REGION)
logger = None

dim_region = ANALYSIS_DIM.COUNTY["code"]
dim_species = ANALYSIS_DIM.species_code()
stacked_data_table_type = SUMMARY.get_table_type("list", dim_region, dim_species)
# Species are always columns (for PAM)
mtx_table_type = SUMMARY.get_table_type("matrix", dim_region, dim_species)


"""
