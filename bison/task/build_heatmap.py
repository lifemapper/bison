"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""

# from bison.common.aws_util import S3
import os

from bison.common.constants import (
    ANALYSIS_DIM, OCCURRENCE_COUNT_FLD, PROJECT,
    S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH, WORKFLOW_ROLE
)
from bison.common.log import Logger
from bison.common.aws_util import S3
from bison.common.util import get_current_datadate_str
from bison.spnet.summary_matrix import SummaryMatrix

"""
Note:
    The analysis dimension should be geospatial, and fully cover the landscape with no
        overlaps.  Each species/occurrence count applies to one and only one record in
        the analysis dimesion.
"""


# from lmpy.spatial.map import (
#     create_point_heatmap_vector, create_site_headers_from_extent,
#     rasterize_geospatial_matrix)
# .............................................................................
# .............................................................................
def download_dataframe(table_type, datestr, bucket, bucket_dir):
    """Download a table written by Redshift to S3 in parquet, return dataframe.

    Args:
        table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
        datestr (str): date string in format YYYY_MM_DD
        bucket (str): S3 bucket for project.
        bucket_dir (str): Folder in S3 bucket for datafile.

    Returns:
        df (pandas.DataFrame): dataframe containing Redshift "counts" table data.
    """
    tbl = SUMMARY.get_table(table_type, datestr=datestr)
    pqt_fname = f"{tbl['fname']}.parquet"
    # axis0_label = tbl["key_fld"]
    # axis1_label = count_field
    # Read stacked (record) data directly into DataFrame
    try:
        df = s3.get_dataframe_from_parquet(bucket, bucket_dir, pqt_fname)
    except Exception as e:
        print(f"Failed to read s3 parquet {pqt_fname} to dataframe. ({e})")
        raise(e)
    return df


# .............................................................................


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    datestr = get_current_datadate_str()
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # Create logger with default INFO messages
    logger = Logger(script_name, log_path="/tmp", log_console=True)
    # For upload to/download from S3
    s3 = S3(PROJECT, WORKFLOW_ROLE)

    # for count_field in (COUNT_FIELDS):
    count_field = OCCURRENCE_COUNT_FLD
    # for dim in ANALYSIS_DIM.analysis_code():
    dim0 = ANALYSIS_DIM.COUNTY["code"]
    # RIIS occurrence status not yet supported
    dim1 = None
    table_type = ANALYSIS_DIM.get_table_type("counts", dim0, dim1)

    df = download_dataframe(table_type, datestr, S3_BUCKET, S3_SUMMARY_DIR)
    # # Remove unused count fields
    # for fld in COUNT_FIELDS:
    #     if fld != count_field:
    #         df.drop(labels=[fld], axis=1)
    sum_mtx = SummaryMatrix(df, table_type, datestr, logger=logger)

"""
from bison.tools.build_heatmap import *
from bison.common.constants import *

datestr = get_current_datadate_str()
datestr = "2024_09_01"
species_dim = ANALYSIS_DIM.species_code

# # Initialize logger and S3 client
# script_name = os.path.splitext(os.path.basename(__file__))[0]
# # Create logger that also prints to console (for AWS CloudWatch)
# logger = Logger(
#     script_name, log_path=TMP_PATH, log_console=True, log_level=INFO)

# Get authenticated S3 client
s3 = S3(PROJECT, WORKFLOW_ROLE)

# Loop through regions with full, non-overlapping polygons
# for region_dim in [ANALYSIS_DIM.COUNTY["code"]]:
region_dim = ANALYSIS_DIM.COUNTY["code"]
datatype = "counts"

    # Occurrence status is 'riis', ex: county_counts and county-x-riis_counts
    # for dim2 in (None, OCCURRENCE_STATUS):
    # for dim2 in [None]:

# Ignore RIIS status for now, will need 2 row headers, region + riis
dim2 = None
counts_table_type = SUMMARY.get_table_type(datatype, region_dim, dim2)

# Build occurrence count heatmap
axis0_label, axis1_label, df = build_heatmap(
    counts_table_type, datestr, OCCURRENCE_COUNT_FLD)



# ##########################################################################
# Debug
# ##########################################################################
# ##########################################################################





    # Get the vector file from S3 for mapping features with values
    # Create a 2d matrix for regions, any order mtx[y][x] = region/value

"""
