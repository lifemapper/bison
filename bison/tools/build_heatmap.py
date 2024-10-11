"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""
from logging import INFO
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, COUNT_FIELDS, OCCURRENCE_STATUS, OCCURRENCE_STATUS_FLD, PROJECT,
    S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH, WORKFLOW_ROLE
)
from bison.common.log import Logger
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



# .............................................................................
def build_heatmap(s3, table_type, datestr, count_field):
    """Get a pandas dataframe from an S3 parquet table with occurrence counts by region.

    Args:
        s3 (bison.tools.aws_util.S3): authenticated boto3 client for S3 interactions.
        table_type (str): code from bison.common.constants.SUMMARY with predefined type
            of data to download, indicating type and contents.
        datestr (str): date of the current dataset, in YYYY_MM_DD format

    Returns:
        axis0_label (str): axis label used for rows in the matrix
        axis1_label (str): axis label used for columns in the matrix
        df (pandas.DataFrame): dataframe containing occurrence counts for the dimension

    Raises:
        Exception: on request for region x occurrence status heatmap.
    """
    _contents, _dim0, dim1, _datatype = SUMMARY.parse_table_type(table_type)
    if dim1 is not None:
        raise Exception(f"Occurrence status {dim1} is not yet supported for heatmaps")

    if count_field not in COUNT_FIELDS:
        raise Exception(f"Invalid count field {count_field}")

    tbl = SUMMARY.get_table(table_type, datestr=datestr)
    pqt_fname = f"{tbl['fname']}.parquet"
    axis0_label = tbl["key_fld"]
    axis1_label = count_field
    # Read stacked (record) data directly into DataFrame
    try:
        df = s3.get_dataframe_from_parquet(S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)
    except Exception as e:
        print(f"Failed to read s3 parquet {pqt_fname} to dataframe. ({e})")
        raise(e)

    # Remove unused count fields
    for fld in COUNT_FIELDS:
        if fld != count_field:
            df.drop(labels=[fld], axis=1)

    return (axis0_label, axis1_label, df)


# .............................................................................


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
# if __name__ == "__main__":
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
