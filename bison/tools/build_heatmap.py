"""Create a matrix of occurrence or species counts by (geospatial) analysis dimension."""

"""
Note: 
    The analysis dimension should be geospatial, and fully cover the landscape with no
        overlaps.  Each species/occurrence count applies to one and only one record in  
        the analysis dimesion.  
"""
from logging import INFO
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, PROJECT, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH, WORKFLOW_ROLE,
)
from bison.common.log import Logger
from bison.common.util import get_current_datadate_str
from bison.spnet.summary_matrix import SummaryMatrix

# from lmpy.spatial.map import (
#     create_point_heatmap_vector, create_site_headers_from_extent,
#     rasterize_geospatial_matrix)

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    datestr = get_current_datadate_str()
    species_dim = ANALYSIS_DIM.species_code

    # Initialize logger and S3 client
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # Create logger that also prints to console (for AWS CloudWatch)
    logger = Logger(
        script_name, log_path=TMP_PATH, log_console=True, log_level=INFO)

    # Get authenticated S3 client
    s3 = S3(PROJECT, WORKFLOW_ROLE)

    # Loop through regions with full, non-overlapping polygons
    for other_dim in [ANALYSIS_DIM.STATE["code"], ANALYSIS_DIM.COUNTY["code"]]:
        # Get the vector file from S3 for mapping features with values
        # Create a 2d matrix for regions, any order mtx[y][x] = region/value
        pass
