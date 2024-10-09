"""Create and save species-x-dim matrix and summaries: species-x-dim, dim-x-species."""
from logging import INFO
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, PROJECT, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH, WORKFLOW_ROLE,
)
from bison.common.log import Logger
from bison.common.util import get_current_datadate_str
from bison.spnet.sparse_matrix import SparseMatrix
from bison.spnet.summary_matrix import SummaryMatrix


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


# .............................................................................
def create_sparse_matrix_from_records(
        s3, stacked_table_type, mtx_table_type, datestr, logger=None):
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

    return agg_sparse_mtx


# .............................................................................
def save_matrix(mtx, local_path, bucket, bucket_path):
    # Compress and save matrix to S3
    out_filename = mtx.compress_to_file(local_path=local_path)
    s3_sparse_data_path = s3.upload(out_filename, bucket, bucket_path)
    return s3_sparse_data_path


# .............................................................................
def create_summary_matrix(agg_sparse_mtx, axis):
    otherdim_sum_mtx = SummaryMatrix.init_from_sparse_matrix(
        agg_sparse_mtx, axis=0, logger=logger)
    otherdim_sum_table_type = otherdim_sum_mtx.table_type
    otherdim_sum_filename = otherdim_sum_mtx.compress_to_file()
    s3.upload(otherdim_sum_filename, S3_BUCKET, S3_SUMMARY_DIR)

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    datestr = get_current_datadate_str()
    species_dim = "species"

    # .................................
    # Create a logger
    # .................................
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # Create logger that also prints to console (for AWS CloudWatch)
    logger = Logger(
        script_name, log_path=TMP_PATH, log_console=True, log_level=INFO)

    # Get authenticated S3 client
    s3 = S3(PROJECT, WORKFLOW_ROLE)

    for other_dim in ANALYSIS_DIM.analysis_code():
        # All lists are analysis_dim x species
        stacked_table_type = SUMMARY.get_table_type(
            "list", other_dim, species_dim)
        # Create matrix with species as rows (axis 0): species x analysis_dim
        mtx_table_type = SUMMARY.get_table_type(
            "matrix", species_dim, other_dim)

        # .................................
        # Download stacked records from S3, build sparse matrix, save to S3.
        # .................................
        agg_sparse_mtx = create_sparse_matrix_from_records(
            s3, stacked_table_type, mtx_table_type, datestr, logger=logger)
        logger.log(
            f"Built aggregated sparse matrix {mtx_table_type} from stacked data "
            f"records in {stacked_table_type}", refname=script_name
        )

        # Save sparse matrix to S3
        s3_sparse_datapath = save_matrix(
            agg_sparse_mtx, TMP_PATH, S3_BUCKET, S3_SUMMARY_DIR)
        logger.log(
            f"Saved {mtx_table_type} to {s3_sparse_datapath}", refname=script_name
        )

        # .................................
        # Create a summary matrix for both dimensions of sparse matrix and save to S3
        # .................................
        # axis0/rows
        # Other dimension total/count of species, down axis 0, one val for every column
        dimsum_mtx = SummaryMatrix.init_from_sparse_matrix(
            agg_sparse_mtx, axis=0, logger=logger)
        logger.log(
            f"Built summary matrix {dimsum_mtx.table_type}", refname=script_name
        )

        # Save to S3
        s3_dimsum_datapath = save_matrix(
            dimsum_mtx, TMP_PATH, S3_BUCKET, S3_SUMMARY_DIR)
        logger.log(
            f"Saved summary matrix to {s3_dimsum_datapath}", refname=script_name
        )
        # axis1/columns
        # Species total/count of other dimension, across axis 1, one val for every row
        speciessum_mtx = SummaryMatrix.init_from_sparse_matrix(
            agg_sparse_mtx, axis=1, logger=logger)
        logger.log(
            f"Built summary matrix {speciessum_mtx.table_type}",
            refname=script_name
        )

        s3_speciessum_datapath = save_matrix(
            speciessum_mtx, TMP_PATH, S3_BUCKET, S3_SUMMARY_DIR)
        logger.log(
            f"Saved summary matrix to {s3_speciessum_datapath}",
            refname=script_name
        )
