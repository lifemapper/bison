"""Create and save species-x-dim matrix and summaries: species-x-dim, dim-x-species."""
from logging import INFO
import os

from bison.common.aws_util import S3
from bison.common.constants import (
    ANALYSIS_DIM, REGION, S3_BUCKET, S3_SUMMARY_DIR, SUMMARY, TMP_PATH)
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
        axis0_label (str): column label in this data to be used as rows in a matrix
        axis1_label (str): column label in this data to be used as columns in a matrix
        val_label (str): : column label in this data to be used as values in a matrix
        stk_df (pandas.DataFrame): dataframe containing stacked data records

    Raises:
        Exception: on failure to download a parquet dataset into a pandas Dataframe.
    """
    # Analysis dimension (i.e. region) in rows/x/axis 1
    stk_tbl = SUMMARY.get_table(table_type, datestr=data_datestr)
    axis1_label = stk_tbl["key_fld"]
    val_label = stk_tbl["value_fld"]
    # Species (taxonKey + name) in columns/y/axis 0
    axis0_label = stk_tbl["species_fld"]
    pqt_fname = f"{stk_tbl['fname']}.parquet"
    # Read stacked (record) data directly into DataFrame
    try:
        stk_df = s3.get_dataframe_from_parquet(S3_BUCKET, S3_SUMMARY_DIR, pqt_fname)
    except Exception as e:
        print(f"Failed to read s3 parquet {pqt_fname} to dataframe. ({e})")
        raise(e)

    return (axis0_label, axis1_label, val_label, stk_df)


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

    Raises:
        Exception: on failure to read records from parquet into a Dataframe.
        Exception: on failure to convert Dataframe to a sparse matrix.
    """
    # Download stacked records from S3 into a dataframe
    try:
        axis0_label, axis1_label, val_label, stk_df = \
            read_stacked_data_records(s3, stacked_table_type, datestr)
    except Exception as e:
        logger.log(f"Failed to read {stacked_table_type} to dataframe. ({e})")
        raise
    logger.log(f"Read stacked data {stacked_table_type}.", refname=script_name)

    # Create matrix from record data, then test consistency and upload.
    try:
        agg_sparse_mtx = SparseMatrix.init_from_stacked_data(
            stk_df, axis0_label, axis1_label, val_label, mtx_table_type, datestr,
            logger=logger)
    except Exception as e:
        logger.log(f"Failed to read {stacked_table_type} to sparse matrix. ({e})")
        raise(e)
    logger.log(
        f"Built {mtx_table_type} from stacked data.", refname=script_name)

    return agg_sparse_mtx


# .............................................................................
def save_matrix(s3, mtx, local_path, bucket, bucket_path, logger):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dim.

    Args:
        s3 (bison.tools.aws_util.S3): authenticated boto3 client for S3 interactions.
        mtx (bison.spnet._AggregateDataMatrix): public subclass of this to save.
        local_path (str): local path for compressing the matrix.
        bucket: name of the S3 bucket destination.
        bucket_path: the data destination inside the S3 bucket (without filename).
        logger (bison.common.log.Logger): for writing messages to file and console

    Returns:
        agg_sparse_mtx (bison.spnet.sparse_matrix.SparseMatrix): sparse matrix
            containing data separated into 2 dimensions

    Raises:
        Exception: on failure to upload datafile to S3.

    """
    # Compress and save matrix to S3
    out_filename = mtx.compress_to_file(local_path=local_path)
    out_basename = os.path.basename(out_filename)
    logger.log(
        f"Compressed {mtx.table_type} to {out_filename}", refname=script_name)
    try:
        s3_datapath = s3.upload(out_filename, bucket, f"{bucket_path}/{out_basename}")
    except Exception as e:
        logger.log(f"Failed to upload {out_filename} to S3. ({e})")
        raise (e)
    logger.log(f"Saved to {s3_datapath}", refname=script_name)
    return s3_datapath


# .............................................................................
def process_dimension(
        s3, species_dim, other_dim, local_path, bucket, bucket_path, logger):
    """Read stacked records from S3, aggregate into a sparse matrix of species x dim.

    Args:
        s3 (bison.tools.aws_util.S3): authenticated boto3 client for S3 interactions.
        species_dim (bison.common.constants.ANALYSIS_DIM.species_code): code for the
            species analysis dimension.
        other_dim (bison.common.constants.ANALYSIS_DIM.analysis_code): code for one of
            the (non-species) analysis dimensions.
        local_path (str): local path for compressing the matrix.
        bucket: name of the S3 bucket destination.
        bucket_path: the data destination inside the S3 bucket (without filename).
        logger (bison.common.log.Logger): for writing messages to file and console
    """
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
    # Save sparse matrix to S3
    save_matrix(s3, agg_sparse_mtx, local_path, bucket, bucket_path, logger)

    # .................................
    # Create a summary matrix for both dimensions of sparse matrix and save to S3
    # .................................
    # axis0/rows
    # Other dimension total/count of species, down axis 0, one val for every column
    dimsum_mtx = SummaryMatrix.init_from_sparse_matrix(
        agg_sparse_mtx, axis=0, logger=logger)
    logger.log(
        f"Built {dimsum_mtx.table_type} from {mtx_table_type}.",
        refname=script_name
    )
    save_matrix(s3, dimsum_mtx, local_path, bucket, bucket_path, logger)

    # axis1/columns
    # Species total/count of other dimension, across axis 1, one val for every row
    speciessum_mtx = SummaryMatrix.init_from_sparse_matrix(
        agg_sparse_mtx, axis=1, logger=logger)
    logger.log(
        f"Built {speciessum_mtx.table_type} from {mtx_table_type}.",
        refname=script_name
    )
    save_matrix(s3, speciessum_mtx, local_path, bucket, bucket_path, logger)


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
    s3 = S3(region=REGION)

    # Loop through analysis dimensions
    for other_dim in ANALYSIS_DIM.analysis_code():
        logger.log(
            f"Summarizing {other_dim} aggregations of {species_dim}.",
            refname=script_name
        )
        try:
            process_dimension(
                s3, species_dim, other_dim, TMP_PATH, S3_BUCKET, S3_SUMMARY_DIR, logger
            )
        except Exception as e:
            logger.log(
                f"Failed to complete summary analysis of {other_dim}. ({e})",
                refname=script_name
            )
            raise(e)
