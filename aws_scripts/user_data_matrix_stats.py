import boto3
from botocore.exceptions import ClientError
import datetime as DT
import io
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas
# import pyarrow.parquet as pq
# import s3fs


DOWNLOAD_NAME = "0098682-230530130749713"
BUCKET = "bison-321942852011-us-east-1"
DATA_PATH = "out_data"
LOG_PATH = "log"

species_county_list_fname = "county_lists_000.parquet"
# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

bison_bucket = "s3://bison-321942852011-us-east-1/"
data_catalog = "bison-metadata"
county_dataname = "county_lists_000.parquet"
output_dataname = "heatmatrix.parquet"
n = DT.datetime.now()
datastr = f"{n.year}-{n.month}-01"


# ----------------------------------------------------
def get_logger(log_name, log_dir=None, log_level=logging.INFO):
    filename = f"{log_name}.log"
    if log_dir is not None:
        filename = os.path.join(log_dir, f"{filename}")
        os.makedirs(log_dir, exist_ok=True)
    # create file handler
    handlers = []
    # for debugging in place
    handlers.append(logging.StreamHandler(stream=sys.stdout))
    # for saving onto S3
    handler = RotatingFileHandler(
        filename, mode="w", maxBytes=LOGFILE_MAX_BYTES, backupCount=10,
        encoding="utf-8"
    )
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    # Get logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    # Add handler to logger
    logger.addHandler(handler)
    logger.propagate = False
    return logger, filename


# ----------------------------------------------------
def download_from_s3(bucket, bucket_path, filename, logger, overwrite=True):
    local_path = os.getcwd()
    local_filename = os.path.join(local_path, filename)
    if os.path.exists(local_filename):
        if overwrite is True:
            os.remove(local_filename)
        else:
            logger.log(logging.INFO, f"{local_filename} already exists")
    else:
        s3_client = boto3.client("s3")
        try:
            s3_client
        except ClientError as e:
            logger.log(
                logging.ERROR,
                f"Failed to download {filename} from {bucket}/{bucket_path}, ({e})")
        else:
            logger.log(
                logging.INFO, f"Downloaded {filename} from S3 to {local_filename}")
    return local_filename


# # ----------------------------------------------------
# def read_s3_parquet_to_pandas_alt(bucket, bucket_path, filename, logger, overwrite=True):
#     s3 = s3fs.S3FileSystem()
#     s3_filename = f"s3://{bucket}/{bucket_path}/{filename}"
#     pandas_df = (pq.ParquetDataset(
#         s3_filename, filesystem=s3).read_pandas().to_pandas())
#     return pandas_df


# ----------------------------------------------------
def read_s3_parquet_to_pandas(
        bucket, bucket_path, filename, logger, s3_client=None, **args):
    """ Read a parquet file from a folder on S3 into a pandas DataFrame.

    Args:
        bucket: Bucket identifier on S3.
        bucket_path: Folder path to the S3 parquet data.
        filename: Filename of parquet data to read from S3.
        logger (object): logger for saving relevant processing messages
        s3_client: object for interacting with Amazon S3.
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        pandas.DataFrame containing the tabular data.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    s3_key = f"{bucket_path}/{filename}"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    except ClientError as e:
        logger.log(logging.ERROR, f"Failed to get {bucket}/{s3_key} from S3, ({e})")
    else:
        logger.log(logging.INFO, f"Read {bucket}/{s3_key} from S3")

    dataframe = pandas.read_parquet(io.BytesIO(obj['Body'].read()), **args)
    return dataframe

# ----------------------------------------------------
def read_s3_multiple_parquets_to_pandas(
        bucket, bucket_path, logger, s3=None, s3_client=None, verbose=False, **args):
    """ Read multiple parquets from a folder on S3 into a pandas DataFrame.

    Args:
        bucket: Bucket identifier on S3.
        bucket_path: Parent folder path to the S3 parquet data.
        logger (object): logger for saving relevant processing messages
        s3: Connection to the S3 resource
        s3_client: object for interacting with Amazon S3.
        verbose: flag indicating whether to log verbose messages
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        pandas.DataFrame containing the tabular data.
    """
    if not bucket_path.endswith("/"):
        bucket_path = bucket_path + "/"
    if s3_client is None:
        s3_client = boto3.client("s3")
    if s3 is None:
        s3 = boto3.resource("s3")
    s3_keys = [
        item.key for item in s3.Bucket(bucket).objects.filter(Prefix=bucket_path) 
        if item.key.endswith(".parquet")]
    if not s3_keys:
        logger.log(logging.ERROR, f"No parquet found in {bucket} {bucket_path}")
    elif verbose:
        logger.log(logging.INFO, "Load parquets:")
        for p in s3_keys:
            logger.log(logging.INFO, f"   {p}")
    dfs = [read_s3_parquet_to_pandas(key, bucket=bucket, s3_client=s3_client, **args)
           for key in s3_keys]
    return pandas.concat(dfs, ignore_index=True)


# ----------------------------------------------------
def upload_to_s3(full_filename, s3_bucket, s3_bucket_path, logger):
    s3_filename = None
    s3_client = boto3.client("s3")
    obj_name = os.path.basename(full_filename)
    if s3_bucket_path:
        obj_name = f"{s3_bucket_path}/{obj_name}"
    try:
        response = s3_client.upload_file(full_filename, s3_bucket, obj_name)
    except ClientError as e:
        msg = f"Failed to upload {obj_name} to {s3_bucket}, ({e})"
        if logger is not None:
            logger.log(logging.ERROR, msg)
        else:
            print(f"Error: {msg}")
    else:
        s3_filename = f"s3://{s3_bucket}/{obj_name}"
        msg = f"Uploaded {s3_filename} to S3"
        if logger is not None:
            logger.log(logging.INFO, msg)
        else:
            print(f"INFO: {msg}")
    return s3_filename

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    n = DT.datetime.now()
    date_str = f"{n.year}-{n.month}-{n.day}"

    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(f"{script_name}_{date_str}")

    # Download
    s3_datafile = f"{DATA_PATH}/{species_county_list_fname}"
    filename = download_from_s3(species_county_list_fname, BUCKET, s3_datafile, logger)

    # Upload logfile to S3
    s3_log_filename = upload_to_s3(log_filename, BUCKET, LOG_PATH)
