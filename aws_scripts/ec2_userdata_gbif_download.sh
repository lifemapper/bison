#!/bin/bash
# This is the user data script to be executed on an EC2 instance.

sudo su -
apt-get update -y
apt-get upgrade -y
apt-get install -y awscli
apt-get install -y python3-pip

aws configure set default.region us-east-1 && \
aws configure set default.output json

pip3 install boto3 pandas pyarrow requests

cat <<EOF > process_data_on_ec2.py
import boto3
from botocore.exceptions import ClientError
import csv
import datetime as DT
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas
import requests
import zipfile

DOWNLOAD_NAME = "0098682-230530130749713"
BUCKET = "bison-321942852011-us-east-1"
DATA_PATH = "orig_data"
LOG_PATH = "log"
GBIF_BASE_URL = "https://api.gbif.org/v1/occurrence/download/request/"
GBIF_OCC_FNAME = "occurrence.txt"

# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5


# ----------------------------------------------------
def get_logger(log_directory, log_name, log_level=logging.INFO):
    filename = f"{log_name}.log"
    if log_directory is not None:
        filename = os.path.join(log_directory, f"{filename}")
        os.makedirs(log_directory, exist_ok=True)
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
def download_from_gbif(gbif_basename, logger):
    local_path = os.getcwd()
    zfname = f"{gbif_basename}.zip"
    zip_filename = os.path.join(local_path, f"{zfname}")
    if os.path.exists(zip_filename):
        logger.log(logging.INFO, f"{zip_filename} already exists")
    else:
        r = requests.get(f"{GBIF_BASE_URL}{zfname}", stream=True)
        with open(f"{zip_filename}", "wb") as zfile:
            for chunk in r.iter_content(chunk_size=1024):
                # write one chunk at a time to zipfile
                if chunk:
                    zfile.write(chunk)
        if not os.path.exists(zip_filename):
            logger.log(logging.ERROR, f"Unable to download {zip_filename}")
            zip_filename = None
    return zip_filename

# ----------------------------------------------------
def get_date_str():
    n = DT.datetime.now()
    date_str = os.path.join(local_path, f"{n.year}-{n.month:02d}-{n.day:02d}")
    return date_str


# ----------------------------------------------------
def extract_occurrences_from_dwca(zip_filename, date_str, logger):
    local_path = os.path.dirname(zip_filename)
    new_filename = os.path.join(local_path, f"gbif_{date_str}.csv")
    if os.path.exists(new_filename):
        logger.log(logging.INFO, f"{new_filename} already exists")
    else:
        try:
            with zipfile.ZipFile(zip_filename, "r") as zfile:
                zfile.extract(GBIF_OCC_FNAME, path=local_path)
        except Exception as e:
            logger.log(logging.ERROR, f"Failed to extract {GBIF_OCC_FNAME} from {zip_filename}, ({e})")
            new_filename = None
        else:
            # Check for success
            orig_filename = os.path.join(local_path, GBIF_OCC_FNAME)
            if os.path.exists(orig_filename):
                os.rename(orig_filename, new_filename)
                logger.log(logging.INFO, f"{new_filename} extracted from {zip_filename}")
            else:
                logger.log(logging.ERROR, f"Failed to find extracted {orig_filename}")
                new_filename = None
    return new_filename


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
        logger.log(logging.ERROR, f"Failed to upload {obj_name} to {s3_bucket}, ({e})")
    else:
        s3_filename = f"s3://{s3_bucket}/{obj_name}"
        logger.log(logging.INFO, f"Uploaded {s3_filename} to S3")
    return s3_filename


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    date_str = get_date_str()
    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(None, f"{script_name}_{date_str})

    # Download
    zip_filename = download_from_gbif(DOWNLOAD_NAME, logger)
    if zip_filename is not None:

        # Unzip
        csv_filename = extract_occurrences_from_dwca(zip_filename, date_str, logger)
        if csv_filename is not None:

            # Upload data to S3
            s3_filename = upload_to_s3(csv_filename, BUCKET, DATA_PATH, logger)
            logger.log(logging.INFO, f"Uploaded {s3_filename} to S3")

    # Upload logfile to S3
    logger = None
    s3_log_filename = upload_to_s3(log_filename, BUCKET, LOG_PATH, logger)
EOF

python3 process_data_on_ec2.py
