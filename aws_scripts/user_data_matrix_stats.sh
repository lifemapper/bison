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
DATA_PATH = "out_data"
LOG_PATH = "log"

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





# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    n = DT.datetime.now()
    date_str = f"{n.year}-{n.month}-{n.day}")

    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(None, f"{script_name}_{date_str})

    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(None, f"{script_name}_{date_str})

EOF

python3 process_data_on_ec2.py
