"""Script to read RIIS records from a file, annotate them, then output to a file."""
import datetime as DT
from logging import INFO
import os

from bison.common.constants import (REGION, S3_BUCKET, S3_IN_DIR, S3_LOG_DIR)
from bison.common.log import Logger
from bison.common.util import get_today_str
from bison.common.aws_util import S3


# .............................................................................
def some_task(logger):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        logger: object for logging execution messages.

    Returns:
        annotated_filename: output file annotated with GBIF accepted taxa.
    """
    datestr = get_today_str()
    some_filename = f"/tmp/some_filename_{datestr}.txt"

    n = DT.datetime.now()
    logger.log(f"Time is {n.time().isoformat()} on {n.date().isoformat()}")
    msg = f"Executing {script_name} for {datestr} dataset"
    logger.log(msg, refname=script_name)
    with open(some_filename, "w") as outf:
        outf.write(msg)
    logger.log(f"Wrote message to {some_filename}", refname=script_name)

    return some_filename


# .............................................................................
if __name__ == '__main__':
    """Test a task to be run in docker on EC2 instance."""
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # Create logger with default INFO messages
    logger = Logger(
        script_name, log_path="/tmp", log_console=True, log_level=INFO)

    some_filename = some_task(logger)
    logger.log(f"Returned {some_filename}", refname=script_name)

    s3 = S3(region=REGION)
    uploaded_fname = s3.upload(
        some_filename, S3_BUCKET,
        f"{S3_IN_DIR}/{os.path.basename(some_filename)}")
    logger.log(
        f"Uploaded {some_filename} to {uploaded_fname}", refname=script_name)

    uploaded_logname = s3.upload(
        logger.filename, S3_BUCKET,
        f"{S3_LOG_DIR}/{os.path.basename(logger.filename)}")
    print(f"Uploaded {logger.filename} to {uploaded_logname}")

    n = DT.datetime.now()
    print(f"Exiting at {n.time().isoformat()} on {n.date().isoformat()}")
    print()
    exit(0)
