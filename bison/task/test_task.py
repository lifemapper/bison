"""Script to read RIIS records from a file, annotate them, then output to a file."""
from logging import INFO
import os

from bison.common.constants import (REGION, S3_BUCKET, S3_IN_DIR)
from bison.common.log import Logger
from bison.common.util import get_today_str
from bison.common.aws_util import S3


# .............................................................................
def some_task():
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Returns:
        annotated_filename: output file annotated with GBIF accepted taxa.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    # Create logger with default INFO messages
    logger = Logger(
        script_name, log_path="/tmp", log_console=True, log_level=INFO)

    datestr = get_today_str()
    some_filename = f"/tmp/some_filename_{datestr}.txt"

    msg = f"Executing {script_name} for {datestr} dataset"
    logger.log(msg, refname=script_name)
    with open(some_filename, "w") as outf:
        outf.write(msg)

    return some_filename


# .............................................................................
if __name__ == '__main__':
    """Test a task to be run in docker on EC2 instance."""
    some_filename = some_task()

    base_fname = os.path.basename(some_filename)
    s3_key = f"{S3_IN_DIR}/{base_fname}"

    s3 = S3(region=REGION)
    uploaded_fname = s3.upload(some_filename, S3_BUCKET, s3_key)
