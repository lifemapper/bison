"""Script to read RIIS records from a file, annotate them, then output to a file."""
from logging import INFO
import os

from bison.common.constants import (REGION, S3_BUCKET, S3_IN_DIR)
from bison.common.log import Logger
from bison.common.util import get_current_datadate_str
from bison.common.aws_util import S3

from bison.provider.constants import INPUT_RIIS_FILENAME
from bison.provider.riis_data import RIIS


# .............................................................................
def annotate_riis():
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Returns:
        annotated_filename: output file annotated with GBIF accepted taxa.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    # Create logger with default INFO messages
    logger = Logger(
        script_name, log_path="/tmp", log_console=True, log_level=INFO)

    datestr = get_current_datadate_str()
    annotated_filename = RIIS.get_annotated_riis_filename(INPUT_RIIS_FILENAME, datestr)

    nnsl = RIIS(INPUT_RIIS_FILENAME, logger=logger)
    print(f"initialized RIIS as {nnsl}")
    # Update species data
    # try:
    #     report = nnsl.resolve_riis_to_gbif_taxa(annotated_filename, overwrite=True)
    # except Exception as e:
    #     logger.log(
    #         f"Unexpected failure {e} in {script_name}", refname=script_name,
    #         log_level=ERROR)
    # else:
    #     logger.log(json.dumps(report))
    #     logger.log(
    #         f"Found {report[REPORT.SUMMARY][REPORT.RIIS_IDENTIFIER]} names, "
    #         f"{report[REPORT.SUMMARY][REPORT.TAXA_RESOLVED]} resolved, "
    #         f"{report[REPORT.SUMMARY][REPORT.RECORDS_UPDATED]} updated, "
    #         f"{report[REPORT.SUMMARY][REPORT.RECORDS_OUTPUT]} written "
    #         f"of total {report[REPORT.RIIS_IDENTIFIER]} from {INPUT_RIIS_FILENAME} "
    #         f"to {report[REPORT.OUTFILE]}.", refname=script_name)
    msg = f"Executing {script_name} for {datestr} dataset"
    logger.log(msg, refname=script_name)
    with open(annotated_filename, "w") as outf:
        outf.write(msg)

    return annotated_filename


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records."""
    annotated_filename = annotate_riis()
    s3 = S3(region=REGION)
    s3.upload(annotated_filename, S3_BUCKET, S3_IN_DIR)