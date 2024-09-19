"""Script to read RIIS records from a file, annotate them, then output to a file."""
import json
from logging import INFO, ERROR
import os

from bison.common.constants import (
    # PROJ_BUCKET, PROJ_INPUT_PATH, REGION,
    REPORT)
from bison.common.log import Logger
from bison.common.util import (
    BisonNameOp, get_today_str,
    # upload_to_s3
)

from bison.provider.constants import RIIS_FILENAME
from bison.provider.riis_data import RIIS

# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records."""
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    log_name = f"{script_name}_{get_today_str()}"
    log_filename = f"/tmp/{log_name}.log"

    # Create logger with default INFO messages
    logger = Logger(
        log_name, log_filename=log_filename, log_console=True, log_level=INFO)

    annotated_filename = BisonNameOp.get_annotated_riis_filename(RIIS_FILENAME)

    nnsl = RIIS(RIIS_FILENAME, logger=logger)
    # Update species data
    try:
        report = nnsl.resolve_riis_to_gbif_taxa(annotated_filename, overwrite=True)
    except Exception as e:
        logger.log(
            f"Unexpected failure {e} in {script_name}", refname=script_name,
            log_level=ERROR)
    else:
        logger.log(json.dumps(report))
        logger.log(
            f"Found {report[REPORT.SUMMARY][REPORT.RIIS_IDENTIFIER]} names, "
            f"{report[REPORT.SUMMARY][REPORT.TAXA_RESOLVED]} resolved, "
            f"{report[REPORT.SUMMARY][REPORT.RECORDS_UPDATED]} updated, "
            f"{report[REPORT.SUMMARY][REPORT.RECORDS_OUTPUT]} written "
            f"of total {report[REPORT.RIIS_IDENTIFIER]} from {RIIS_FILENAME} "
            f"to {report[REPORT.OUTFILE]}.", refname=script_name)

    # upload_to_s3(annotated_filename, PROJ_BUCKET, PROJ_INPUT_PATH, region=REGION)
