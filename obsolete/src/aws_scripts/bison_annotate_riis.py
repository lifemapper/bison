"""Script to annotate RIIS data with GBIF accepted taxon."""
import argparse
import json
from logging import ERROR
import os

from bison.provider.riis_data import RIIS
from bison.common.util import (BisonNameOp, get_logger, get_today_str)
script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """Execute one or more steps of annotating GBIF data with RIIS
                assessments, and summarizing by species, county, and state"""


# .............................................................................
class REPORT:
    """Common keys for process report dictionary."""
    PROCESS = "process"
    RIIS_IDENTIFIER = "riis_ids"
    RIIS_TAXA = "riis_taxa"
    RIIS_RESOLVE_FAIL = "riis_bad_species"
    TAXA_RESOLVED = "names_resolved"
    RECORDS_UPDATED = "records_updated"
    RECORDS_OUTPUT = "records_output"
    INFILE = "input_filename"
    OUTFILE = "output_filename"
    LOGFILE = "log_filename"
    REPORTFILE = "report_filename"
    SUMMARY = "summary"
    REGION = "region"
    MIN_VAL = "min_val_for_presence"
    MAX_VAL = "min_val_for_presence"
    LOCATION = "locations"
    AGGREGATION = "riis_assessments_by_location"
    SPECIES = "species"
    OCCURRENCE = "occurrences"
    HEATMATRIX = "heatmatrix"
    ROWS = "rows"
    COLUMNS = "columns"
    ANNOTATE_FAIL = "annotate_gbifid_failed"
    ANNOTATE_FAIL_COUNT = "records_failed_annotate"
    RANK_FAIL = "rank_failed"
    RANK_FAIL_COUNT = "records_failed_rank"
    MESSAGE = "message"


# .............................................................................
def resolve_riis_taxa(
        riis_filename, logger, overwrite=False):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for RIIS data records.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing resolved file.

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.
    """
    nnsl = RIIS(riis_filename, logger=logger)
    annotated_filename = BisonNameOp.get_annotated_riis_filename(riis_filename)
    # Update species data
    try:
        report = nnsl.resolve_riis_to_gbif_taxa(annotated_filename, overwrite=overwrite)
    except Exception as e:
        logger.log(
            f"Unexpected failure {e} in resolve_riis_taxa", refname=script_name,
            log_level=ERROR)
    else:
        logger.log(
            f"Found {report[REPORT.SUMMARY][REPORT.RIIS_IDENTIFIER]} names, "
            f"{report[REPORT.SUMMARY][REPORT.TAXA_RESOLVED]} resolved, "
            f"{report[REPORT.SUMMARY][REPORT.RECORDS_UPDATED]} updated, "
            f"{report[REPORT.SUMMARY][REPORT.RECORDS_OUTPUT]} written "
            f"of total {report[REPORT.RIIS_IDENTIFIER]} from {riis_filename} "
            f"to {report[REPORT.OUTFILE]}.", refname=script_name)

    return report


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--riis_file", type=str, help='Path to RIIS file.')
    args = parser.parse_args()
    riis_file = args.riis_file
    pth, tmp = os.path.split(riis_file)
    base_riis_name, ext = os.path.splitext(tmp)

    log_name = os.path.join(f"{script_name}_{get_today_str()}")
    logger = get_logger(log_name, log_dir=pth)

    report = resolve_riis_taxa(riis_file, logger)

    # Write output report
    if report:
        try:
            with open(report[REPORT.REPORTFILE], mode='wt') as out_file:
                json.dump(report, out_file, indent=4)
        except OSError:
            raise
        except IOError:
            raise
        logger.log(
            f"Wrote report file to {report[REPORT.REPORTFILE]}", refname=script_name)
