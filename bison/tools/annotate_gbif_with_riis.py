"""Tool for annotating GBIF occurrence data with USGS RIIS record annotations."""
import json
import os

from bison.process.annotate import annotate_occurrence_file
from bison.tools._config_parser import (HELP_PARAM, IS_FILE_PARAM, TYPE_PARAM,
                                        get_common_arguments)

DESCRIPTION = """\
        Annotate a CSV file containing GBIF Occurrence records with determinations
        from the USGS Registry for Introduced and Invasive Species (RIIS).  Input RIIS
        data must contain accepted names from GBIF, so records may be matched
        on species and location. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "dwc_filename":
                {
                    TYPE_PARAM: str,
                    IS_FILE_PARAM: True,
                    HELP_PARAM: "Filename of GBIF occurrence records in CSV format"
                },
            "riis_with_gbif_filename":
                {
                    TYPE_PARAM: str,
                    HELP_PARAM:
                        "Filename of USGS RIIS records, annotated with GBIF "
                        "accepted taxa."
                },
            "dwc_with_riis_filename":
                {
                    TYPE_PARAM: str,
                    HELP_PARAM:
                        "Filename to write GBIF occurrence records "
                        "annotated with RIIS identifiers."
                },
        },
    "optional":
        {
            "log_filename":
                {
                    TYPE_PARAM: str,
                    HELP_PARAM: "Filename to write logging data."},
            "report_filename":
                {
                    TYPE_PARAM: str,
                    HELP_PARAM: "Filename to write summary metadata."}

        }
}


# .....................................................................................
def cli():
    """Command-line interface to annotate GBIF occurrence data with RIIS identifiers.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    report = annotate_occurrence_file(
        config["dwc_filename"], config["riis_with_gbif_filename"],
        config["dwc_with_riis_filename"], logger)

    # If the output report was requested, write it
    if report_filename:
        try:
            with open(report_filename, mode='wt') as out_file:
                json.dump(report, out_file, indent=4)
        except OSError:
            raise
        except IOError:
            raise
        logger.log(
            f"Wrote report file to {report_filename}", refname=script_name)
