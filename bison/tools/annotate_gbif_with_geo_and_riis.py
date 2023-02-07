"""Tool to add locations and RIIS identifiers to GBIF occurrence records."""
import json
import os

from bison.process.annotate import annotate_occurrence_file
from bison.tools._config_parser import (
    HELP_PARAM, IS_INPUT_DIR_PARAM, IS_INPUT_FILE_PARAM, IS_OUPUT_DIR_PARAM, TYPE_PARAM,
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
            "input_path":
                {
                    TYPE_PARAM: str,
                    IS_INPUT_DIR_PARAM: True,
                    HELP_PARAM: "Source directory containing input data."
                },
            "dwc_filenames":
                {
                    TYPE_PARAM: list,
                    IS_INPUT_FILE_PARAM: True,
                    HELP_PARAM: "Filenames of GBIF occurrence records in CSV format"
                },
            "riis_with_gbif_taxa_filename":
                {
                    TYPE_PARAM: str,
                    IS_INPUT_FILE_PARAM: True,
                    HELP_PARAM:
                        "Filename of USGS RIIS records, annotated with GBIF "
                        "accepted taxa. If this file does not exist, "
                },
            "output_path":
                {
                    TYPE_PARAM: str,
                    IS_OUPUT_DIR_PARAM: True,
                    HELP_PARAM: "Destination directory for output data."
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
    """CLI to add locations and RIIS identifiers to GBIF occurrence records.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    # Add locality-intersections and RIIS determinations to GBIF DwC records
    report = annotate_occurrence_file(
        config["dwc_filename"], config["riis_with_gbif_taxa_filename"],
        config["dwc_with_geo_and_riis_filename"], logger)

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


# .....................................................................................
__all__ = ["cli"]


# .....................................................................................
if __name__ == '__main__':  # pragma: no cover
    cli()
