"""Tool to add locations and RIIS identifiers to GBIF occurrence records."""
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.process.annotate import annotate_occurrence_file
from bison.tools._config_parser import get_common_arguments

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
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Source directory containing input data."
                },
            "dwc_filenames":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP: "Filenames of GBIF occurrence records in CSV format"
                },
            "riis_with_gbif_taxa_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Filename of USGS RIIS records, annotated with GBIF "
                        "accepted taxa. If this file does not exist, "
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for output data."
                },
        },
    "optional":
        {
            "log_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP: "Filename to write logging data."},
            "report_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP: "Filename to write summary metadata."}

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
