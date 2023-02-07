"""Tool for annotating USGS RIIS data with accepted names from GBIF."""
import json
import os

from bison.providers.riis_data import resolve_riis_taxa
from bison.tools._config_parser import (HELP_PARAM, IS_FILE_PARAM, TYPE_PARAM,
                                        get_common_arguments)

DESCRIPTION = """\
Annotate a CSV file containing the USGS Registry for Introduced and Invasive
Species (RIIS) with accepted names from GBIF. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "riis_filename":
                {
                    TYPE_PARAM: str,
                    IS_FILE_PARAM: True,
                    HELP_PARAM: "Filename of the most current USGS RIIS Master List"
                },
            "annotated_riis_filename":
                {
                    TYPE_PARAM: str,
                    HELP_PARAM: "Filename to write the annotated RIIS list."
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
    """Command-line interface to annotate RIIS records with GBIF accepted taxon.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    report = resolve_riis_taxa(
        config["riis_filename"], config["annotated_riis_filename"], logger,
        overwrite=True)

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
