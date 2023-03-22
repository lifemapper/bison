"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
from datetime import datetime
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.process.aggregate import Aggregator
from bison.tools._config_parser import get_common_arguments

DESCRIPTION = """\
Count summary occurrences from all aggregated summaries and compare to known counts. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "combined_summary_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP: "CSV file summarizing all annotation summaries."
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for output data."
                }
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
    """Command-line interface to split a very large CSV file into smaller files.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    logger.log(f"Start Time : {datetime.now()}", refname=script_name)
    agg = Aggregator(logger)
    report = agg.aggregate_file_summary_for_regions(
        config["combined_summary_filename"], config["riis_with_gbif_taxa_filename"],
        config["output_path"])
    logger.log(f"End Time : {datetime.now()}", refname=script_name)

    # If the output report was requested, write it
    report_filename = config["report_filename"]
    if report_filename is not None:
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
