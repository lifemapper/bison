"""Tool for annotating USGS RIIS data with accepted names from GBIF."""
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.common.util import Chunker
from bison.tools._config_parser import get_common_arguments

DESCRIPTION = """\
Split a CSV file containing GBIF DwC occurrence records into smaller files. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "big_csv_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP: "Large CSV file to split into manageable chunks."
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
            "number_of_chunks":
                {
                    CONFIG_PARAM.TYPE: int,
                    CONFIG_PARAM.HELP: "Number of subset files to create from this large file."
                },
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
        Exception: on missing input file.
        Exception: on missing output directory.
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    infilename = config["big_csv_filename"]
    output_path = config["output_path"]
    if not os.path.exists(infilename):
        raise Exception(f"Input file {infilename} does not exist.")
    if not os.path.exists(output_path):
        raise Exception(f"Output path {output_path} does not exist.")

    _, report = Chunker.chunk_files(infilename, output_path, logger)

    # If the output report was requested, write it
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
