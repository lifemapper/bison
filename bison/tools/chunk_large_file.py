"""Tool for annotating USGS RIIS data with accepted names from GBIF."""
import json
import os

from bison.common.log import Logger
from bison.common.util import chunk_files
from bison.tools._config_parser import (HELP_PARAM, IS_FILE_PARAM,
                                        build_parser,
                                        process_arguments_from_file)

DESCRIPTION = """\
Split a CSV file containing GBIF DwC occurrence records into smaller files. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "big_csv_filename":
                {
                    "type": str,
                    IS_FILE_PARAM: True,
                    HELP_PARAM: "Large CSV file to split into manageable chunks"
                }
        },
    "optional":
        {
            "log_filename":
                {
                    "type": str,
                    HELP_PARAM: "Filename to write logging data."},
            "report_filename":
                {
                    "type": str,
                    HELP_PARAM: "Filename to write summary metadata."}

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
    parser = build_parser(script_name, DESCRIPTION)
    args = process_arguments_from_file(parser, PARAMETERS)
    logger = Logger(script_name, log_filename=args.log_filename)

    _, report = chunk_files(args.big_csv_filename, logger)

    # If the output report was requested, write it
    if args.report_filename:
        try:
            with open(args.report_filename, mode='wt') as out_file:
                json.dump(report, out_file, indent=4)
        except OSError:
            raise
        except IOError:
            raise
        logger.log(
            f"Wrote report file to {args.report_filename}", refname=script_name)


# .....................................................................................
__all__ = ["cli"]


# .....................................................................................
if __name__ == '__main__':  # pragma: no cover
    cli()
