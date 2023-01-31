"""Tool for annotating USGS RIIS data with accepted names from GBIF."""
import json
import os

from bison.common.log import Logger
from bison.common.util import chunk_files
from bison.tools._config_parser import build_parser, process_arguments

COMMAND = "annotate_riis"
DESCRIPTION = """\
Split a CSV file containing GBIF DwC occurrence records into smaller files. """
# Options to be placed in a configuration file for the command
ARGUMENTS = {
    "required":
        {
            "big_csv_filename":
                {
                    "type": str,
                    "test_file_existence": True,
                    "help": "Large CSV file to split into manageable chunks"
                }
        },
    "optional":
        {
            "log_filename":
                {
                    "type": str,
                    "help": "Filename to write logging data."},
            "report_filename":
                {
                    "type": str,
                    "help": "Filename to write summary metadata."}

        }
}


# .....................................................................................
def cli():
    """Command-line interface to split a very large CSV file into smaller files.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
    """
    parser = build_parser(COMMAND, DESCRIPTION)
    args = process_arguments(parser, config_arg='config_file')
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = Logger(
        script_name,
        log_filename=args.log_filename
    )

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
