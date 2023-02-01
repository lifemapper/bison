"""Tool for annotating USGS RIIS data with accepted names from GBIF."""
import json
import os

from bison.common.log import Logger
from bison.providers.riis_data import resolve_riis_taxa
from bison.tools._config_parser import build_parser, process_arguments

COMMAND = "annotate_riis"
DESCRIPTION = """\
Annotate a CSV file containing the USGS Registry for Introduced and Invasive
Species (RIIS) with accepted names from GBIF. """
# Options to be placed in a configuration file for the command
ARGUMENTS = {
    "required":
        {
            "riis_filename":
                {
                    "type": str,
                    "test_file_existence": True,
                    "help": "Filename of the most current USGS RIIS Master List"
                },
            "annotated_riis_filename":
                {
                    "type": str,
                    "help": "Filename to write the annotated RIIS list."
                },
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
    """Command-line interface to build grid.

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

    report = resolve_riis_taxa(
        args.riis_filename, args.annotated_riis_filename, logger, overwrite=True)

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
