"""Tool for annotating USGS RIIS data with accepted names from GBIF."""
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.common.util import BisonNameOp
from bison.provider.riis_data import resolve_riis_taxa
from bison.tools._config_parser import get_common_arguments

DESCRIPTION = """\
Annotate a CSV file containing the USGS Registry for Introduced and Invasive
Species (RIIS) with accepted names from GBIF. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "riis_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Filename of the most current USGS RIIS Master List"
                },
            "outpath":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP:
                        "Destination directory for the output annotated RIIS list."
                },
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
    config, logger = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)
    annotated_riis_filename = BisonNameOp.get_annotated_riis_filename(
        config["riis_filename"])

    report = resolve_riis_taxa(
        config["riis_filename"], annotated_riis_filename, logger, overwrite=True)

    try:
        with open(config["report_filename"], mode='wt') as out_file:
            json.dump(report, out_file, indent=4)
    except OSError:
        raise
    except IOError:
        raise
    logger.log(
        f"Wrote report file to {config['report_filename']}", refname=script_name)


# .....................................................................................
__all__ = ["cli"]


# .....................................................................................
if __name__ == '__main__':  # pragma: no cover
    cli()
