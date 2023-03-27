"""Tool to summarize summaries of occ records by geographic area and RIIS assessment."""
import json
import os
from datetime import datetime

from bison.common.constants import CONFIG_PARAM
from bison.common.util import BisonNameOp
from bison.process.aggregate import Aggregator
from bison.tools._config_parser import get_common_arguments

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Annotate a CSV file containing GBIF Occurrence records with geographic areas
        including state and county designations from census boundaries,
        American Indian/Alaska Native Areas/Hawaiian Home Lands (AIANNH), and US
        Protected Areas (US-PAD), and determinations from the USGS Registry for
        Introduced and Invasive Species (RIIS).  Input RIIS data must contain accepted
        names from GBIF, so records may be matched on species and state. """
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "summary_filenames":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Fullpath of filenames containing GBIF occurrence records, "
                        "annotated with regions and RIIS determination, in CSV format"
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for output file."
                },
        }
}


# .....................................................................................
def cli():
    """CLI to add locations and RIIS identifiers to GBIF occurrence records.

    Raises:
        FileNotFoundError: on any missing files in summary_filenames.
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
        Exception: on unknown JSON write error.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    summary_filenames = config["summary_filenames"]
    for sum_fname in summary_filenames:
        if not os.path.exists(sum_fname):
            raise FileNotFoundError(f"Missing input summary file {sum_fname}.")

    out_filename = BisonNameOp.get_combined_summary_name(
        summary_filenames[0], config["output_path"]
    )
    # report = summarize_annotation_summaries(
    #     config["summary_filenames"], outfilename, logger)

    logger.log(f"Start Time : {datetime.now()}", refname=script_name)
    agg = Aggregator(logger)
    report = agg.summarize_summaries(summary_filenames, out_filename)
    logger.log(f"End Time : {datetime.now()}", refname=script_name)

    try:
        with open(config["report_filename"], mode='wt') as out_file:
            json.dump(report, out_file, indent=4)
    except OSError:
        raise
    except IOError:
        raise
    except Exception:
        raise
    logger.log(
        f"Wrote report file to {config['report_filename']}", refname=script_name)


# .....................................................................................
__all__ = ["cli"]


# .....................................................................................
if __name__ == '__main__':  # pragma: no cover
    cli()
