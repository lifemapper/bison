"""Tool to summarize annotated GBIF records by geographic area and RIIS assessment."""
import json
import os
from datetime import datetime

from bison.common.constants import CONFIG_PARAM, LMBISON_PROCESS
from bison.common.util import BisonNameOp
from bison.process.aggregate import Aggregator
from bison.tools._config_parser import get_common_arguments

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Summarize one or more annotated CSV files containing GBIF Occurrence records
        with geographic areas including state and county designations from census
        boundaries, American Indian/Alaska Native Areas/Hawaiian Home Lands (AIANNH),
        and US Protected Areas (US-PAD), and determinations from the USGS Registry for
        Introduced and Invasive Species (RIIS)."""
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "annotated_dwc_filenames":
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


# .............................................................................
def summarize_occurrence_annotations(
        annotated_dwc_filenames, output_path, logger):
    """Summarize annotated GBIF records by geographic area and RIIS assessment.

    Args:
        annotated_dwc_filenames (list): full filenames containing annotated GBIF data.
        output_path: destination directory for output files of annotated records
        logger (object): logger for saving relevant processing messages

    Returns:
        report (dict): dictionary of metadata about the data and process.

    Raises:
        FileNotFoundError: on missing annotated RIIS file
        FileNotFoundError: on missing DWC input file(s).
    """
    report = {
        "annotated_dwc_inputs": [],
    }
    for ann_fname in annotated_dwc_filenames:
        if not os.path.exists(ann_fname):
            raise FileNotFoundError(f"Missing input DWC occurrence file {ann_fname}.")

    agg = Aggregator(logger)
    for ann_fname in annotated_dwc_filenames:
        output_summary_fname = BisonNameOp.get_process_outfilename(
            ann_fname, outpath=output_path, step_or_process=LMBISON_PROCESS.SUMMARIZE)

        logger.log(
            f"Start Time: {datetime.now()}: Submit {ann_fname} for aggregation "
            f"to {output_summary_fname}", refname=script_name)

        # Add locality-intersections and RIIS determinations to GBIF DwC records
        process_rpt = agg.summarize_annotated_recs_by_location(
            ann_fname, output_summary_fname, overwrite=True)
        report["annotated_dwc_inputs"].append(process_rpt)
        logger.log(f"End Time : {datetime.now()}", refname=script_name)

    return report


# .....................................................................................
def cli():
    """CLI to add locations and RIIS identifiers to GBIF occurrence records.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
        Exception: on unknown JSON write error.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    report = summarize_occurrence_annotations(
        config["annotated_dwc_filenames"], config["output_path"], logger)

    # If the output report was requested, write it
    if report_filename:
        try:
            with open(report_filename, mode='wt') as out_file:
                json.dump(report, out_file, indent=4)
        except OSError:
            raise
        except IOError:
            raise
        except Exception:
            raise
        logger.log(
            f"Wrote report file to {report_filename}", refname=script_name)


# .....................................................................................
__all__ = ["cli"]


# .....................................................................................
if __name__ == '__main__':  # pragma: no cover
    cli()
