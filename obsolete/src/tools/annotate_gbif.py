"""Tool to add locations and RIIS identifiers to GBIF occurrence records."""
import json
import os
from datetime import datetime

from obsolete.src.common.constants2 import CONFIG_PARAM, LMBISON_PROCESS
from obsolete.src.common.util import BisonNameOp
from obsolete.src.process.annotate import Annotator
from obsolete.src.tools._config_parser import get_common_arguments

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
            "geoinput_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_DIR: True,
                    CONFIG_PARAM.HELP:
                        "Source directory containing geospatial input data."
                },
            "dwc_filenames":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Fullpath of filenames containing raw GBIF occurrence records in CSV format"
                },
            "riis_with_gbif_taxa_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Fullpath of filename of USGS RIIS records, annotated with GBIF "
                        "accepted taxa. If this file does not exist, "
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for output data."
                },
        }
}


# .............................................................................
def annotate_occurrence_files(
        dwc_filenames, annotated_riis_filename, geo_path, output_path, logger):
    """Annotate GBIF records with geographic areas, and RIIS key and assessment.

    Args:
        dwc_filenames (list): full filenames containing GBIF data for annotation.
        annotated_riis_filename (str): filename containing RIIS data annotated with
             GBIF accepted taxon name and ID.
        geo_path (str): Base directory containing geospatial data inputs.
        output_path: destination directory for output files of annotated records
        logger (object): logger for saving relevant processing messages

    Returns:
        report (dict): dictionary of metadata about the data and process.

    Raises:
        FileNotFoundError: on missing annotated RIIS file
        FileNotFoundError: on missing DWC input file(s).
    """
    report = {
        "annotated_riis_filename": annotated_riis_filename,
        "geospatial_data_dir": geo_path,
        "dwc_inputs": [],
    }
    if not os.path.exists(annotated_riis_filename):
        raise FileNotFoundError(
            f"Missing annotated RIIS file {annotated_riis_filename}.")
    for dwc_fname in dwc_filenames:
        if not os.path.exists(dwc_fname):
            raise FileNotFoundError(f"Missing input DWC occurrence file {dwc_fname}.")

    ant = Annotator(
        logger, geo_path, annotated_riis_filename=annotated_riis_filename)
    for dwc_fname in dwc_filenames:
        out_fname = BisonNameOp.get_process_outfilename(
            dwc_fname, outpath=output_path, step_or_process=LMBISON_PROCESS.ANNOTATE)

        logger.log(
            f"Start Time: {datetime.now()}: Submit {dwc_fname} for annotation "
            f"to {out_fname}", refname=script_name)

        # Add locality-intersections and RIIS determinations to GBIF DwC records
        process_rpt = ant.annotate_dwca_records(dwc_fname, out_fname)
        report["dwc_inputs"].append(process_rpt)
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
    config, logger = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    report = annotate_occurrence_files(
        config["dwc_filenames"], config["riis_with_gbif_taxa_filename"],
        config["geoinput_path"], config["output_path"], logger)

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
