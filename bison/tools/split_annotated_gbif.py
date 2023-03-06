"""Tool to split all annotated GBIF occurrence records by species for further processing."""
import glob
import json
import os
from datetime import datetime

from bison.common.constants import CONFIG_PARAM
from bison.tools._config_parser import get_common_arguments

from lmpy.data_preparation.occurrence_splitter import (
    DEFAULT_MAX_WRITERS, get_writer_key_from_fields_func, get_writer_filename_func,
    OccurrenceSplitter)
from lmpy.point import PointCsvReader

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Split one or more CSV files containing GBIF Occurrence records annotated with
        geographic areas and determinations from the USGS Registry for
        Introduced and Invasive Species (RIIS).  Split CSV files by any field, but
        preferably by acceptedScientificName."""
# Options to be placed in a configuration file for the command
PARAMETERS = {
    "required":
        {
            "csv_file_pattern":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Fullpath with filepattern of filenames containing raw GBIF "
                        "occurrence records in CSV format"
                },
            "species_key":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP:
                        "Fieldname for the group-by value for splitting data."
                },
            "x_key":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP:
                        "Fieldname for the longitude value in data."
                },
            "y_key":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP:
                        "Fieldname for the latitude value in data."
                },
            "out_fields":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.HELP:
                        "Fieldnames for fields to write to the output files."
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
                    CONFIG_PARAM.HELP: "Filename to write logging data."
                },
            "report_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP: "Filename to write summary metadata."
                }

        }
}


# .............................................................................
def split_occurrence_files(
        csv_filenames, species_key, x_key, y_key, out_fields, output_path, logger):
    """Annotate GBIF records with geographic areas, and RIIS key and assessment.

    Args:
        csv_filenames (list): full filenames containing GBIF data to merge and split by
            species.
        species_key (str): name of the field for grouping by species
        x_key (str): name of the field containing longitude value.
        y_key (str): name of the field containing latitude value.
        out_fields (list): names of the fields to write to output files.
        output_path: destination directory for output species CSV files.
        logger (object): logger for writing relevant processing messages.

    Returns:
        report (dict): dictionary of metadata about the data and process.
    """
    report = {
        "species_key": species_key,
        "x_key": x_key,
        "y_key": y_key,
    }

    # Establish functions for getting writer key and filename
    writer_key_func = get_writer_key_from_fields_func(*tuple([species_key]))
    writer_filename_func = get_writer_filename_func(output_path)

    logger.log(f"Start Time : {datetime.now()}", refname=script_name)
    # Initialize processor
    with OccurrenceSplitter(
        writer_key_func, writer_filename_func, write_fields=out_fields,
        max_writers=DEFAULT_MAX_WRITERS, logger=logger
    ) as occurrence_processor:
        for csv_fn in csv_filenames:
            reader = PointCsvReader(csv_fn, species_key, x_key, y_key)
            # We will do no other wrangling on these data
            wranglers = []
            curr_report = occurrence_processor.process_reader(reader, wranglers)
            report[csv_fn] = curr_report

        logger.log(
            f"Start Time: {datetime.now()}: Submit {csv_fn} for splitting by "
            f"species to {output_path}", refname=script_name)

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

    if config["csv_file_pattern"] is not None:
        csv_filenames = glob.glob(os.path.join(config["csv_file_pattern"]))

    report = split_occurrence_files(
        csv_filenames, config["species_key"], config["x_key"], config["y_key"],
        config["out_fields"], config["output_path"], logger)

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
