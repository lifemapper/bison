"""Tool to split all annotated GBIF occurrence records by species for further processing."""
import glob
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.common.util import BisonNameOp
from bison.tools._config_parser import get_common_arguments

from lmpy.data_preparation.build_grid import build_grid
from lmpy.point import PointCsvReader
from lmpy.spatial.map import create_point_heatmap_vector

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Encode all species CSV files into presence-absence layers in a PAM."""

# Options to be placed in a configuration file for the command

PARAMETERS = {
    "required":
        {
            "csv_file_pattern":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Fullpath with filepattern of filenames, one per species, "
                        "containing occurrence records in CSV format"
                },
            "csv_filename":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "List of full filenames, one per species, containing "
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
            "min_x":
                {
                    CONFIG_PARAM.TYPE: float,
                    CONFIG_PARAM.HELP: "Minimum longitude boundary for the output PAM."
                },
            "min_y":
                {
                    CONFIG_PARAM.TYPE: float,
                    CONFIG_PARAM.HELP: "Minimum latitude boundary for the output PAM."
                },
            "max_x":
                {
                    CONFIG_PARAM.TYPE: float,
                    CONFIG_PARAM.HELP: "Maximum longitude boundary for the output PAM."
                },
            "max_y":
                {
                    CONFIG_PARAM.TYPE: float,
                    CONFIG_PARAM.HELP: "Maximum latitude boundary for the output PAM."
                },
            "resolution":
                {
                    CONFIG_PARAM.TYPE: float,
                    CONFIG_PARAM.HELP:
                        "Grid cell size in decimal degrees for the output PAM."
                },
            "out_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination file for output PAM."
                },
        },
    "optional":
        {
            "csv_file_pattern":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Fullpath with filepattern of filenames, one per species, "
                        "containing occurrence records in CSV format"
                },
            "csv_filename":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "List of full filenames, one per species, containing "
                        "occurrence records in CSV format"
                },
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
def build_grid(min_x, min_y, max_x, max_y, resolution, outpath, logger):
    """Create a grid to structure the species layers.

    Args:
        min_x (float): minimum longitude boundary for the output PAM.
        min_y (float): minimum latitude boundary for the output PAM.
        max_x (float): maximum longitude boundary for the output PAM.
        max_y (float): maximum latitude boundary for the output PAM.
        resolution (float): resolution of gridcells for the output PAM>
        output_path (str): destination directory for output species CSV files.
        logger (object): logger for writing relevant processing messages.

    Returns:
        report (dict): dictionary of metadata about the data and process.
    """
    cell_sides = 4
    epsg = 4326
    grid_filename = BisonNameOp.get_grid_filename(resolution, outpath)
    report = build_grid(
        grid_filename,
        min_x,
        min_y,
        max_x,
        max_y,
        resolution,
        epsg,
        cell_sides,
        logger=logger
    )
    report = {
        "min_x": min_x,
        "min_y": min_y,
        "max_x": max_x,
        "max_y": max_y,
        "resolution": resolution,
        "cell_sides": cell_sides,
        "epsg": epsg,
        "grid_filename": grid_filename
    }

    return report


# .....................................................................................
def cli():
    """CLI to add locations and RIIS identifiers to GBIF occurrence records.

    Raises:
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
        Exception: on unknown JSON write error.
    """
    report = {}

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    csv_filenames = []
    if config["csv_file_pattern"] is not None:
        csv_filenames = glob.glob(os.path.join(config["csv_file_pattern"]))

    if config["csv_filename"]:
        for fn in config["csv_filename"]:
            if os.path.exists(fn):
                csv_filenames.append(fn)

    # Build base PAM grid
    grid_report = build_grid(
        csv_filenames, config["x_min"], config["y_min"], config["x_max"],
        config["x_max"], config["resolution"], config["output_path"], logger)
    report["grid"] = grid_report
    site_headers = report["site_headers"]

    heat_mtx = None
    for csv_fn in csv_filenames:
        # Create a
        reader = PointCsvReader(
            csv_fn, config["species_key"], config["x_key"], config["y_key"])

        basename = os.path.splitext(os.path.basename(csv_fn))[0]
        data_label = basename.replace(" ", "_")
        heat_vct, sp_report = create_point_heatmap_vector(
            reader, site_headers, data_label, logger=logger)

        if not heat_mtx:
            heat_mtx = heat_vct
            report["inputs"] = [sp_report]
        else:
            heat_mtx.concatenate([heat_vct])
            report["inputs"].append(sp_report)

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
