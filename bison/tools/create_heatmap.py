"""Tool to split all annotated GBIF occurrence records by species for further processing."""
import glob
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.common.util import BisonNameOp, get_site_headers_from_shapefile
from bison.tools._config_parser import get_common_arguments

from lmpy.data_preparation.build_grid import build_grid, build_grid_matrix_headers
from lmpy.matrix import Matrix
from lmpy.point import PointCsvReader
from lmpy.spatial.map import (
    create_point_heatmap_vector, create_site_headers_from_extent,
    rasterize_geospatial_matrix)

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Encode all species CSV files into presence-absence layers in a PAM."""

# Options to be placed in a configuration file for the command

PARAMETERS = {
    "required":
        {
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
            "process_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Large destination directory for temporary data."
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for outputs."
                },
            "output_basename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Base filename for output PAM."
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
                    CONFIG_PARAM.HELP: "Filename to write logging data."},
            "report_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.HELP: "Filename to write summary metadata."}

        }
}


# .....................................................................................
def cli():
    """CLI to add locations and RIIS identifiers to GBIF occurrence records.

    Raises:
        Exception: on no input data.
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
        Exception: on unknown JSON write error.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)
    report = {}

    # Check both optional csv_file_pattern and csv_filename for inputs
    csv_filenames = []
    try:
        pattern = config["csv_file_pattern"]
    except KeyError:
        pass
    else:
        csv_filenames = glob.glob(pattern)
    try:
        filenames = config["csv_filename"]
    except KeyError:
        pass
    else:
        for fn in filenames:
            if os.path.exists(fn):
                csv_filenames.append(fn)

    if not csv_filenames:
        raise Exception("No input occurrence files provided for encoding")

    site_headers = create_site_headers_from_extent(
        config["min_x"], config["min_y"], config["max_x"], config["max_y"],
        config["resolution"])

    heat_mtx = None
    for csv_fn in csv_filenames:
        reader = PointCsvReader(
            csv_fn, config["species_key"], config["x_key"], config["y_key"])

        basename = os.path.splitext(os.path.basename(csv_fn))[0]
        data_label = basename.replace(" ", "_")
        # heat_vct, sp_report = create_point_pa_vector(
        #     reader, site_headers, data_label, min_points=1, logger=logger)
        heat_vct, sp_report = create_point_heatmap_vector(
            reader, site_headers, data_label, logger=logger)

        if heat_mtx is None:
            heat_mtx = heat_vct
            report["inputs"] = [sp_report]
        else:
            heat_mtx = Matrix.concatenate([heat_mtx, heat_vct], axis=1)
            report["inputs"].append(sp_report)

    out_fname_noext = os.path.join(
        config["output_path"], f"{config['output_basename']}")

    out_matrix_filename = f"{out_fname_noext}.lmm"
    heat_mtx.write(out_matrix_filename)
    report["out_matrix_filename"] = out_matrix_filename

    out_raster_filename = f"{out_fname_noext}.tif"
    rst_report = rasterize_geospatial_matrix(
        heat_mtx, out_raster_filename, is_pam=False, nodata=-9999, logger=logger)
    report["raster"] = rst_report

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
