"""Tool to split all annotated GBIF occurrence records by species for further processing."""
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.tools._config_parser import get_common_arguments

from lmpy.matrix import Matrix
from lmpy.spatial.map import rasterize_geospatial_matrix

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Encode all species CSV files into presence-absence layers in a PAM."""

# Options to be placed in a configuration file for the command

PARAMETERS = {
    "required":
        {
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for output data."
                },
            "matrix":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "List of input matrix filenames for writing as CSV, "
                        "raster formats."
                },
        }
}


# .....................................................................................
def write_geo_matrix_std_ways(geo_mtx, geo_mtx_basename, logger, is_pam=False):
    report = {}
    out_raster_filename = f"{geo_mtx_basename}.tif"
    rast_rpt = rasterize_geospatial_matrix(
        geo_mtx, out_raster_filename, is_pam=is_pam, nodata=-9999, logger=None)
    report["raster"] = rast_rpt

    out_csv_filename = f"{geo_mtx_basename}.csv"
    csv_report = geo_mtx.write_csv(out_csv_filename, is_pam=is_pam)
    report["csv"] = csv_report

    logger.log(
        f"Wrote matrix as raster {out_raster_filename}, csv {out_csv_filename}.",
        refname=script_name)

    return report


# .....................................................................................
def cli():
    """CLI to add locations and RIIS identifiers to GBIF occurrence records.

    Raises:
        FileNotFoundError: on missing input file.
        Exception: on no input data.
        OSError: on failure to write to report_filename.
        IOError: on failure to write to report_filename.
        Exception: on unknown JSON write error.
    """
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    config, logger = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    # Check both optional csv_file_pattern and csv_filename for inputs
    mtx_filenames = config["matrix"]
    if mtx_filenames is not list:
        mtx_filenames = [mtx_filenames]
    for fn in mtx_filenames:
        if not os.path.exists(fn):
            raise FileNotFoundError(f"Matrix file {fn} does not exist")

    if not mtx_filenames:
        raise Exception("No input matrix files provided for converting")

    report = {}
    for fn in mtx_filenames:
        mtx = Matrix.load(fn)
        # There is no way to identify binary data except "pam" in the filename
        is_pam = True if fn.find("pam") > 0 else False
        basename = os.path.splitext(os.path.basename(fn))[0]
        out_base_filename = os.path.join(config["output_path"], f"{basename}")
        rpt = write_geo_matrix_std_ways(mtx, out_base_filename, logger, is_pam=is_pam)
        report["basename"] = rpt

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
