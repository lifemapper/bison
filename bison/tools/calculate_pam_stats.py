"""Tool to split all annotated GBIF occurrence records by species for further processing."""
import json
import os

from bison.common.constants import CONFIG_PARAM
from bison.tools._config_parser import get_common_arguments

from lmpy.matrix import Matrix
from lmpy.spatial.map import rasterize_geospatial_matrix
from lmpy.statistics.pam_stats import PamStats

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """\
        Encode all species CSV files into presence-absence layers in a PAM."""

# Options to be placed in a configuration file for the command

PARAMETERS = {
    "required":
        {
            "matrix_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP: "Filename of the PAM matrix"
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Destination directory for outputs."
                }
        }
}


# .....................................................................................
def write_matrix_one_way(mtx, out_filename, logger):
    mtx.write(out_filename)
    report = mtx.get_report()
    report["filename"] = out_filename
    logger.log(
        f"Wrote matrix to {out_filename}.", refname=script_name)
    return report


# .....................................................................................
def write_geo_matrix_all_ways(geo_mtx, geo_mtx_fname, logger):
    geo_mtx.write(geo_mtx_fname)
    logger.log(
        f"Wrote statistics to {geo_mtx_fname}.", refname=script_name)
    report = geo_mtx.get_report()
    report["filename"] = geo_mtx_fname

    out_raster_filename = geo_mtx_fname.replace(".lmm", ".tif")
    rast_rpt = rasterize_geospatial_matrix(
        geo_mtx, out_raster_filename, is_pam=False, nodata=-9999, logger=None)
    report["raster"] = rast_rpt

    out_csv_filename = geo_mtx_fname.replace(".lmm", ".csv")
    csv_report = geo_mtx.write_csv(out_csv_filename)
    report["csv"] = csv_report

    logger.log(
        f"Wrote matrix as matrix {geo_mtx_fname}, raster {out_raster_filename}, "
        f"csv {out_csv_filename}.", refname=script_name)

    return report


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
    config, logger = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)
    report = {}

    pam = Matrix.load(config["matrix_filename"])
    stats = PamStats(pam, logger=logger)

    out_basename, _ = os.path.splitext(os.path.basename(config["matrix_filename"]))
    out_fname_noext = os.path.join(config["output_path"], f"{out_basename}")

    # Covariance: sigma_sites (site x site), sigma_species (species x species)
    covariance_stats = stats.calculate_covariance_statistics()
    for name, mtx in covariance_stats:
        fn = f"{out_fname_noext}_covariance_{name.replace(' ', '_')}.lmm"
        rpt = write_matrix_one_way(mtx, fn, logger)
        report[f"covariance_matrix_{name}"] = rpt

    # Diversity statistics (not geographic)
    diversity_mtx_fname = f"{out_fname_noext}_diversity.lmm"
    diversity_stats = stats.calculate_diversity_statistics()
    diversity_rpt = write_matrix_one_way(diversity_stats, diversity_mtx_fname, logger)
    report["diversity_matrix"] = diversity_rpt

    # Site statistics (4 geographic stats)
    site_stats_mtx_fname = f"{out_fname_noext}_site_stats.lmm"
    site_stats = stats.calculate_site_statistics()
    # Write matrix, raster, csv
    site_stats_rpt = write_geo_matrix_all_ways(
        site_stats, site_stats_mtx_fname, logger)
    report["site_stats_matrix"] = site_stats_rpt

    # Species statistics (not geographic)
    species_stats_mtx_fname = f"{out_fname_noext}_species_stats.lmm"
    species_stats = stats.calculate_species_statistics()
    species_stats_rpt = write_matrix_one_way(
        species_stats, species_stats_mtx_fname, logger)
    report["species_stats_matrix"] = species_stats_rpt

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
