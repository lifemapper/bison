"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import csv
from logging import DEBUG, ERROR, INFO
import json
import os
import time

from bison.common.constants import (
    APPEND_TO_DWC, CONFIG_PARAM, LMBISON_PROCESS, GBIF, ENCODING, EXTRA_CSV_FIELD, LOG,
    REGION, REPORT)
from bison.common.util import (
    BisonNameOp, Chunker, delete_file, get_csv_dict_reader)
from bison.process.aggregate import Aggregator
from bison.process.annotate import (Annotator, parallel_annotate, parallel_annotate_update)
from bison.process.geoindex import (GeoResolver, GeoException)
from bison.process.geo_matrix import SiteMatrix
from bison.process.sanity_check import Counter
from bison.provider.riis_data import RIIS
from bison.tools._config_parser import get_common_arguments

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """Execute one or more steps of annotating GBIF data with RIIS
                assessments, and summarizing by species, county, and state"""
# .............................................................................
PARAMETERS = {
    "required":
        {
            "riis_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Full filename of input USGS RIIS data in CSV format."
                },
            "gbif_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Full filename of input GBIF occurrence data in CSV format."
                },
            "do_split":
                {
                    CONFIG_PARAM.TYPE: bool,
                    CONFIG_PARAM.HELP:
                        "Flag indicating whether the GBIF data is to be (or has been) "
                        "split into smaller subsets."
                },
            "geo_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_DIR: True,
                    CONFIG_PARAM.HELP:
                        "Source directory containing geospatial input data."
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
                    CONFIG_PARAM.HELP: "Large destination directory for output data."
                }
        },
    "optional":
        {
            "gbif_id":
                {
                    CONFIG_PARAM.TYPE: int,
                    CONFIG_PARAM.HELP:
                        "Identifier, gbifId, of troublesome record in original or "
                        "annotated occurrence file."
                },
            "line_num":
                {
                    CONFIG_PARAM.TYPE: int,
                    CONFIG_PARAM.HELP:
                        "Line number of record to examine in original or "
                        "annotated occurrence file."
                },
            "examine_filenames":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "List of full filenames of input occurrence files to inspect."
                }
        }
}


# .............................................................................
def a_find_or_create_subset_files(gbif_filename, output_path, logger):
    """Find or create subset files from a large file based on the file size and CPUs.

    Args:
        gbif_filename (str): full filename of data file to be subsetted into chunks.
        output_path (str): Destination directory for subset files.
        logger (object): logger for saving relevant processing messages

    Returns:
        chunk_filenames (list): full filenames for subset files created from large
            input file.
    """
    chunk_filenames = Chunker.identify_chunk_files(gbif_filename, output_path)
    # If any are missing, delete them all and split
    re_split = False
    for chunk_fname in chunk_filenames:
        if not os.path.exists(chunk_fname):
            re_split = True
            break
    if re_split is True:
        # Delete any existing files
        for chunk_fname in chunk_filenames:
            delete_file(chunk_fname)
        # Resplit into subset files
        chunk_filenames = Chunker.chunk_files(gbif_filename, output_path, logger)

    return chunk_filenames


# .............................................................................
def a_resolve_riis_taxa(riis_filename, logger, overwrite=False):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for RIIS data records.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing resolved file.

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.
    """
    nnsl = RIIS(riis_filename, logger=logger)
    # Update species data
    try:
        report = nnsl.resolve_riis_to_gbif_taxa(overwrite=overwrite)
    except Exception as e:
        logger.log(
            f"Unexpected failure {e} in resolve_riis_taxa", refname=script_name,
            log_level=ERROR)
    else:
        logger.log(
            f"Found {report[REPORT.SUMMARY][REPORT.RIIS_IDENTIFIER]} names, "
            f"{report[REPORT.SUMMARY][REPORT.TAXA_RESOLVED]} resolved, "
            f"{report[REPORT.SUMMARY][REPORT.RECORDS_UPDATED]} updated, "
            f"{report[REPORT.SUMMARY][REPORT.RECORDS_OUTPUT]} written "
            f"of total {report[REPORT.RIIS_IDENTIFIER]} from {riis_filename} "
            f"to {report[REPORT.OUTFILE]}.", refname=script_name)
        report_filename = BisonNameOp.get_process_report_filename(
            config["riis_filename"], output_path=config["output_path"],
            step_or_process=LMBISON_PROCESS.RESOLVE)
        report[REPORT.REPORTFILE] = report_filename

    return report


# .............................................................................
def bb_annotate_pad_occurrence_files(
        annotated_filenames, annotated_riis_filename, geo_path, process_path, logger,
        run_parallel=True, overwrite=False):
    """Annotate GBIF records with PAD, after DOI annotation.

    Args:
        annotated_filenames (list): list of full filenames containing
            partially-annotated GBIF data for PAD annotation.
        annotated_riis_filename (str): Full path to RIIS data annotated with GBIF names.
        geo_path (str): Base directory containing geospatial region files
        process_path (str): Destination directory for output files.
        logger (object): logger for saving relevant processing messages
        run_parallel (bool): Flag indicating whether to process subset files in parallel
        overwrite (bool): Flag indicating whether to overwrite existing annotated file(s).

    Returns:
        report: full filenames for GBIF data newly annotated with state,
            county, RIIS assessment, and RIIS key.  If a file exists, do not annotate.
    """
    if run_parallel and len(annotated_filenames) > 1:
        rpts = parallel_annotate_update(
            annotated_filenames, annotated_riis_filename, geo_path, process_path, logger,
            overwrite, [REGION.DOI, REGION.PAD])
        reports = {"reports": rpts}

    else:
        reports = {"reports": []}
        # Compare with serial execution
        all_start = time.perf_counter()
        # Use the same RIIS for all Annotators
        riis = RIIS(annotated_riis_filename, logger)
        riis.read_riis()

        logger.log(
            f"Annotate files with PAD: {time.asctime()}", refname=script_name)
        for part_ann_fname in annotated_filenames:
            # Add locality-intersections for DOI and PAD to GBIF DwC records
            start = time.perf_counter()
            ant = Annotator(
                logger, geo_path, riis=riis, regions=[REGION.DOI, REGION.PAD])
            rpt = ant.annotate_dwca_records_update(
                part_ann_fname, version=2, overwrite=overwrite)

            end = time.perf_counter()

            reports["reports"].append(rpt)
            logger.log(
                f"Elapsed time: {end-start} seconds",
                refname=script_name)

        all_end = time.perf_counter()
        logger.log(
            f"PAD Annotation End Time: {time.asctime()}, {all_end-all_start} "
            f"seconds elapsed", refname=script_name)

    return reports


# .............................................................................
def b_annotate_occurrence_files(
        occ_filenames, annotated_riis_filename, geo_path, process_path, logger,
        run_parallel=True, overwrite=False):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        occ_filenames (list): list of full filenames containing GBIF data for
            annotation.
        annotated_riis_filename (str): Full path to RIIS data annotated with GBIF names.
        geo_path (str): Base directory containing geospatial region files
        process_path (str): Destination directory for output files.
        logger (object): logger for saving relevant processing messages
        run_parallel (bool): Flag indicating whether to process subset files in parallel
        overwrite (bool): Flag indicating whether to overwrite existing annotated file(s).

    Returns:
        annotated_filenames: full filenames for GBIF data newly annotated with state,
            county, RIIS assessment, and RIIS key.  If a file exists, do not annotate.
    """
    if run_parallel and len(occ_filenames) > 1:
        rpts = parallel_annotate(
            occ_filenames, annotated_riis_filename, geo_path, process_path, logger,
            overwrite)
        reports = {"reports": rpts}

    else:
        reports = {"reports": []}
        # Compare with serial execution
        all_start = time.perf_counter()
        # Use the same RIIS for all Annotators
        riis = RIIS(annotated_riis_filename, logger)
        riis.read_riis()

        logger.log(
            f"Annotate files serially: {time.asctime()}", refname=script_name)
        for occ_fname in occ_filenames:
            # Add locality-intersections and RIIS determinations to GBIF DwC records
            start = time.perf_counter()
            ant = Annotator(logger, geo_path, riis=riis)
            rpt = ant.annotate_dwca_records(occ_fname, process_path, overwrite=overwrite)
            end = time.perf_counter()

            reports["reports"].append(rpt)
            logger.log(
                f"Elapsed time: {end-start} seconds",
                refname=script_name)

        all_end = time.perf_counter()
        logger.log(
            f"Serial Annotation End Time: {time.asctime()}, {all_end-all_start} "
            f"seconds elapsed", refname=script_name)

    return reports


# .............................................................................
def c_summarize_combine_annotated_files(
        annotated_filenames, full_summary_filename, output_path, logger,
        overwrite=True):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        annotated_filenames (list): full filenames containing annotated GBIF data.
        full_summary_filename (str): Full filename for output combined summary file.
        output_path (str): Destination directory for log, report, and final output files.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing summarized and
            combined files.

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.

    Note:
        This process is fast, and need not be performed in parallel
    """
    summary_filenames = []
    for ann_fname in annotated_filenames:
        agg = Aggregator(logger)
        process_path, _ = os.path.split(ann_fname)

        # Summarize each annotated file
        rpt = agg.summarize_annotated_recs_by_location(
            ann_fname, process_path, overwrite=overwrite)
        summary_filenames.append(rpt[REPORT.OUTFILE])

    # Combine all the subset summaries into a single summary data written to
    # report["full_summary_filename"]
    if len(summary_filenames) > 1:
        report = agg.summarize_summaries(summary_filenames, full_summary_filename)
    else:
        report = rpt

    report_filename = BisonNameOp.get_process_report_filename(
        report[REPORT.OUTFILE], output_path=output_path,
        step_or_process=LMBISON_PROCESS.SUMMARIZE)
    report[REPORT.REPORTFILE] = report_filename

    return report


# .............................................................................
def d_aggregate_summary_by_region(
        full_summary_filename, annotated_riis_filename, output_path, logger, overwrite=True):
    """Aggregate annotated GBIF records with region, and RIIS key and assessment.

    Args:
        full_summary_filename (str): Full filename containing summarized
            GBIF data by region for RIIS assessment of records.
        annotated_riis_filename (str): full filename of RIIS data annotated with GBIF
            names.
        output_path (str): Destination directory for final summary files.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing
            summarized-by-region files.

    Note:
        It is preferable to manually delete old summary files prior to running this
        function, since the filenames cannot be anticipated without executing the full
        process.

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.
    """
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(logger)
    # Aggregate to files
    report = agg.aggregate_summary_for_regions_assessments(
        full_summary_filename, annotated_riis_filename, output_path, overwrite=overwrite)
    report_filename = BisonNameOp.get_process_report_filename(
        full_summary_filename, output_path=output_path,
        step_or_process=LMBISON_PROCESS.AGGREGATE)
    report[REPORT.REPORTFILE] = report_filename

    return report


# .............................................................................
def e_county_heatmap(
        full_summary_filename, geo_path, heatmatrix_filename, logger, overwrite=True):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        full_summary_filename (str): Full filename containing summarized
            GBIF data by region for RIIS assessment of records.
        geo_path (str): Base directory containing geospatial region files
        heatmatrix_filename (str): Full filename for output pandas.DataFrame.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing matrix file(s).

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.
    """
    region_type = "county"
    # from common.constants.REGION.COUNTY["map"]
    county_fldname = "NAME"
    state_fldname = "STUSPS"
    county_filename = os.path.join(geo_path, REGION.COUNTY["file"])

    report = {
        REPORT.INFILE: full_summary_filename,
        REPORT.REGION: region_type,
        REPORT.PROCESS: LMBISON_PROCESS.HEATMATRIX
    }

    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(logger)
    # Aggregate species/location/count
    agg.read_region_assessments(full_summary_filename)
    species_lookup = agg.get_species_lookup()

    # Initialize dictionary for species in counties structure
    # Species may be different RIIS region (AK, HI, L48), each as a different key
    # Do not include counts for species "na"
    species_lookup.pop("na")
    species_fids = {}
    for rr_species_key in species_lookup.keys():
        species_fids[rr_species_key] = {}

    county_matrix = SiteMatrix(
        spatial_filename=county_filename, fieldnames=[state_fldname, county_fldname],
        logger=logger)
    location_fid = county_matrix.row_lookup_by_attribute()

    region_names = agg.get_locations_for_region_type(region_type)
    # for each cell (county) in the county matrix
    for reg_name in region_names:
        fid = location_fid[reg_name]
        species_counts = agg.get_species_counts_for_location(region_type, reg_name)
        # Fill species_fids with species = {fid: count}
        for rr_species_key, sp_count in species_counts.items():
            if rr_species_key != "na":
                species_fids[rr_species_key][fid] = sp_count
            else:
                pass

    species_columns = {}
    for rr_species_key, fid_count in species_fids.items():
        species_columns[rr_species_key] = county_matrix.create_data_column(
            fid_count)
    county_matrix.create_dataframe_from_cols(species_columns)
    county_matrix.write_matrix(heatmatrix_filename, overwrite=overwrite)

    report[REPORT.OUTFILE] = heatmatrix_filename
    report[REPORT.HEATMATRIX] = {
        REPORT.ROWS: county_matrix.row_count,
        REPORT.COLUMNS: county_matrix.columns
    }
    return report


# .............................................................................
def f_calculate_pam_stats(heatmatrix_filename, output_path, min_val, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        heatmatrix_filename (str): Full filename for output pandas.DataFrame.
        output_path (str): Destination directory for output files.
        min_val (numeric): minimum value for which to code a cell 1.
        logger (object): logger for saving relevant processing messages

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.
    """
    region_type = "county"
    sites_diversity_filename = BisonNameOp.get_process_outfilename(
        heatmatrix_filename, outpath=output_path, postfix="sites")
    species_diversity_filename = BisonNameOp.get_process_outfilename(
        heatmatrix_filename, outpath=output_path, postfix="species")
    psi_filename = BisonNameOp.get_process_outfilename(
        heatmatrix_filename, outpath=output_path, postfix="psi")
    psi_avg_proportional_filename = BisonNameOp.get_process_outfilename(
        heatmatrix_filename, outpath=output_path, postfix="psi_avg_proportional")
    stats_filename = BisonNameOp.get_process_outfilename(
        heatmatrix_filename, outpath=output_path, postfix="stats")
    report = {
        REPORT.INFILE: heatmatrix_filename,
        REPORT.OUTFILE: [
            sites_diversity_filename, species_diversity_filename, stats_filename
        ],
        REPORT.REGION: region_type,
        REPORT.MIN_VAL: min_val,
        REPORT.PROCESS: LMBISON_PROCESS.PAM,
    }

    heatmatrix = SiteMatrix(matrix_filename=heatmatrix_filename, logger=logger)
    # report[REPORT.HEATMATRIX] = {"cells": heatmatrix.row_lookup_by_attribute()}
    report["total_species"] = heatmatrix.column_count
    report["present_species"] = heatmatrix.num_species
    # aka Gamma diversity
    report["total_sites"] = heatmatrix.row_count
    report["present_sites"] = heatmatrix.num_sites

    pam = heatmatrix.convert_to_binary(min_val)

    # .....................................
    # Sites/County diversity
    # .....................................
    # Alpha diversity (species richness) for each site (county)
    alpha_series = pam.alpha()
    # Beta diversity (gamma/alpha) for each site (county)
    beta_series = pam.beta()
    # Combine alpha and beta diversities to a matrix and write
    site_diversity_mtx = SiteMatrix.concat_columns((alpha_series, beta_series))
    site_diversity_mtx.write_matrix(sites_diversity_filename)
    logger.log(
        f"Wrote site statistics to {sites_diversity_filename}.",
        refname=script_name)

    # .....................................
    # Species diversity
    # .....................................
    # Omega (range size) for each species
    omega_series = pam.omega()
    # Omega proportional (mean proportional range size) for each species
    omega_pr_series = pam.omega_proportional()
    omega_mtx = SiteMatrix.concat_rows((omega_series, omega_pr_series))
    omega_mtx.write_matrix(species_diversity_filename)
    logger.log(
        f"Wrote species statistics to {species_diversity_filename}.",
        refname=script_name)

    # .....................................
    # Range richness by species and site
    # .....................................
    # mean proportional species diversity by county
    psi_mtx = pam.psi()
    psi_mtx.write_matrix(psi_filename)
    # Range richness of each species in each county
    psi_average_proportional_mtx = pam.psi_average_proportional()
    psi_average_proportional_mtx.write_matrix(psi_avg_proportional_filename)
    logger.log(
        f"Wrote psi statistics to {psi_filename} and {psi_avg_proportional_filename}.",
        refname=script_name)

    # .....................................
    # PAM (landscape) diversity measures
    # .....................................
    pam.calc_write_pam_measures(stats_filename, overwrite=True)

    return report


# .............................................................................
def z_test(summary_filenames, full_summary_filename, output_path, logger):
    """Test the outputs to make sure counts are in sync.

    Args:
        summary_filenames (list): full filenames of summaries of location, species,
            occurrence counts, one file per each file in annotated_filenames.
        full_summary_filename (str): Full filename containing combined summarized
            GBIF data by region for RIIS assessment of records.
        output_path (str): Destination directory for testing report file.
        logger (object): logger for saving relevant processing messages

    Returns:
        report (dict): dictionary summarizing metadata about the processes and
            output files.
    """
    report = Counter.compare_location_species_counts(
        summary_filenames, full_summary_filename, logger)
    report_filename = BisonNameOp.get_process_report_filename(
        full_summary_filename, output_path=output_path,
        step_or_process=LMBISON_PROCESS.AGGREGATE)
    report[REPORT.REPORTFILE] = report_filename
    return report


# .............................................................................
def log_list(logger, msg, outlist):
    """Log output.

    Args:
        logger: logger
        msg: Message
        outlist: optional list of strings to be printed on individual lines
    """
    msg = f"{msg}\n"
    for elt in outlist:
        msg += f"  {elt}\n"
    logger.log(msg, refname=script_name, log_level=INFO)


# ...............................................
def _find_county_state(geo_county, lon, lat, buffer_vals):
    county = state = None
    if None not in (lon, lat):
        # Intersect coordinates with county boundaries for state and county values
        try:
            fldvals, ogr_seconds = geo_county.find_enclosing_polygon(
                lon, lat, buffer_vals=buffer_vals)
        except ValueError:
            raise
        except GeoException:
            raise
        county = fldvals[APPEND_TO_DWC.RESOLVED_CTY]
        state = fldvals[APPEND_TO_DWC.RESOLVED_ST]
    return county, state, ogr_seconds


# .............................................................................
def test_bad_line(trouble_id, raw_filenames, geo_path, logger):
    """Test georeferencing line with gbif_id .

    Args:
        trouble_id: gbifID for bad record to find
        raw_filenames: List of files to test, looking for troublesome data.
        geo_path: Full base directory for geospatial data
        logger: logger for writing messages.
    """
    geofile = os.path.join(geo_path, REGION.COUNTY["file"])
    geo_county = GeoResolver(geofile, REGION.COUNTY["map"], logger)
    for csvfile in raw_filenames:
        try:
            f = open(csvfile, "r", newline="", encoding="utf-8")
            rdr = csv.DictReader(f, quoting=csv.QUOTE_NONE, delimiter="\t",
                                 restkey=EXTRA_CSV_FIELD)
        except Exception as e:
            logger.log(
                f"Unexpected open error {e} on file {csvfile}", refname=script_name,
                log_level=ERROR)
        else:
            logger.log(f"Opened file {csvfile}", refname=script_name)
            try:
                # iterate over DwC records
                dwcrec = next(rdr)
                while dwcrec is not None:
                    gbif_id = dwcrec[GBIF.ID_FLD]
                    if (rdr.line_num % LOG.INTERVAL) == 0:
                        logger.log(
                            f"*** Record number {rdr.line_num}, gbifID: {gbif_id} ***",
                            refname=script_name, log_level=DEBUG)

                    # Debug: examine data
                    if gbif_id == trouble_id:
                        logger.log(
                            f"Found gbifID {trouble_id} on line {rdr.line_num}",
                            refname=script_name, log_level=DEBUG)

                    if EXTRA_CSV_FIELD in dwcrec.keys():
                        logger.log(
                            "Extra fields detected: possible bad read for record "
                            f"{gbif_id} on line {rdr.line_num}", refname=script_name,
                            log_level=DEBUG)

                    # Find county and state for these coords
                    try:
                        _county, _state, ogr_seconds = _find_county_state(
                            geo_county, dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD],
                            buffer_vals=REGION.COUNTY["buffer"])
                    except ValueError as e:
                        logger.log(
                            f"Record gbifID: {gbif_id}: {e}", refname=script_name,
                            log_level=ERROR)
                    except GeoException as e:
                        logger.log(
                            f"Record gbifID: {gbif_id}: {e}", refname=script_name,
                            log_level=ERROR)
                    if ogr_seconds > 0.75:
                        logger.log(
                            f"Record gbifID: {gbif_id}; OGR time {ogr_seconds}",
                            refname=script_name, log_level=DEBUG)

                    dwcrec = next(rdr)

            except Exception as e:
                logger.log(
                    f"Unexpected read error {e} on file {csvfile}", refname=script_name,
                    log_level=ERROR)


# .............................................................................
def read_bad_line(in_filename, logger, gbif_id=None, line_num=None):
    """Test troublesome lines.

    Args:
        in_filename: File to test, looking for troublesome data.
        logger: logger for writing messages.
        gbif_id (int): target GBIF identifier we are searching for
        line_num (int): target line number we are searching for

    Raises:
        Exception: on missing one of gbif_id or line_num.
    """
    if gbif_id is None and line_num is None:
        raise Exception("Must provide troublesome gbifID or line number")

    rdr, inf = get_csv_dict_reader(
        in_filename, GBIF.DWCA_DELIMITER, encoding=ENCODING, quote_none=True,
        restkey=EXTRA_CSV_FIELD)

    try:
        for dwcrec in rdr:
            if (rdr.line_num % LOG.INTERVAL) == 0:
                logger.log(
                    f"*** Record number {rdr.line_num}, gbifID: {gbif_id} ***",
                    refname=script_name, log_level=DEBUG)

            # Debug: examine data
            if gbif_id == dwcrec[GBIF.ID_FLD]:
                logger.log(
                    f"Found gbifID {gbif_id} on line {rdr.line_num}",
                    refname=script_name, log_level=DEBUG)
            elif rdr.line_num == line_num:
                logger.log(
                    f"Found line {rdr.line_num}", refname=script_name,
                    log_level=DEBUG)

            if EXTRA_CSV_FIELD in dwcrec.keys():
                logger.log(
                    f"Extra fields detected: possible bad read for record {gbif_id} on "
                    f"line {rdr.line_num}", refname=script_name,
                    log_level=DEBUG)

    except Exception as e:
        logger.log(
            f"Unexpected read error {e} on file {in_filename}", refname=script_name,
            log_level=ERROR)


# .............................................................................
def _prepare_args(config):
    """Prepare arguments, assembling paths, filenames, and splitting data if needed.

    Args:
        config (dict): user-provided parameters

    Returns:
        annotated_riis_filename: Full annotated RIIS filename
        out_path: destination path for output and logfiles
        raw_filenames: input CVS file(s) of original GBIF records
        annotated_filenames: output file(s) of annotated GBIF records
        summary_filenames: output file(s) of subset summaries of GBIF annotated records
        full_summary_filename: : output file of all annotated GBIF records

    Raises:
        FileNotFoundError: on missing input occurrence file.
    """
    # Geospatial/output/processing filenames will be generated, and located in paths
    # Process entire GBIF file, or do it in chunks
    booltmp = str(config["do_split"]).lower()
    # First determine whether to process one file or smaller subsets
    do_split = False
    if (booltmp in ("yes", "y", "true", "1")) or config["command"] == "split":
        do_split = True

    infile = config["gbif_filename"]
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Missing input file {infile}")
    out_path = config["output_path"]

    # First determine whether to process one file or smaller subsets
    if do_split is True:
        # Find existing or create subset files
        raw_filenames = a_find_or_create_subset_files(
            infile, config["process_path"], logger)
    else:
        raw_filenames = [infile]

    # annotated_filenames = [
    #     BisonNameOp.get_process_outfilename(
    #         csvfile, outpath=config["process_path"],
    #         step_or_process=LMBISON_PROCESS.ANNOTATE)
    #     for csvfile in raw_filenames]
    #
    # summary_filenames = [
    #     BisonNameOp.get_process_outfilename(
    #         csvfile, outpath=config["process_path"],
    #         step_or_process=LMBISON_PROCESS.SUMMARIZE)
    #     for csvfile in raw_filenames]
    #
    # full_summary_filename = BisonNameOp.get_process_outfilename(
    #     infile, outpath=config["process_path"],
    #     step_or_process=LMBISON_PROCESS.SUMMARIZE)
    #
    # heatmatrix_filename = BisonNameOp.get_process_outfilename(
    #         infile, outpath=config["process_path"],
    #         step_or_process=LMBISON_PROCESS.HEATMATRIX)

    return (infile, raw_filenames, out_path)


# .............................................................................
def execute_command(config, logger):
    """Execute processing command with parameters extracted from configuration file.

    Args:
        config (dict): dictionary of keyword parameters and values for current process.
        logger (obj): logger for writing messages to file and processing window.

    Returns:
        report: dictionary of metadata for process.

    Raises:
        FileNotFoundError: on missing input file.
    """
    report = {}
    step_or_process = None
    (infile, raw_filenames, out_path) = _prepare_args(config)

    annotated_riis_filename = BisonNameOp.get_annotated_riis_filename(
        config["riis_filename"])
    annotated_filenames = [
        BisonNameOp.get_process_outfilename(
            csvfile, outpath=config["process_path"],
            step_or_process=LMBISON_PROCESS.ANNOTATE)
        for csvfile in raw_filenames]
    summary_filenames = [
        BisonNameOp.get_process_outfilename(
            csvfile, outpath=config["process_path"],
            step_or_process=LMBISON_PROCESS.SUMMARIZE)
        for csvfile in raw_filenames]
    full_summary_filename = BisonNameOp.get_process_outfilename(
        config["gbif_filename"], outpath=config["process_path"],
        step_or_process=LMBISON_PROCESS.SUMMARIZE)
    heatmatrix_filename = BisonNameOp.get_process_outfilename(
            infile, outpath=config["process_path"],
            step_or_process=LMBISON_PROCESS.HEATMATRIX)

    # Make sure input files exist
    for csv_fname in raw_filenames:
        if not os.path.exists(csv_fname):
            raise FileNotFoundError(f"Expected file {csv_fname} does not exist")
    # log_list(logger, "Input filenames:", raw_filenames)

    if config["command"] == "chunk":
        _, report = Chunker.chunk_files(infile, out_path, logger)

    elif config["command"] == "resolve":
        step_or_process = LMBISON_PROCESS.RESOLVE
        report = a_resolve_riis_taxa(
            config["riis_filename"], logger, overwrite=False)
        logger.log(
            f"Resolved RIIS filename: {report[REPORT.OUTFILE]}", refname=script_name)

    elif config["command"] == "annotate":
        step_or_process = LMBISON_PROCESS.ANNOTATE
        # Annotate DwC records with regions, and if found, RIIS determination
        report = b_annotate_occurrence_files(
            raw_filenames, annotated_riis_filename, config["geo_path"],
            config["process_path"], logger, run_parallel=True, overwrite=True)

    elif config["command"] == "update_doi_pad":
        step_or_process = LMBISON_PROCESS.UPDATE
        # Annotate DwC records with regions, and if found, RIIS determination
        report = bb_annotate_pad_occurrence_files(
            annotated_filenames, annotated_riis_filename, config["geo_path"],
            config["process_path"], logger, run_parallel=True, overwrite=True)

    elif config["command"] == "summarize":
        step_or_process = LMBISON_PROCESS.SUMMARIZE

        # Summarize each annotated file by region, write summary to a file
        report = c_summarize_combine_annotated_files(
            annotated_filenames, full_summary_filename, config["output_path"], logger,
            overwrite=True)
        logger.log("Summary of annotations", report[REPORT.OUTFILE])

    elif config["command"] == "aggregate":
        step_or_process = LMBISON_PROCESS.AGGREGATE
        # Write summaries for each region to its own file
        report = d_aggregate_summary_by_region(
            full_summary_filename, annotated_riis_filename, out_path, logger,
            overwrite=True)
        for region_type in report.keys():
            try:
                log_list(
                    logger, f"Region {region_type} filenames, assessment filename:",
                    report[region_type][REPORT.OUTFILE])
            except TypeError:
                pass

    elif config["command"] == "check_counts":
        step_or_process = LMBISON_PROCESS.CHECK_COUNTS
        # Test summarized summaries
        report = z_test(summary_filenames, full_summary_filename, out_path, logger)

    elif config["command"] == "heat_matrix":
        step_or_process = LMBISON_PROCESS.HEATMATRIX
        report = e_county_heatmap(
            full_summary_filename, config["geo_path"], heatmatrix_filename, logger,
            overwrite=True)

    elif config["command"] == "pam_stats":
        step_or_process = LMBISON_PROCESS.PAM
        min_presence = 1
        report = f_calculate_pam_stats(
            heatmatrix_filename, out_path, min_presence, logger)

    elif config["command"] == "test_bad_data" and config["gbif_id"] is not None:
        test_bad_line(
            config["gbif_id"], config["examine_filenames"], config["geo_path"], logger)

    elif config["command"] == "find_bad_record":
        # Should only be one file in the examine_filenames list
        read_bad_line(
            config["examine_filenames"][0], logger, gbif_id=config["gbif_id"],
            line_num=config["line_num"])

    else:
        logger.log(
            f"Unsupported command {config['command']}", refname=script_name,
            log_level=ERROR)

    if step_or_process is not None and report is not None:
        report_filename = BisonNameOp.get_process_report_filename(
            config["gbif_filename"], output_path=config["output_path"],
            step_or_process=step_or_process)
        report[REPORT.REPORTFILE] = report_filename

    return report


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    # log_name = f"{script_name}_{start.isoformat()}"
    config, logger = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    logger.log(f"Command: {config['command']}", refname=script_name)
    logger.log(f"main start time: {time.asctime()}", refname=script_name)

    report = execute_command(config, logger)

    # Write output report
    if report:
        try:
            with open(report[REPORT.REPORTFILE], mode='wt') as out_file:
                json.dump(report, out_file, indent=4)
        except OSError:
            raise
        except IOError:
            raise
        logger.log(
            f"Wrote report file to {report[REPORT.REPORTFILE]}", refname=script_name)

    logger.log(f"main end time: {time.asctime()}", refname=script_name)

# logger = None
# output_path = "/volumes/bison/big_data/process"
# heatmatrix_filename = f"{output_path}/gbif_2023-01-26_10k_heatmatrix.csv"
# sites_diversity_filename = BisonNameOp.get_process_outfilename(
#     heatmatrix_filename, outpath=output_path, postfix="sites")
# species_diversity_filename = BisonNameOp.get_process_outfilename(
#     heatmatrix_filename, outpath=output_path, postfix="species")
# stats_filename = BisonNameOp.get_process_outfilename(
#     heatmatrix_filename, outpath=output_path, postfix="stats")
#
# heatmatrix = SiteMatrix(matrix_filename=heatmatrix_filename, logger=logger)
# report[REPORT.HEATMATRIX] = {"cells": heatmatrix.row_lookup_by_attribute()}
# report["total_species"] = heatmatrix.column_count
# report["present_species"] = heatmatrix.num_species
# # aka Gamma diversity
# report["total_sites"] = heatmatrix.row_count
# report["present_sites"] = heatmatrix.num_sites
#
# pam = heatmatrix.convert_to_binary()
#
# # .....................................
# # Sites/County diversity
# # .....................................
# # Alpha diversity (species richness) for each site (county)
# alpha_series = pam.alpha()
# # Beta diversity (gamma/alpha) for each site (county)
# beta_series = pam.beta()
# # Combine alpha and beta diversities to a matrix and write
# site_diversity_mtx = SiteMatrix.concat_columns((alpha_series, beta_series))
# site_diversity_mtx.write_matrix(sites_diversity_filename)
# logger.log(
#     f"Wrote site statistics to {sites_diversity_filename}.",
#     refname=script_name)
#
# # .....................................
# # Species diversity
# # .....................................
# # Omega (range size) for each species
# omega_series = pam.omega()
# # Omega proportional (mean proportional range size) for each species
# omega_pr_series = pam.omega_proportional()
# omega_mtx = SiteMatrix.concat_columns((omega_series, omega_pr_series))
# omega_mtx.write_matrix(species_diversity_filename)
