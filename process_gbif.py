"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import csv
from datetime import datetime
import json
import os

from bison.common.constants import (
    APPEND_TO_DWC, CONFIG_PARAM, LMBISON_PROCESS, GBIF, ENCODING, EXTRA_CSV_FIELD, LOG,
    REGION, REPORT, RIIS_DATA)
from bison.common.log import Logger
from bison.common.util import BisonNameOp, Chunker, delete_file, get_csv_dict_reader
from bison.process.aggregate import Aggregator
from bison.process.annotate import Annotator, parallel_annotate
from bison.process.geoindex import GeoResolver, GeoException
from bison.process.sanity_check import Counter
from bison.provider.riis_data import RIIS
from bison.tools._config_parser import get_common_arguments

script_name = os.path.splitext(os.path.basename(__file__))[0]
DESCRIPTION = """Execute one or more steps of annotating GBIF data with RIIS
                assessments, and summarizing by species, county, and state"""
COMMANDS = ("resolve", "split", "annotate", "summarize", "aggregate", "test")
# .............................................................................
PARAMETERS = {
    "required":
        {
            "command":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.CHOICES: COMMANDS,
                    CONFIG_PARAM.HELP:
                        "Full filename of input USGS RIIS data in CSV format."
                },
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
            "geoinput_path":
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
def a_resolve_riis_taxa(riis_filename, output_path, logger, overwrite=False):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for RIIS data records.
        output_path (str): Destination directory for subset files.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing resolved file.

    Returns:
        riis_resolved_filename: output file containing RIIS records annotated with GBIF
            accepted names.
    """
    nnsl = RIIS(riis_filename, logger=logger)
    # Update species data
    try:
        report = nnsl.resolve_riis_to_gbif_taxa(output_path)
        logger.info(
            f"Found {report[REPORT.TAXA_RESOLVED]} names, "
            f"{report[REPORT.RECORDS_UPDATED]} updated, "
            f"{report[REPORT.RECORDS_OUTPUT]} written "
            f"of total {report[REPORT.RIIS_IDENTIFIER]} from {riis_filename} "
            f"to {report[REPORT.OUTFILE]}.")
    except Exception as e:
        logger.error(f"Unexpected failure {e} in resolve_riis_taxa")
    else:
        if report[REPORT.RECORDS_OUTPUT] != RIIS_DATA.SPECIES_GEO_DATA_COUNT:
            logger.debug(
                f"Wrote {report[REPORT.RECORDS_OUTPUT]} RIIS records, expecting "
                f"{RIIS_DATA.SPECIES_GEO_DATA_COUNT}")
    return report


# .............................................................................
def c_summarize_annotated_files(annotated_filenames, output_path, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        annotated_filenames (list): full filenames containing annotated GBIF data.
        output_path (str): Destination directory for summary files.
        logger (object): logger for saving relevant processing messages

    Returns:
        summary_filenames (list): full filenames of summaries of location, species,
            occurrence counts, one file per each file in annotated_filenames.

    Note:
        This process is fast, and need not be performed in parallel
    """
    summary_filenames = []
    for ann_fname in annotated_filenames:
        agg = Aggregator(logger)

        # Overwrite existing summary
        rpt = agg.summarize_annotated_recs_by_location(
            ann_fname, output_path, overwrite=True)
        summary_filenames.append(rpt["annotated_filename"])

    # Aggregate all the subset summaries into a single
    # Summary data written to report["full_summary_filename"]
    if len(summary_filenames) > 1:
        report = agg.summarize_summaries(summary_filenames, output_path)
    else:
        report = rpt

    return report


# .............................................................................
def b_annotate_occurrence_files(
        occ_filenames, riis_annotated_filename, geoinput_path, output_path, logger,
        log_path, run_parallel=False):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        occ_filenames (list): list of full filenames containing GBIF data for
            annotation.
        riis_annotated_filename (str): Full path to RIIS data annotated with GBIF names.
        geoinput_path (str): Base directory containing geospatial region files
        output_path (str): Destination directory for output files.
        logger (object): logger for saving relevant processing messages
        log_path (str): Destination directory for log files.
        run_parallel (bool): Flag indicating whether to process subset files in parallel

    Returns:
        annotated_filenames: full filenames for GBIF data newly annotated with state,
            county, RIIS assessment, and RIIS key.  If a file exists, do not annotate.
    """
    if run_parallel and len(occ_filenames) > 1:
        log_list(logger, "Annotate files in parallel: ", occ_filenames)
        reports = parallel_annotate(
            occ_filenames, riis_annotated_filename, geoinput_path, output_path, logger)

    else:
        reports = {}
        ant = Annotator(
            geoinput_path, logger, riis_with_gbif_filename=riis_annotated_filename)
        for occ_fname in occ_filenames:
            logger.log(
                f"Start Time: {datetime.now()}: Submit {occ_fname} for annotation",
                refname=script_name)

            basename = os.path.splitext(os.path.basename(occ_fname))[0]
            logname = f"annotate_{basename}"
            log_filename = os.path.join(log_path, f"{logname}.log")
            logger = Logger(
                logname, log_filename=log_filename, log_console=False)

            # Add locality-intersections and RIIS determinations to GBIF DwC records
            rpt = ant.annotate_dwca_records(occ_fname, output_path)

            reports[basename] = rpt

    return reports


# .............................................................................
def d_aggregate_summary_by_region(
        summary_filename, resolved_riis_filename, output_path, logger):
    """Aggregate annotated GBIF records with region, and RIIS key and assessment.

    Args:
        summary_filename (str): Full filename containing summarized
            GBIF data by region for RIIS assessment of records.
        resolved_riis_filename (str): full filename of RIIS data annotated with GBIF
            names.
        output_path (str): Destination directory for summary files.
        logger (object): logger for saving relevant processing messages

    Returns:
        state_aggregation_filenames (list): full filenames of species counts and
            percentages for each state.
        cty_aggregation_filename (list): full filenames of species counts and
            percentages for each county-state.
    """
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(logger)
    report = agg.aggregate_file_summary_for_regions(
            summary_filename, resolved_riis_filename, output_path)

    return report


# .............................................................................
def a_find_or_create_subset_files(gbif_filename, output_path, logger):
    """Find or create subset files from a large file based on the file size and CPUs.

    Args:
        gbif_filename (str): full filename of data file to be subsetted into chunks.
        output_path (str): Destination directory for subset files.
        logger (object): logger for saving relevant processing messages

    Returns:
        raw_filenames (list): full filenames for subset files created from large
            input file.
        output_path (str): Destination directory for subset files.
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
    logger.log(msg)


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
def test_bad_line(trouble_id, raw_filenames, geoinput_path, logger):
    """Test georeferencing line with gbif_id .

    Args:
        trouble_id: gbifID for bad record to find
        raw_filenames: List of files to test, looking for troublesome data.
        geoinput_path: Full base directory for geospatial data
        logger: logger for writing messages.
    """
    geofile = os.path.join(geoinput_path, REGION.COUNTY["file"])
    geo_county = GeoResolver(geofile, REGION.COUNTY["map"], logger)
    for csvfile in raw_filenames:
        try:
            f = open(csvfile, "r", newline="", encoding="utf-8")
            rdr = csv.DictReader(f, quoting=csv.QUOTE_NONE, delimiter="\t",
                                 restkey=EXTRA_CSV_FIELD)
        except Exception as e:
            logger.error(f"Unexpected open error {e} on file {csvfile}")
        else:
            logger.info(f"Opened file {csvfile}")
            try:
                # iterate over DwC records
                dwcrec = next(rdr)
                while dwcrec is not None:
                    gbif_id = dwcrec[GBIF.ID_FLD]
                    if (rdr.line_num % LOG.INTERVAL) == 0:
                        logger.debug(f"*** Record number {rdr.line_num}, gbifID: {gbif_id} ***")

                    # Debug: examine data
                    if gbif_id == trouble_id:
                        logger.debug(f"Found gbifID {trouble_id} on line {rdr.line_num}")

                    if EXTRA_CSV_FIELD in dwcrec.keys():
                        logger.debug(
                            "Extra fields detected: possible bad read for record "
                            f"{gbif_id} on line {rdr.line_num}")

                    # Find county and state for these coords
                    try:
                        _county, _state, ogr_seconds = _find_county_state(
                            geo_county, dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD],
                            buffer_vals=REGION.COUNTY["buffer"])
                    except ValueError as e:
                        logger.error(f"Record gbifID: {gbif_id}: {e}")
                    except GeoException as e:
                        logger.error(f"Record gbifID: {gbif_id}: {e}")
                    if ogr_seconds > 0.75:
                        logger.debug(
                            f"Record gbifID: {gbif_id}; OGR time {ogr_seconds}")

                    dwcrec = next(rdr)

            except Exception as e:
                logger.error(f"Unexpected read error {e} on file {csvfile}")


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
                logger.debug(f"*** Record number {rdr.line_num}, gbifID: {gbif_id} ***")

            # Debug: examine data
            if gbif_id == dwcrec[GBIF.ID_FLD]:
                logger.debug(f"Found gbifID {gbif_id} on line {rdr.line_num}")
            elif rdr.line_num == line_num:
                logger.debug(f"Found line {rdr.line_num}")

            if EXTRA_CSV_FIELD in dwcrec.keys():
                logger.debug(
                    f"Extra fields detected: possible bad read for record {gbif_id} on "
                    f"line {rdr.line_num}")

    except Exception as e:
        logger.error(f"Unexpected read error {e} on file {in_filename}")


# .............................................................................
def _prepare_args(config):
    """Prepare arguments, assembling paths, filenames, and splitting data if needed.

    Args:
        config (dict): user-provided parameters

    Returns:
        riis_annotated_filename: Full annotated RIIS filename
        log_path: destination path for logfiles
        raw_filenames: input CVS file(s) of original GBIF records
        annotated_filenames: output file(s) of annotated GBIF records
        summary_filenames: output file(s) of subset summaries of GBIF annotated records
        full_summary_filename: : output file of all annotated GBIF records

    Raises:
        FileNotFoundError: on missing input occurrence file.
    """
    # Input files, logfile, report file must be full path
    # riis_filename =
    # inpath, fname = os.path.split(riis_filename)
    # basename, ext = os.path.splitext(fname)
    # riis_annotated_filename = BisonNameOp.get_process_outfilename(
    #     config["riis_filename"], outpath=config["process_path"],
    #     step_or_process=LMBISON_PROCESS.RESOLVE)
    # os.path.join(inpath, f"{basename}_resolved{ext}")
    log_path, _ = os.path.split(config["log_filename"])
    # Geospatial/output/processing filenames will be generated, and located in paths
    # Process entire GBIF file, or do it in chunks
    booltmp = str(config["do_split"]).lower()
    # First determine whether to process one file or smaller subsets
    if (booltmp in ("yes", "y", "true", "1")) or config["command"] == "split":
        do_split = True

    infile = config["gbif_filename"]
    if not os.path.exists(infile):
        raise FileNotFoundError(f"Missing input file {infile}")

    # First determine whether to process one file or smaller subsets
    if do_split is True:
        # Find existing or create subset files
        raw_filenames = a_find_or_create_subset_files(
            infile, config["process_path"], logger)
    else:
        raw_filenames = [infile]

    riis_annotated_filename = BisonNameOp.get_process_outfilename(
        config["riis_filename"], outpath=config["process_path"],
        step_or_process=LMBISON_PROCESS.RESOLVE)

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
        infile, outpath=config["process_path"],
        step_or_process=LMBISON_PROCESS.SUMMARIZE)

    return (
        riis_annotated_filename, log_path,
        raw_filenames, annotated_filenames, summary_filenames,
        full_summary_filename
    )


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
    (riis_annotated_filename, log_path,
        raw_filenames, annotated_filenames, summary_filenames,
        full_summary_filename) = _prepare_args(config)

    # Make sure input files exist
    for csv_fname in raw_filenames:
        if not os.path.exists(csv_fname):
            raise FileNotFoundError(f"Expected file {csv_fname} does not exist")
    log_list(logger, "Input filenames:", raw_filenames)

    if config["command"] == "resolve":
        report = a_resolve_riis_taxa(
            config["riis_filename"], riis_annotated_filename, logger)
        logger.log(f"Resolved RIIS filename: {report[REPORT.OUTFILE]}")

    elif config["command"] == "annotate":
        log_list(logger, "Input filenames", raw_filenames)
        # Annotate DwC records with regions, and if found, RIIS determination
        report = b_annotate_occurrence_files(
            raw_filenames, riis_annotated_filename, config["geoinput_path"],
            config["process_path"], logger, log_path, run_parallel=False)
        log_list(
            logger, "Newly annotated filenames:",
            [rpt[REPORT.OUTFILE] for rpt in report.values()])

    elif config["command"] == "summarize":
        # Summarize each annotated file by region, write summary to a file
        report = c_summarize_annotated_files(
            annotated_filenames, config["output_path"], logger)
        logger.log("Summary of annotations", report[REPORT.OUTFILE])

    elif config["command"] == "aggregate":
        # Write summaries for each region to its own file
        report = d_aggregate_summary_by_region(
            full_summary_filename, config["output_path"], log_path, logger)
        for region_type in report.keys():
            log_list(
                logger, "Region filenames, assessment filename:",
                report[region_type][REPORT.OUTFILE])

    elif config["command"] == "test":
        # Test summarized summaries
        full_summary_filename = BisonNameOp.get_process_outfilename(
            summary_filenames, outpath=config["output_path"],
            step_or_process=LMBISON_PROCESS.AGGREGATE)

        report = Counter.compare_location_species_counts(
            summary_filenames, full_summary_filename, logger)

    elif config["command"] == "test_bad_data" and config["gbif_id"] is not None:
        test_bad_line(
            config["gbif_id"], config["examine_filenames"], config["geoinput_path"], logger)

    elif config["command"] == "find_bad_record":
        # Should only be one file in the examine_filenames list
        read_bad_line(
            config["examine_filenames"][0], logger, gbif_id=config["gbif_id"],
            line_num=config["line_num"])

    else:
        logger.error(f"Unsupported command {config['command']}")

    return report


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    # log_name = f"{script_name}_{start.isoformat()}"
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    logger.log(f"Command: {config['command']}")
    logger.log(f"Start Time : {datetime.now().isoformat()}")

    report = execute_command(config, logger)
    # If the output report was requested, write it
    if report_filename:
        try:
            with open(report_filename, mode='wt') as out_file:
                json.dump(report, out_file, indent=4)
        except OSError:
            raise
        except IOError:
            raise
        logger.log(
            f"Wrote report file to {report_filename}", refname=script_name)

    logger.info(f"End Time : {datetime.now().isoformat()}")
