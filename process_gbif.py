"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import csv
from datetime import datetime
import os

from bison.process.aggregate import (
    Aggregator, parallel_summarize, summarize_annotations)
from bison.process.annotate import (
    Annotator, annotate_occurrence_file, parallel_annotate)
from bison.process.geoindex import GeoResolver, GeoException
from bison.process.sanity_check import Counter

from bison.common.constants import CONFIG_PARAM, DWC_PROCESS
from bison.common.constants import (
    APPEND_TO_DWC, GBIF, DATA_PATH, ENCODING, EXTRA_CSV_FIELD, LOG, REGION, RIIS_DATA)
from bison.common.log import Logger
from bison.common.util import BisonNameOp, Chunker, delete_file, get_csv_dict_reader

from bison.tools._config_parser import get_common_arguments
from bison.provider.riis_data import RIIS

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
            "do-split":
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

        }
}

# .............................................................................
def split_files(gbif_filename, output_path, logger):
    """Split files into smaller subsets for faster processing.

    Args:
        gbif_filename (str): full filename for splitting into smaller files.
        output_path (str): Destination directory for subset files.
        logger (object): logger for saving relevant processing messages

    Returns:
        chunk_filenames (list): full filenames for subset files.
    """
    chunk_filenames = Chunker.chunk_files(gbif_filename, output_path, logger)
    logger.info(f"{len(chunk_filenames)} chunk files created.")
    return chunk_filenames


# .............................................................................
def resolve_riis_taxa(riis_filename, riis_resolved_filename, logger, overwrite=False):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for RIIS data records.
        riis_resolved_filename (str): full filename for annotated RIIS recors
        logger (object): logger for saving relevant processing messages
        overwrite (bool): Flag indicating whether to overwrite existing resolved file.
    """
    nnsl = RIIS(riis_filename, logger=logger)
    # Update species data
    try:
        nnsl.resolve_riis_to_gbif_taxa(riis_resolved_filename, overwrite=overwrite)
        logger.info(
            f"Resolved {len(nnsl.by_riis_id)} taxa in {riis_filename}, next, write.")
        count = nnsl.write_resolved_riis(overwrite=True)
        logger.info(f"Wrote {count} records to {riis_resolved_filename}.")
    except Exception as e:
        logger.error(f"Unexpected failure {e} in resolve_riis_taxa")
    else:
        if count != RIIS_DATA.DATA_COUNT:
            logger.debug(
                f"Resolved {count} RIIS records, expecting "
                f"{RIIS_DATA.SPECIES_GEO_DATA_COUNT}")


# .............................................................................
def annotate_occurrence_files(
        occ_filenames, riis_annotated_filename, geoinput_path, output_path, logger,
        log_path, run_parallel=False):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        occ_filenames (list): list of full filenames containing GBIF data for
            annotation.
        logger (object): logger for saving relevant processing messages

    Returns:
        annotated_filenames: full filenames for GBIF data newly annotated with state,
            county, RIIS assessment, and RIIS key.  If a file exists, do not annotate.
    """
    annotated_filenames = []

    if run_parallel and len(input_filenames) > 1:
        log_list(logger, "Annotate files in parallel: ", input_filenames)
        annotated_filenames = parallel_annotate(
            occ_filenames, riis_annotated_filename, logger, geoinput_path, output_path)

    else:
        annotated_filenames = []
        reports = []
        for occ_fname in occ_filenames:
            logger.log(
                f"Start Time: {datetime.now()}: Submit {occ_fname} for annotation",
                refname=script_name)

            basename = os.path.split(os.path.basename(occ_fname))[0]

            logname = f"annotate_{basename}"
            log_filename = os.path.join(log_path, f"{logname}.log")
            logger = Logger(
                logname, log_filename=log_filename, log_console=False)

            # Add locality-intersections and RIIS determinations to GBIF DwC records
            process_rpt = annotate_occurrence_file(
                occ_fname, riis_annotated_filename, geoinput_path, output_path,
                log_path)
            annotated_filenames.append(process_rpt["dwc_with_geo_and_riis_filename"])
            reports.append(process_rpt)

    return reports, annotated_filenames


# .............................................................................
def summarize_annotated_files(annotated_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        annotated_filenames (list): full filenames containing annotated GBIF data.
        logger (object): logger for saving relevant processing messages

    Returns:
        summary_filenames (list): full filenames of summaries of location, species,
            occurrence counts, one file per each file in annotated_filenames.
    """
    summary_filenames = []
    if len(annotated_filenames) > 1:
        log_list(logger, "Summarize files in parallel: ", annotated_filenames)
        # Does not overwrite existing summary files
        summary_filenames = parallel_summarize(annotated_filenames, logger)
    else:
        ann_filename = annotated_filenames[0]
        # Do not overwrite existing summary file
        summary_filename = summarize_annotations(ann_filename)
        summary_filenames.append(summary_filename)

        # agg = Aggregator(ann_filename, logger=logger)
        # # Do not overwrite existing summary file
        # summary_filename = agg.summarize_by_file(overwrite=False)
        # summary_filenames.append(summary_filename)
    return summary_filenames


# .............................................................................
def aggregate_summaries(summary_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        summary_filenames (list): list of full filenames containing GBIF data summarized
            by state/county for RIIS assessment of records.
        logger (object): logger for saving relevant processing messages

    Returns:
        state_aggregation_filenames (list): full filenames of species counts and
            percentages for each state.
        cty_aggregation_filename (list): full filenames of species counts and
            percentages for each county-state.
    """
    aggregated_filenames = []
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(summary_filenames[0], logger=logger)

    # Aggregate by region
    region_summary_filenames = agg.aggregate_regions(summary_filenames)
    aggregated_filenames.extend(region_summary_filenames)

    # Aggregate by RIIS assessment
    assess_summary_filename = agg.aggregate_assessments(region_summary_filenames)
    aggregated_filenames.append(assess_summary_filename)

    return aggregated_filenames


# .............................................................................
def summarize_regions(summary_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        summary_filenames (list): list of full filenames containing GBIF data summarized
            by state/county for RIIS assessment of records.
        logger (object): logger for saving relevant processing messages

    Returns:
        state_aggregation_filenames (list): full filenames of species counts and
            percentages for each state.
        cty_aggregation_filename (list): full filenames of species counts and
            percentages for each county-state.
    """
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(summary_filenames[0], logger=logger)

    # Aggregate by region
    region_summary_filenames = agg.summarize_regions(summary_filenames)

    return region_summary_filenames


# .............................................................................
def find_or_create_subset_files(gbif_filename, output_path, logger):
    """Find or create subset files from a large file based on the file size and CPUs.

    Args:
        gbif_filename (str): full filename of data file to be subsetted into chunks.
        logger (object): logger for saving relevant processing messages

    Returns:
        input_filenames (list): full filenames for subset files created from large
            input file.
        output_path (str): Destination directory for subset files.
    """
    chunk_filenames = Chunker.identify_chunk_files(gbif_filename)
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
        chunk_filenames = split_files(gbif_filename, output_path, logger)

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
        county = fldvals[APPEND_TO_DWC.RESOLVED_COUNTY]
        state = fldvals[APPEND_TO_DWC.RESOLVED_STATE]
    return county, state, ogr_seconds


# .............................................................................
def test_bad_line(input_filenames, logger, trouble_id):
    """Test georeferencing line with gbif_id .

    Args:
        input_filenames: List of files to test, looking for troublesome data.
        logger: logger for writing messages.
        trouble_id: gbifID for bad record to find
    """
    geofile = os.path.join(DATA_PATH, REGION.COUNTY["file"])
    geo_county = GeoResolver(geofile, REGION.COUNTY["map"], logger)
    for csvfile in input_filenames:
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
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    # log_name = f"{script_name}_{start.isoformat()}"
    config, logger, report_filename = get_common_arguments(
        script_name, DESCRIPTION, PARAMETERS)

    # Input files, logfile, report file must be full path
    gbif_filename = config["gbif_filename"]
    riis_filename = config["riis_filename"]
    inpath, fname = os.path.split(riis_filename)
    basename, ext = os.path.splitext(fname)
    riis_annotated_filename = os.path.join(inpath, f"{basename}_resolved{ext}")
    riis_annotated_filename = os.path.join(inpath, f"{basename}_resolved{ext}")
    log_filename = config["log_filename"]
    log_path, _ = os.path.split(log_filename)
    report_filename = config["report_filename"]
    # Geospatial/output/processing filenames will be generated, and located in paths
    process_path = config["process_path"]
    output_path = config["output_path"]
    geoinput_path = config["geoinput_path"]
    # Process entire GBIF file, or do it in chunks
    booltmp = config["do_split"].lower()
    do_split = True if booltmp in ("yes", "y", "true", "1") else False

    cmd = config["command"]
    logger.log(f"Command: {cmd}")
    logger.log(f"Start Time : {datetime.now().isoformat()}")

    if cmd == "resolve":
        resolved_riis_filename = resolve_riis_taxa(
            riis_filename, riis_annotated_filename, logger)
        logger.log(f"Resolved RIIS filename: {resolved_riis_filename}")

    elif cmd == "split":
        if not os.path.exists(gbif_filename):
            raise FileNotFoundError(f"Expected file {gbif_filename} does not exist")
        input_filenames = find_or_create_subset_files(
            gbif_filename, process_path, logger)
        log_list(logger, "Input filenames:", input_filenames)

    else:
        # Find or create subset files if requested
        if do_split is True:
            input_filenames = find_or_create_subset_files(
                gbif_filename, process_path, logger)
        else:
            input_filenames = [gbif_filename]
        # Make sure input files exist
        for csv_fname in input_filenames:
            if not os.path.exists(csv_fname):
                raise FileNotFoundError(f"Expected file {csv_fname} does not exist")

        if cmd == "annotate":
            logger.log(f"Command = {cmd}")
            log_list(logger, "Input filenames", input_filenames)
            # Annotate DwC records with county, state, and if found, RIIS assessment
            # and RIIS occurrenceID
            reports, annotated_filenames = annotate_occurrence_files(
                input_filenames, riis_annotated_filename, process_path, geoinput_path,
                logger, log_path, run_parallel=False)
            log_list(logger, "Newly annotated filenames:", annotated_filenames)

        elif cmd == "summarize":
            annotated_filenames = [
                BisonNameOp.get_out_process_filename(
                    csvfile, outpath=output_path, step_or_process=DWC_PROCESS.ANNOTATE)
                for csvfile in input_filenames]
            # Summarize each annotated file by region, write summary to a file
            summary_filenames = summarize_annotated_files(annotated_filenames, logger)
            log_list(logger, "Aggregated county/state filenames:", summary_filenames)

        elif cmd == "aggregate":
            summary_filenames = [
                BisonNameOp.get_out_process_filename(
                    csvfile, outpath=output_path, step_or_process=DWC_PROCESS.SUMMARIZE)
                for csvfile in input_filenames]
            # Aggregate all summary files then write summaries for each region to its own file
            region_assess_summary_filenames = aggregate_summaries(
                summary_filenames, logger)
            log_list(
                logger, "Region filenames, assessment filename:",
                region_assess_summary_filenames)

        elif cmd == "test":
            record_counter = Counter(gbif_filename, do_split=True, logger=logger)
            record_counter.compare_counts()
            # annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
            # assessments = Counter.count_assessments(annotated_filenames[0])
            # check_further = True
            # for ass, count in assessments.items():
            #     if count == 0:
            #         check_further = False
            #         logger.warn(f"Zero records found with {ass} assessment in {input_filenames[0]}")
            # if check_further is True:
            #     record_counter = Counter(gbif_filename, do_split=True, logger=logger)
            #     record_counter.compare_counts()

        elif cmd == "test_bad_data":
            test_bad_line(input_filenames, logger)

        elif cmd == "find_bad_record":
            read_bad_line(gbif_filename, logger, gbif_id=None, line_num=9868792)

        else:
            logger.error(f"Unsupported command '{cmd}'")

    logger.info(f"End Time : {datetime.now().isoformat()}")
