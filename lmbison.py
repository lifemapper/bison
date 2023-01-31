"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import csv
import os

from bison.common.constants import (
    GBIF, DATA_PATH, ENCODING, EXTRA_CSV_FIELD, LOG, NEW_RESOLVED_COUNTY,
    NEW_RESOLVED_STATE, POINT_BUFFER_RANGE, US_CENSUS_COUNTY)
from bison.common.log import log_output
from bison.common.util import chunk_files, delete_file, get_csv_dict_reader, identify_chunk_files

from bison.process.aggregate import (Aggregator)
from bison.process.annotate import (
    Annotator, annotate_occurrence_file, parallel_annotate)
from bison.process.geoindex import GeoResolver, GeoException


# .............................................................................
def split_files(gbif_filename, logger):
    """Split files into smaller subsets for faster processing.

    Args:
        gbif_filename (str): full filename for splitting into smaller files.
        logger (object): logger for saving relevant processing messages

    Returns:
        chunk_filenames (list): full filenames for subset files.
    """
    chunk_filenames = chunk_files(gbif_filename)
    logger.info(f"{len(chunk_filenames)} chunk files created.")
    return chunk_filenames


# .............................................................................
def annotate_occurrence_files(input_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        input_filenames (list): list of full filenames containing GBIF data for annotation.
        logger (object): logger for saving relevant processing messages

    Returns:
        annotated_filenames: full filenames for GBIF data newly annotated with state, county, RIIS assessment,
            and RIIS key.  If a file exists, do not re-annotate.
    """
    annotated_filenames = []
    if len(input_filenames) > 1:
        log_output(logger, "Annotate files in parallel: ", outlist=input_filenames)
        annotated_filenames = parallel_annotate(input_filenames, logger)
    else:
        csv_filename = input_filenames[0]
        # If this one is complete, do not re-annotate
        outfname = Annotator.construct_annotated_name(csv_filename)
        if os.path.exists(outfname):
            logger.info(f"Annotations exist in {outfname}, moving on.")
        else:
            annotated_dwc_fname = annotate_occurrence_file(
                csv_filename, logger.log_directory)
            annotated_filenames.append(annotated_dwc_fname)

        # nnsl = NNSL(riis_filename, logger=logger)
        # nnsl.read_riis()
        # # If this one is complete, do not re-annotate
        # outfname = Annotator.construct_annotated_name(csv_filename)
        # if os.path.exists(outfname):
        #     logger.info(f"Annotations exist in {outfname}, moving on.")
        # else:
        #     ant = Annotator(csv_filename, nnsl=nnsl, logger=logger)
        #     annotated_dwc_fname = ant.annotate_dwca_records()
        #     annotated_filenames.append(annotated_dwc_fname)
    return annotated_filenames


# # .............................................................................
# def summarize_annotated_files(annotated_filenames, logger):
#     """Annotate GBIF records with census state and county, and RIIS key and assessment.
#
#     Args:
#         annotated_filenames (list): list of full filenames containing annotated GBIF data.
#         logger (object): logger for saving relevant processing messages
#
#     Returns:
#         summary_filenames (list): full filenames of summaries of location, species, occurrence counts, one file
#             per each file in annotated_filenames.
#     """
#     summary_filenames = []
#     if len(annotated_filenames) > 1:
#         log_output(logger, "Summarize files in parallel: ", outlist=annotated_filenames)
#         # Does not overwrite existing summary files
#         summary_filenames = parallel_summarize(annotated_filenames, logger)
#     else:
#         ann_filename = annotated_filenames[0]
#         # Do not overwrite existing summary file
#         summary_filename = summarize_annotations(ann_filename, logger.log_directory)
#         summary_filenames.append(summary_filename)
#
#         # agg = Aggregator(ann_filename, logger=logger)
#         # # Do not overwrite existing summary file
#         # summary_filename = agg.summarize_by_file(overwrite=False)
#         # summary_filenames.append(summary_filename)
#     return summary_filenames


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
def aggregate_regions(summary_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        summary_filenames (list): list of full filenames containing GBIF data summarized by state/county for RIIS
            assessment of records.
        logger (object): logger for saving relevant processing messages

    Returns:
        state_aggregation_filenames (list): full filenames of species counts and percentages for each state.
        cty_aggregation_filename (list): full filenames of species counts and percentages for each county-state.
    """
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(summary_filenames[0], logger=logger)

    # Aggregate by region
    region_summary_filenames = agg.aggregate_regions(summary_filenames)

    return region_summary_filenames


# .............................................................................
def find_or_create_subset_files(gbif_filename, logger):
    """Find or create subset files from a large file based on the file size and number of CPUs.

    Args:
        gbif_filename (str): full filename of data file to be subsetted into chunks.
        logger (object): logger for saving relevant processing messages

    Returns:
        input_filenames (list): full filenames for subset files created from large input file.
    """
    chunk_filenames = identify_chunk_files(gbif_filename)
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
        chunk_filenames = split_files(gbif_filename, logger)

    return chunk_filenames


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
        county = fldvals[NEW_RESOLVED_COUNTY]
        state = fldvals[NEW_RESOLVED_STATE]
    return county, state, ogr_seconds


# .............................................................................
def test_bad_line(input_filenames, logger, trouble_id):
    """Test georeferencing line with gbif_id .

    Args:
        input_filenames: List of files to test, looking for troublesome data.
        logger: logger for writing messages.
        trouble_id: gbifID for bad record to find
    """
    geofile = os.path.join(DATA_PATH, US_CENSUS_COUNTY.FILE)
    geo_county = GeoResolver(geofile, US_CENSUS_COUNTY.CENSUS_BISON_MAP, logger)
    for csvfile in input_filenames:
        try:
            f = open(csvfile, "r", newline="", encoding="utf-8")
            rdr = csv.DictReader(
                f, quoting=csv.QUOTE_NONE, delimiter="\t", restkey=EXTRA_CSV_FIELD)
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
                        logger.debug(
                            f"*** Record number {rdr.line_num}, gbifID: {gbif_id} ***")

                    # Debug: examine data
                    if gbif_id == trouble_id:
                        logger.debug(
                            f"Found gbifID {trouble_id} on line {rdr.line_num}")

                    if EXTRA_CSV_FIELD in dwcrec.keys():
                        logger.debug(
                            "Extra fields detected: possible bad read for record " +
                            f"{gbif_id} on line {rdr.line_num}")

                    # Find county and state for these coords
                    try:
                        _county, _state, ogr_seconds = _find_county_state(
                            geo_county, dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD],
                            buffer_vals=POINT_BUFFER_RANGE)
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
        in_filename, GBIF.DWCA_DELIMITER, encoding=ENCODING, quote_none=True, restkey=EXTRA_CSV_FIELD)

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
                    f"Extra fields detected: possible bad read for record {gbif_id} on line {rdr.line_num}")

    except Exception as e:
        logger.error(f"Unexpected read error {e} on file {in_filename}")


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    COMMANDS = ("resolve", "split", "annotate", "summarize", "aggregate", "test")
    # import argparse
    #
    # parser = argparse.ArgumentParser(
    #     description=(
    #             "Execute one or more steps of annotating GBIF data with RIIS " +
    #             "assessments, and summarizing by species, county, and state"))
    # parser.add_argument("cmd", type=str, choices=COMMANDS, default="test_bad_data")
    # parser.add_argument(
    #     "--riis_filename", type=str, default=RIIS.SPECIES_GEO_FNAME,
    #     help="The full path to US RIIS input file.")
    # parser.add_argument(
    #     "--gbif_filename", type=str, default=GBIF.INPUT_DATA,
    #     help="The full path to GBIF input species occurrence data.")
    # parser.add_argument(
    #     "--do-split", type=str, choices=("True", "False"), default="True",
    #     help=("True to process subsetted files; False to process gbif_filename directly."))
    # parser.add_argument(
    #     "--trouble_id", type=str, default=None,
    #     help="A GBIF identifier for further examination.")
    #
    # args = parser.parse_args()
    # cmd = args.cmd
    # if cmd not in COMMANDS:
    #     raise Exception(f"Unknown command {cmd}")
    #
    # trouble_id = args.trouble_id
    # # Args may be full path, or base filename in default path
    # gbif_filename = args.gbif_filename
    # riis_filename = args.riis_filename
    # do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False
    #
    # if not os.path.exists(gbif_filename):
    #     gbif_filename = os.path.join(BIG_DATA_PATH, gbif_filename)
    # if not os.path.exists(riis_filename):
    #     riis_filename = os.path.join(DATA_PATH, riis_filename)
    #
    # # logger = get_logger(os.path.join(BIG_DATA_PATH, LOG.DIR), logname=f"main_{cmd}")
    # logname = f"main_{cmd}"
    # logger = Logger(
    #     logname, os.path.join(BIG_DATA_PATH, LOG.DIR, logname), log_console=True)
    # logger.info(f"Command: {cmd}")
    # logger.info("Start Time : {}".format(datetime.now()))
    #
    # if cmd == "resolve":
    #     resolved_riis_filename = resolve_riis_taxa(riis_filename, logger)
    #     log_output(logger, f"Resolved RIIS filename: {resolved_riis_filename}")
    #
    # elif cmd == "split":
    #     if not os.path.exists(gbif_filename):
    #         raise FileNotFoundError(f"Expected file {gbif_filename} does not exist")
    #     input_filenames = find_or_create_subset_files(gbif_filename, logger)
    #     log_output(logger, "Input filenames:", outlist=input_filenames)
    #
    # else:
    #     # Find or create subset files if requested
    #     if do_split is True:
    #         input_filenames = find_or_create_subset_files(gbif_filename, logger)
    #     else:
    #         input_filenames = [gbif_filename]
    #     # Make sure input files exist
    #     for csv_fname in input_filenames:
    #         if not os.path.exists(csv_fname):
    #             raise FileNotFoundError(f"Expected file {csv_fname} does not exist")
    #
    #     if cmd == "annotate":
    #         log_output(logger, f"Command = {cmd}")
    #         log_output(logger, "Input filenames", outlist=input_filenames)
    #         # Annotate DwC records with county, state, and if found,
    #         # RIIS assessment and RIIS occurrenceID
    #         annotated_filenames = annotate_occurrence_files(input_filenames, logger)
    #         log_output(
    #             logger, "Newly annotated filenames:", outlist=annotated_filenames)
    #
    #     elif cmd == "summarize":
    #         annotated_filenames = [
    #             Annotator.construct_annotated_name(csvfile)
    #             for csvfile in input_filenames]
    #         # Summarize each annotated file by region (state and county) write summary to a file
    #         summary_filenames = summarize_annotated_files(annotated_filenames, logger)
    #         log_output(
    #             logger, "Aggregated county/state filenames:", outlist=summary_filenames)
    #
    #     elif cmd == "aggregate":
    #         annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
    #         summary_filenames = [Aggregator.construct_summary_name(annfile) for annfile in annotated_filenames]
    #         # Aggregate all summary files then write summaries for each region to its own file
    #         region_assess_summary_filenames = aggregate_summaries(summary_filenames, logger)
    #         log_output(logger, "Region filenames, assessment filename:", outlist=region_assess_summary_filenames)
    #
    #     elif cmd == "test":
    #         record_counter = Counter(gbif_filename, do_split=True, logger=logger)
    #         record_counter.compare_counts()
    #         # annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
    #         # assessments = Counter.count_assessments(annotated_filenames[0])
    #         # check_further = True
    #         # for ass, count in assessments.items():
    #         #     if count == 0:
    #         #         check_further = False
    #         #         logger.warn(f"Zero records found with {ass} assessment in {input_filenames[0]}")
    #         # if check_further is True:
    #         #     record_counter = Counter(gbif_filename, do_split=True, logger=logger)
    #         #     record_counter.compare_counts()
    #
    #     elif cmd == "test_bad_data":
    #         test_bad_line(input_filenames, logger, trouble_id)
    #
    #     elif cmd == "find_bad_record":
    #         read_bad_line(gbif_filename, logger, gbif_id=None, line_num=9868792)
    #
    #     else:
    #         logger.error(f"Unsupported command '{cmd}'")
    #
    # logger.info("End Time : {}".format(datetime.now()))
