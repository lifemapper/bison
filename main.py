"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import csv
import glob
import os

from bison.common.aggregate import Aggregator
from bison.common.annotate import Annotator
from bison.common.constants import (
    GBIF, DATA_PATH, EXTRA_CSV_FIELD, LOG, NEW_RESOLVED_COUNTY, NEW_RESOLVED_STATE, POINT_BUFFER_RANGE, RIIS_SPECIES,
    US_CENSUS_COUNTY)
from bison.common.geoindex import GeoResolver, GeoException
from bison.common.riis import NNSL
from bison.tools.util import chunk_files, delete_file, get_logger, identify_chunk_files


# .............................................................................
def split_files(big_csv_filename, logger):
    """Split files into smaller subsets for faster processing.

    Args:
        big_csv_filename (str): full filename for splitting into smaller files.
        logger (object): logger for saving relevant processing messages

    Returns:
        chunk_filenames (list): full filenames for subset files.
    """
    chunk_filenames = chunk_files(big_csv_filename)
    logger.info(f"Chunk files created: {chunk_filenames}")
    return chunk_filenames


# .............................................................................
def resolve_riis_taxa(riis_filename, logger):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for RIIS data records.
        logger (object): logger for saving relevant processing messages

    Returns:
        resolved_riis_filename: full output filename for RIIS data records with updated taxa and taxonKeys from GBIF.
    """
    nnsl = NNSL(riis_filename, logger=logger)
    # Update species data
    nnsl.resolve_riis_to_gbif_taxa()
    count = nnsl.write_resolved_riis()
    if count != RIIS_SPECIES.DATA_COUNT:
        logger.debug(f"Resolved {count} RIIS records, expecting {RIIS_SPECIES.DATA_COUNT}")
    resolved_riis_filename = nnsl.gbif_resolved_riis_fname
    return resolved_riis_filename


# .............................................................................
def annotate_occurrence_files(input_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        input_filenames (list): list of full filenames containing GBIF data for annotation.
        logger (object): logger for saving relevant processing messages

    Returns:
        annotated_filenames: fill filenames for GBIF data annotated with state, county, RIIS assessment, and RIIS key.
    """
    annotated_filenames = []
    nnsl = NNSL(riis_filename, logger=logger)
    nnsl.read_riis(read_resolved=True)
    for csv_filename in input_filenames:
        ant = Annotator(csv_filename, nnsl=nnsl, logger=logger)
        annotated_dwc_fname = ant.annotate_dwca_records()
        annotated_filenames.append(annotated_dwc_fname)
    return annotated_filenames


# .............................................................................
def summarize_annotations(annotated_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        annotated_filenames (list): list of full filenames containing annotated GBIF data.
        logger (object): logger for saving relevant processing messages

    Returns:
        summary_filenames (list): full filenames of summaries of location, species, occurrence counts, one file
            per each file in annotated_filenames.
    """
    summary_filenames = []
    for csv_filename in annotated_filenames:
        agg = Aggregator(csv_filename, logger=logger)
        summary_filename = agg.summarize_by_file()
        summary_filenames.append(summary_filename)
    return summary_filenames


# .............................................................................
def summarize_region_assessment(summary_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        summary_filenames (list): list of full filenames containing GBIF data summarized by state/county for RIIS
            assessment of records.
        logger (object): logger for saving relevant processing messages

    Returns:
        state_aggregation_filenames (list): full filenames of species counts and percentages for each state.
        cty_aggregation_filename (list): full filenames of species counts and percentages for each county-state.
    """
    aggregated_filenames = []
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(summary_filenames[0], logger=logger)

    # Aggregate by region
    region_summary_filenames = agg.summarize_regions(summary_filenames)
    aggregated_filenames.extend(region_summary_filenames)

    # Aggregate by RIIS assessment
    assess_summary_filename = agg.summarize_assessments(region_summary_filenames)
    aggregated_filenames.append(assess_summary_filename)

    return aggregated_filenames


# .............................................................................
def summarize_regions(summary_filenames, logger):
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
    region_summary_filenames = agg.summarize_regions(summary_filenames)

    return region_summary_filenames


# .............................................................................
def summarize_assessments(region_summary_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        region_summary_filenames (list): list of full filenames containing GBIF data summarized by state/county for RIIS
            assessment of records.
        logger (object): logger for saving relevant processing messages

    Returns:
        assess_summary_filename (str): full filename of occurrence and species counts and percentages for each state.
    """
    # Create a new Aggregator, ignore file used for construction,
    agg = Aggregator(region_summary_filenames[0], logger=logger)

    # Aggregate by RIIS assessment
    assess_summary_filename = agg.summarize_assessments(region_summary_filenames)

    return assess_summary_filename


# .............................................................................
def find_or_create_subset_files(big_csv_filename, logger):
    """Find or create subset files from a large file based on the file size and number of CPUs.

    Args:
        big_csv_filename (str): full filename of data file to be subsetted into chunks.
        logger (object): logger for saving relevant processing messages

    Returns:
        input_filenames (list): full filenames for subset files created from large input file.
    """
    chunk_filenames = identify_chunk_files(big_csv_filename)
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
        chunk_filenames = split_files(big_csv_filename, logger)

    return chunk_filenames


# .............................................................................
def log_output(logger, msg, outlist=None):
    """Log output.

    Args:
        logger: logger
        msg: Message
        outlist: optional list of strings to be printed on individual lines
    """
    msg = f"{msg}\n"
    if outlist is not None:
        for elt in outlist:
            msg += f"  {elt}\n"
    logger.info(msg)


# ...............................................
def _find_county_state(geo_county, lon, lat, buffer_vals):
    county = state = None
    if None not in (lon, lat):
        # Intersect coordinates with county boundaries for state and county values
        try:
            fldvals, ogr_seconds = geo_county.find_enclosing_polygon(lon, lat, buffer_vals=buffer_vals)
        except ValueError:
            raise
        except GeoException:
            raise
        county = fldvals[NEW_RESOLVED_COUNTY]
        state = fldvals[NEW_RESOLVED_STATE]
    return county, state, ogr_seconds


# .............................................................................
def test_bad_line(input_filenames, logger):
    """Test troublesome lines.

    Args:
        input_filenames: List of files to test, looking for troublesome data.
        logger: logger for writing messages.
    """
    trouble = "1698055779"
    trouble_next = "1698058398"
    geo_county = GeoResolver(US_CENSUS_COUNTY.FILE, US_CENSUS_COUNTY.CENSUS_BISON_MAP, logger)
    for csvfile in input_filenames:
        try:
            f = open(csvfile, "r", newline="", encoding="utf-8")
            rdr = csv.DictReader(f, quoting=csv.QUOTE_NONE, delimiter="\t", restkey=EXTRA_CSV_FIELD)
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
                    if gbif_id == trouble:
                        logger.debug(f"Found gbifID {trouble} on line {rdr.line_num}")
                    if gbif_id == trouble_next:
                        logger.debug(f"Not so troubling on line {rdr.line_num}")

                    if EXTRA_CSV_FIELD in dwcrec.keys():
                        logger.debug(f"Extra fields detected: possible bad read for record {gbif_id} on line {rdr.line_num}")

                    # Find county and state for these coords
                    try:
                        _county, _state, ogr_seconds = _find_county_state(
                            geo_county, dwcrec[GBIF.LON_FLD], dwcrec[GBIF.LAT_FLD], buffer_vals=POINT_BUFFER_RANGE)
                    except ValueError as e:
                        logger.error(f"Record gbifID: {gbif_id}: {e}")
                    except GeoException as e:
                        logger.error(f"Record gbifID: {gbif_id}: {e}")
                    if ogr_seconds > 0.75:
                        logger.debug(f"Record gbifID: {gbif_id}; OGR time {ogr_seconds}")

                    dwcrec = next(rdr)

            except Exception as e:
                logger.error(f"Unexpected read error {e} on file {csvfile}")


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    riis_filename = os.path.join(DATA_PATH, RIIS_SPECIES.FNAME)
    gbif_infile = os.path.join(DATA_PATH, GBIF.INPUT_DATA)
    gbif_infile = os.path.join(DATA_PATH, "gbif_2022-02-15.csv")

    parser = argparse.ArgumentParser(
        description="Execute one or more steps of annotating GBIF data with RIIS assessments, and summarizing by species, county, and state")
    parser.add_argument(
        "cmd", type=str, choices=("resolve", "split", "annotate", "summarize", "aggregate", "full", "test_bad_data"), default="test_bad_data")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "--do-split", type=str, choices=("True", "False"), default="True",
        help="True to process subsetted/chunked files; False to process big_csv_filename directly.  Command 'split' assumes do_subset is True")

    args = parser.parse_args()
    cmd = args.cmd
    big_csv_filename = os.path.join(DATA_PATH, args.big_csv_filename)
    do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False

    # ...............................................
    # Test data
    # ...............................................
    cmd = "summarize_assessments"
    big_csv_filename = os.path.join(DATA_PATH, "gbif_2022-03-16_100k.csv")
    # ...............................................
    # ...............................................

    logger = get_logger(DATA_PATH, logname=f"main_{cmd}")
    logger.info(f"Command: {cmd}")
    if cmd == "resolve":
        resolved_riis_filename = resolve_riis_taxa(riis_filename, logger)
        print(resolved_riis_filename)
        log_output(logger, f"Resolved RIIS filename: {resolved_riis_filename}")
    elif cmd == "split":
        input_filenames = find_or_create_subset_files(big_csv_filename, logger)
        log_output(logger, "Input filenames:", outlist=input_filenames)
    else:
        if do_split is True:
            input_filenames = find_or_create_subset_files(big_csv_filename, logger)
        else:
            input_filenames = [big_csv_filename]
        # Make sure files to be processed exist
        for csv_fname in input_filenames:
            if not os.path.exists(csv_fname):
                raise FileNotFoundError(f"Expected file {csv_fname} does not exist")

        if cmd == "annotate":
            # Annotate DwC records with county, state, and if found, RIIS assessment and RIIS occurrenceID
            annotated_filenames = annotate_occurrence_files(input_filenames, logger)
            log_output(logger, "Annotated filenames:", outlist=annotated_filenames)

        elif cmd == "summarize":
            annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
            # Summarize each annotated file by region (state and county) write summary to a file
            summary_filenames = summarize_annotations(annotated_filenames, logger)
            log_output(
                logger, "Aggregated county/state filenames:", outlist=summary_filenames)

        elif cmd == "aggregate":
            annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
            summary_filenames = [Aggregator.construct_summary_name(annfile) for annfile in annotated_filenames]
            # Aggregate all summary files then write summaries for each region to its own file
            region_assess_summary_filenames = summarize_region_assessment(summary_filenames, logger)
            log_output(
                logger, "Region filenames, assessment filename:", outlist=region_assess_summary_filenames)

        elif cmd == "summarize_assessments":
            state_pattern = os.path.join(DATA_PATH, "out", "state*")
            county_pattern = os.path.join(DATA_PATH, "out", "county*")
            region_summary_filenames = glob.glob(state_pattern)
            region_summary_filenames.extend(glob.glob(county_pattern))
            assess_summary_filename = summarize_assessments(region_summary_filenames, logger)

        elif cmd == "full":
            annotated_filenames = annotate_occurrence_files(input_filenames, logger)
            log_output(logger, "Annotated filenames:", outlist=annotated_filenames)
            # Summarize each annotated file by state and county, write summary to a file
            summary_filenames = summarize_annotations(annotated_filenames, logger)
            log_output(
                logger, "Aggregated county/state filenames:", outlist=summary_filenames)
            # Aggregate all summary files then write summaries for each state and county to a file for that region
            region_assess_summary_filenames = summarize_region_assessment(summary_filenames, logger)
            log_output(
                logger, "Region filenames, assessment filename:", outlist=region_assess_summary_filenames)

        elif cmd == "test_bad_data":
            test_bad_line(input_filenames, logger)

        else:
            logger.error(f"Unsupported command '{cmd}'")
