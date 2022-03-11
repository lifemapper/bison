"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import os

from bison.common.aggregate import Aggregator
from bison.common.annotate import Annotator
from bison.common.constants import GBIF, DATA_PATH, RIIS_SPECIES
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
def annotate_occurrence_files(csv_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        csv_filenames (list): list of full filenames containing GBIF data for annotation.
        logger (object): logger for saving relevant processing messages

    Returns:
        annotated_filenames: fill filenames for GBIF data annotated with state, county, RIIS assessment, and RIIS key.
    """
    annotated_filenames = []
    for csv_filename in csv_filenames:
        ant = Annotator(csv_filename, do_resolve=False, logger=logger)
        annotated_dwc_fname = ant.append_dwca_records()
        annotated_filenames.append(annotated_dwc_fname)
    return annotated_filenames


# .............................................................................
def summarize_occurrence_contents(csv_filenames, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        csv_filenames (list): list of full filenames containing GBIF data for annotation.
        logger (object): logger for saving relevant processing messages

    Returns:
        state_aggregation_filenames (list): full filenames of species counts and percentages for each state.
        cty_aggregation_filename (list): full filenames of species counts and percentages for each county-state.
    """
    summary_filenames = []
    for csv_filename in csv_filenames:
        agg = Aggregator(csv_filename, logger=logger)
        summary_filename = agg.summarize_to_file()
        summary_filenames.append(summary_filename)

    # Create a new Aggregator, ignore annotated occurrence file used for construction,
    agg = Aggregator(csv_filenames[0], logger=logger)
    # read summaries from all files
    state_aggregation_filenames, cty_aggregation_filenames = agg.write_location_aggregates(summary_filenames)
    return state_aggregation_filenames, cty_aggregation_filenames


# .............................................................................
def identify_subset_files(big_csv_filename, logger):
    """Find or create subset files from a large file based on the file size and number of CPUs.

    Args:
        big_csv_filename (str): full filename of data file to be subsetted into chunks.
        logger (object): logger for saving relevant processing messages

    Returns:
        csv_filenames (list): full filenames for subset files created from large input file.
    """
    csv_filenames = identify_chunk_files(big_csv_filename)
    # If any are missing, delete them all and split
    re_split = False
    for csv_filename in csv_filenames:
        if not os.path.exists(csv_filename):
            re_split = True
            break
    # raise FileNotFoundError(f"Expected chunk file {csv_filename} does not exist")
    if re_split is True:
        # Delete any existing files
        for csv_filename in csv_filenames:
            delete_file(csv_filename)
        # Resplit into subset files
        csv_filenames = split_files(big_csv_filename, logger)

    return csv_filenames


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    riis_filename = os.path.join(DATA_PATH, RIIS_SPECIES.FNAME)
    gbif_infile = os.path.join(DATA_PATH, GBIF.INPUT_DATA)
    gbif_infile = os.path.join(DATA_PATH, "gbif_2022-02-15_100k.csv")
    default_output_basename = os.path.join(DATA_PATH)

    parser = argparse.ArgumentParser(
        description="Execute one or more steps of annotating GBIF data with RIIS assessments, and summarizing by species, county, and state")
    parser.add_argument("cmd", type=str, default="split")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "--do_split", type=str, choices=("True", "False"), default="True",
        help="True to process subsetted/chunked files; False to process big_csv_filename directly.  Command 'split' assumes do_subset is True")

    args = parser.parse_args()
    cmd = args.cmd
    big_csv_filename = args.big_csv_filename
    do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False
    logger = get_logger(DATA_PATH, logname=f"main_{cmd}")

    if cmd == "resolve":
        resolved_riis_filename = resolve_riis_taxa(riis_filename, logger)
        print(resolved_riis_filename)
    else:
        if do_split is True:
            csv_filenames = identify_subset_files(big_csv_filename, logger)
        else:
            csv_filenames = [big_csv_filename]

        # Make sure files to be processed exist
        for csv_filename in csv_filenames:
            if not os.path.exists(csv_filename):
                raise FileNotFoundError(f"Expected file {csv_filename} does not exist")

        if cmd == "annotate":
            annotated_filenames = annotate_occurrence_files(csv_filenames, logger)

        elif cmd == "summarize":
            state_aggregation_filenames, cty_aggregation_filenames = summarize_occurrence_contents(csv_filenames, logger)
