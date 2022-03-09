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
    """
    chunk_filenames = chunk_files(big_csv_filename)
    logger.info(f"Chunk files created: {chunk_filenames}")

# .............................................................................
def resolve_riis_taxa(riis_filename, logger):
    """Split files into smaller subsets for faster processing.

    Args:
        big_csv_filename (str): full filename for splitting into smaller files.
    """
    nnsl = NNSL(riis_filename, logger=logger)
    # Update species data
    nnsl.resolve_riis_to_gbif_taxa()
    count = nnsl.write_resolved_riis()
    resolved_riis_filename = nnsl.gbif_resolved_riis_fname
    return resolved_riis_filename

# .............................................................................
def annotate_occurrence_files(csv_filenames, do_resolve, logger):
    annotated_filenames = []
    for csv_filename in csv_filenames:
        ant = Annotator(csv_filename, do_resolve=False, logger=logger)
        annotated_dwc_fname = ant.append_dwca_records()
        annotated_filenames.append(annotated_dwc_fname)
    return annotated_filenames


# .............................................................................
def summarize_occurrence_contents(csv_filenames, location_summary_filename, species_summary_filename, logger):
    summary_filenames = []
    for csv_filename in csv_filenames:
        agg = Aggregator(csv_filename, logger=logger)
        summary_filename = agg.summarize_to_file()
        summary_filenames.append(summary_filename)

    # Create a new Aggregator, ignore annotated occurrence file used for construction,
    agg = Aggregator(csv_filenames[0], logger=logger)
    # do not re-summarize, read summaries from files
    agg.read_summaries(summary_filenames, do_clear=False)

    agg.structure_summary()
    agg.summarize_stats(location_summary_filename, species_summary_filename)


# .............................................................................
def identify_subset_files(csv_filenames, logger):
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
        csv_filenames = split_files(big_csv_filename)

    return csv_filenames

# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    riis_filename = os.path.join(DATA_PATH, RIIS_SPECIES.FNAME)
    gbif_infile = os.path.join(DATA_PATH, GBIF.INPUT_DATA)
    default_output_basename = os.path.join(DATA_PATH)

    parser = argparse.ArgumentParser(
        description="Execute one or more steps of annotating GBIF data with RIIS assessments, and summarizing by species, county, and state")
    parser.add_argument("cmd", type=str, default="split")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "do_split", type=str, default=True,
        help="True to process subsetted/chunked files; False to process big_csv_filename directly.  Command 'split' assumes do_subset is True")

    args = parser.parse_args()
    cmd = args.cmd
    big_csv_filename = args.big_csv_filename
    do_split = args.do_split
    logger = get_logger(DATA_PATH, logname=f"main_{cmd}")

    if do_split is True:
        csv_filenames = identify_subset_files(big_csv_filename, logger)
    else:
        csv_filenames = [big_csv_filename]

    # Make sure files to be processed exist
    for csv_filename in csv_filenames:
        if not os.path.exists(csv_filename):
            raise FileNotFoundError(f"Expected file {csv_filename} does not exist")

    if cmd == "resolve":
        resolved_riis_filename = resolve_riis_taxa(riis_filename)

    elif args.cmd == "annotate":
        annotated_filenames = annotate_occurrence_files(csv_filenames, logger)

    elif cmd == "summarize":
        summarize_occurrence_contents(csv_filenames, logger)
