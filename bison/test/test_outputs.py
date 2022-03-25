"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
# import csv
# import glob
import os

from bison.common.aggregate import Aggregator
from bison.common.constants import DATA_PATH  # , LMBISON_HEADER
# from bison.tools.util import count_lines, get_csv_dict_reader, identify_chunk_files, parse_chunk_filename

# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    gbif_infile = os.path.join(DATA_PATH, "gbif_2022-03-16.csv")

    parser = argparse.ArgumentParser(
        description="Test outputs of summarized GBIF data with RIIS assessments.")
    parser.add_argument(
        "big_csv_filename", type=str, default=gbif_infile,
        help="The full path to GBIF input species occurrence data.")
    parser.add_argument(
        "--do-split", type=str, choices=("True", "False"), default="True",
        help="True to test subsetted/chunked files; False to process big_csv_filename directly.")

    args = parser.parse_args()
    big_csv_filename = os.path.join(DATA_PATH, args.big_csv_filename)
    do_split = True if args.do_split.lower() in ("yes", "y", "true", "1") else False

    in_base_filename, ext = os.path.splitext(big_csv_filename)
    chunk_pattern = f"{in_base_filename}_chunk*"

    annotated_pattern = f"{in_base_filename}_chunk*annotated{ext}"
    summary_annotated_pattern = f"{in_base_filename}_chunk*annotated_summary{ext}"

    state_summary_pattern = Aggregator.construct_location_summary_name(DATA_PATH, "*")
    county_summary_pattern = Aggregator.construct_location_summary_name(DATA_PATH, "*", county="*")
