"""Main script to execute all elements of the summarize-GBIF BISON workflow."""
import os

from bison.common.annotate import Annotator
from bison.common.constants import GBIF, DATA_PATH
from bison.tools.util import chunk_files, identify_chunk_files

# .............................................................................
def split_files(big_csv_filename):
    chunk_filenames = chunk_files(args.big_csv_filename)
    print(chunk_filenames)


# .............................................................................
def annotate_files(big_csv_filename, do_resolve, logger):
    datapath, _ = os.path.split(big_csv_filename)
    chunk_filenames = identify_chunk_files(big_csv_filename)
    for chunk_filename in chunk_filenames:
        if not os.path.exists(chunk_filename):
            raise FileNotFoundError(f"Expected chunk file {chunk_filename} does not exist")
        _, chunk_fname = os.path.split(chunk_fname)
        Annotator.__init__(
            chunk_filename, do_resolve=do_resolve, logger=logger)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import argparse

    default_infile = os.path.join(DATA_PATH, GBIF.INPUT_DATA)
    default_output_basename = os.path.join(DATA_PATH)

    parser = argparse.ArgumentParser(description="Split")
    parser.add_argument("cmd", type=str, default="split")
    parser.add_argument(
        "big_csv_filename", type=str, default=GBIF.INPUT_DATA,
        help='The full path to GBIF input species occurrence data.')
    args = parser.parse_args()

    cmd = args.cmd
    csv_fname = args.big_csv_filename


    if args.cmd == "split":
        chunk_filenames = split_files(csv_fname)
    elif args.cmd == "annotate":
        chunk_filenames = annotate_files(big_csv_filename, do_resolve, logger)
