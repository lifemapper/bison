"""Process BISON RIIS and GBIF Species data to annotate GBIF records and aggregate results."""
import argparse

from bison.common.riis import NNSL

# ...............................................
if __name__ == "__main__":
    DEFAULT_BISON_PATH = "/home/astewart/git/bison"
    DEFAULT_GBIF_FNAME = "/tank/bison/2022/gbif_2022_01_0-100.csv"

    parser = argparse.ArgumentParser(
        description='Annotate GBIF records with BISON RIIS determinations and aggregate results.')
    parser.add_argument(
        'bison_path', type=str, default=DEFAULT_BISON_PATH,
        help='The base path for BISON input data and outputs.')
    parser.add_argument(
        'gbif_fname', type=str, default=DEFAULT_GBIF_FNAME,
        help='The full path to GBIF input species occurrence data.')
    args = parser.parse_args()

    bison = NNSL(args.bison_path)
    bison.read_species()
    # Update species data
    bison.resolve_gbif_species()
    bison.write_species()
    # Step through GBIF data and annotate with RIIS Ids
