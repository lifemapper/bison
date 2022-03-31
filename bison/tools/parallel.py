"""Run a single process concurrently across local CPUs."""
# import argparse
from concurrent.futures import ProcessPoolExecutor
import os

from bison.common.annotate import Annotator
from bison.common.constants import LOG
from bison.tools.util import get_logger


# .............................................................................
def annotate_occurrence_file(input_filename, logger):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        input_filename (str): full filename containing GBIF data for annotation.
        logger (object): logger for saving relevant processing messages

    Returns:
        annotated_dwc_fname: full filename for GBIF data annotated with state, county, RIIS assessment, and RIIS key.
    """
    ant = Annotator(input_filename, logger=logger)
    annotated_dwc_fname = ant.annotate_dwca_records()
    return annotated_dwc_fname


# .............................................................................
def parallel_annotate(input_filenames):
    """Main method for parallel execution of DwC annotation script.

    Args:
        input_filenames (list): list of full filenames containing GBIF data for annotation.
    """
    with ProcessPoolExecutor() as executor:
        for in_csv_fn in input_filenames:
            datapath, basefname = os.path.split(in_csv_fn)
            basename, _ = os.path.splitext(basefname)
            logger = get_logger(os.path.join(datapath, LOG.DIR), f"annotate_{basename}")
            executor.submit(annotate_occurrence_file, in_csv_fn, logger)

    annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
    for fn in annotated_filenames:
        if not os.path.exists(fn):
            logger.info(f"File {fn} does not yet exist")

#
# # .............................................................................
# if __name__ == '__main__':
#     """Main method for script."""
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         'csv_filename', type=str, help='Input record CSV file path.')
#     parser.add_argument(
#         'terrestrial_shapefile_path', type=str,
#         help='Terrestrial shapefile for intersection.')
#     parser.add_argument(
#         'marine_shapefile_path', type=str,
#         help='Marine shapefile for intersection.')
#     parser.add_argument(
#         'out_csv_path', type=str,
#         help='File path for output recordds CSV file.')
#     args = parser.parse_args()
#     logger = get_logger(DATA_PATH, logname="test_annotate")
