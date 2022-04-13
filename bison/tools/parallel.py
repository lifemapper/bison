"""Run a single process concurrently across local CPUs."""
import argparse
from datetime import datetime
# from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pool, cpu_count
import os

from bison.common.annotate import Annotator
from bison.common.constants import DATA_PATH, LOG, RIIS_SPECIES
from bison.common.riis import NNSL
from bison.tools.util import get_logger, identify_chunk_files, parse_chunk_filename


# .............................................................................
def annotate_occurrence_file(input_filename):
    """Annotate GBIF records with census state and county, and RIIS key and assessment.

    Args:
        input_filename (str): full filename containing GBIF data for annotation.

    Returns:
        annotated_dwc_fname: full filename for GBIF data annotated with state, county, RIIS assessment, and RIIS key.
    """
    datapath, basefname = os.path.split(input_filename)
    basename, _ = os.path.splitext(basefname)
    in_base_filename, start, stop, ext = parse_chunk_filename(basename)
    logger = get_logger(os.path.join(datapath, LOG.DIR), f"annotate_{start}-{stop}")
    logger.info(f"Submit {basefname} for annotation")

    orig_riis_filename = os.path.join(DATA_PATH, RIIS_SPECIES.FNAME)
    nnsl = NNSL(orig_riis_filename, logger=logger)
    nnsl.read_riis(read_resolved=True)

    logger.info("Start Time : {}".format(datetime.now()))
    ant = Annotator(input_filename, nnsl=nnsl, logger=logger)
    annotated_dwc_fname = ant.annotate_dwca_records()
    logger.info("End Time : {}".format(datetime.now()))
    return annotated_dwc_fname


# .............................................................................
def parallel_annotate_multiprocess(input_filenames, main_logger):
    """Main method for parallel execution of DwC annotation script.

    Args:
        input_filenames (list): list of full filenames containing GBIF data for annotation.
        main_logger (logger): logger for the process that calls this function, initiating subprocesses

    Returns:
        annotated_dwc_fnames (list): list of full output filenames
    """
    infiles = []
    # Process only needed files
    for in_csv in input_filenames:
        out_csv = Annotator.construct_annotated_name(in_csv)
        if os.path.exists(out_csv):
            main_logger.info(f"Annotations exist in {out_csv}, moving on.")
        else:
            infiles.append(in_csv)

    # Do not use all CPUs
    pool = Pool(cpu_count() - 2)
    # Map input files asynchronously onto function
    map_result = pool.map_async(annotate_occurrence_file, infiles)
    # Wait for results
    map_result.wait()
    annotated_dwc_fnames = map_result.get()

    return annotated_dwc_fnames

# # .............................................................................
# def parallel_annotate_multithread(input_filenames, main_logger):
#     """Main method for parallel execution of DwC annotation script.
#
#     Args:
#         input_filenames (list): list of full filenames containing GBIF data for annotation.
#         main_logger (logger): logger for the process that calls this function, initiating subprocesses
#
#     Returns:
#         annotated_filenames: list of resulting annotated files
#     """
#     with ProcessPoolExecutor() as executor:
#         for in_csv_fn in input_filenames:
#             datapath, basefname = os.path.split(in_csv_fn)
#             basename, _ = os.path.splitext(basefname)
#
#             main_logger.info(f"Submit {basefname} for annotation")
#             logger = get_logger(os.path.join(datapath, LOG.DIR), f"annotate_{basename}")
#             executor.submit(annotate_occurrence_file, in_csv_fn, logger)
#
#     annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
#     for fn in annotated_filenames:
#         if not os.path.exists(fn):
#             main_logger.info(f"File {fn} does not yet exist")
#
#     return annotated_filenames


# .............................................................................
if __name__ == '__main__':
    """Main method for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'big_csv_filename', type=str, help='Input original CSV filename with path.')
    args = parser.parse_args()

    big_csv = args.big_csv_filename
    datapath, basefname = os.path.split(big_csv)
    basename, _ = os.path.splitext(basefname)
    logger = get_logger(os.path.join(datapath, LOG.DIR), f"annotate_{basename}")
    chunk_filenames = identify_chunk_files(big_csv)

    parallel_annotate_multiprocess(chunk_filenames, logger)
