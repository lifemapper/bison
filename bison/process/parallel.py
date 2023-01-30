"""Run a single process concurrently across local CPUs."""
# from datetime import datetime
# # from concurrent.futures import ProcessPoolExecutor
# from multiprocessing import Pool, cpu_count
# import os
#
# from bison.common.constants import DATA_PATH, RIIS
#
# from bison.common.log import Logger
#
# from bison.process.aggregate import Aggregator
# from bison.process.annotate import Annotator
# from bison.providers.riis_data import NNSL
#
#
# # .............................................................................
# def annotate_occurrence_file(input_filename, log_directory):
#     """Annotate GBIF records with census state and county, and RIIS key and assessment.
#
#     Args:
#         input_filename (str): full filename containing GBIF data for annotation.
#
#     Returns:
#         annotated_dwc_fname: full filename for GBIF data annotated with state, county, RIIS assessment, and RIIS key.
#
#     Raises:
#         FileNotFoundError: on missing input file
#     """
#     if not os.path.exists(input_filename):
#         raise FileNotFoundError(input_filename)
#
#     _, basefname = os.path.split(input_filename)
#     refname = f"annotate_{basefname}"
#     logger = Logger(refname, os.path.join(log_directory, f"{refname}.log"))
#     logger.log(f"Submit {basefname} for annotation", refname=refname)
#
#     orig_riis_filename = os.path.join(DATA_PATH, RIIS.SPECIES_GEO_FNAME)
#     nnsl = NNSL(orig_riis_filename, logger)
#     nnsl.read_riis(read_resolved=True)
#
#     logger.info("Start Time : {}".format(datetime.now()))
#     ant = Annotator(input_filename, nnsl=nnsl, logger=logger)
#     annotated_dwc_fname = ant.annotate_dwca_records()
#     logger.info("End Time : {}".format(datetime.now()))
#     return annotated_dwc_fname
#
#
# # .............................................................................
# def parallel_annotate_multiprocess(input_filenames, main_logger):
#     """Main method for parallel execution of DwC annotation script.
#
#     Args:
#         input_filenames (list): list of full filenames containing GBIF data for annotation.
#         main_logger (logger): logger for the process that calls this function, initiating subprocesses
#
#     Returns:
#         annotated_dwc_fnames (list): list of full output filenames
#     """
#     inputs = []
#     # Process only needed files
#     for in_csv in input_filenames:
#         out_csv = Annotator.construct_annotated_name(in_csv)
#         if os.path.exists(out_csv):
#             main_logger.info(f"Annotations exist in {out_csv}, moving on.")
#         else:
#             inputs.append((in_csv, main_logger.log_directory))
#
#     # Do not use all CPUs
#     pool = Pool(cpu_count() - 2)
#     # Map input files asynchronously onto function
#     map_result = pool.starmap_async(annotate_occurrence_file, inputs)
#     # Wait for results
#     map_result.wait()
#     annotated_dwc_fnames = map_result.get()
#
#     return annotated_dwc_fnames
#
# # # .............................................................................
# # def parallel_annotate_multithread(input_filenames, main_logger):
# #     """Main method for parallel execution of DwC annotation script.
# #
# #     Args:
# #         input_filenames (list): list of full filenames containing GBIF data for annotation.
# #         main_logger (logger): logger for the process that calls this function, initiating subprocesses
# #
# #     Returns:
# #         annotated_filenames: list of resulting annotated files
# #     """
# #     with ProcessPoolExecutor() as executor:
# #         for in_csv_fn in input_filenames:
# #             datapath, basefname = os.path.split(in_csv_fn)
# #             basename, _ = os.path.splitext(basefname)
# #
# #             main_logger.info(f"Submit {basefname} for annotation")
# #             logger = get_logger(os.path.join(datapath, LOG.DIR), f"annotate_{basename}")
# #             executor.submit(annotate_occurrence_file, in_csv_fn, logger)
# #
# #     annotated_filenames = [Annotator.construct_annotated_name(csvfile) for csvfile in input_filenames]
# #     for fn in annotated_filenames:
# #         if not os.path.exists(fn):
# #             main_logger.info(f"File {fn} does not yet exist")
# #
# #     return annotated_filenames
#
#
# # .............................................................................
# def summarize_annotations(ann_filename, log_directory):
#     """Summarize data in an annotated GBIF DwC file by state, county, and RIIS assessment, do not overwrite existing file.
#
#     Args:
#         ann_filename (str): full filename to an annotated GBIF data file.
#         log_directory (str): destination directory for logfile
#
#     Returns:
#         summary_filename (str): full filename of a summary file
#
#     Raises:
#         FileNotFoundError: on missing input file
#     """
#     # overwrite = False
#     if not os.path.exists(ann_filename):
#         raise FileNotFoundError(ann_filename)
#
#     datapath, basefname = os.path.split(ann_filename)
#     refname = f"summarize_{basefname}"
#     logger = Logger(refname, os.path.join(log_directory, f"{refname}.log"))
#     logger.log(f"Submit {basefname} for summarizing.", refname=refname)
#
#     logger.log(f"Start Time : {datetime.now()}", refname=refname)
#     agg = Aggregator(ann_filename, logger=logger)
#     summary_filename = agg.summarize_by_file(overwrite=False)
#     logger.log(f"End Time : {datetime.now()}", refname=refname)
#     return summary_filename
#
#
# # .............................................................................
# def parallel_summarize_multiprocess(annotated_filenames, main_logger):
#     """Main method for parallel execution of summarization script.
#
#     Args:
#         annotated_filenames (list): list of full filenames containing annotated GBIF data.
#         main_logger (logger): logger for the process that calls this function, initiating subprocesses
#
#     Returns:
#         annotated_dwc_fnames (list): list of full output filenames
#     """
#     inputs = []
#     for in_csv in annotated_filenames:
#         out_csv = Aggregator.construct_summary_name(in_csv)
#         if os.path.exists(out_csv):
#             main_logger.info(f"Summaries exist in {out_csv}, moving on.")
#         else:
#             inputs.append((in_csv, main_logger.log_directory))
#
#     # Do not use all CPUs
#     pool = Pool(cpu_count() - 2)
#     # Map input files asynchronously onto function
#     map_result = pool.map_async(summarize_annotations, inputs)
#     # Wait for results
#     map_result.wait()
#     summary_filenames = map_result.get()
#
#     return summary_filenames
