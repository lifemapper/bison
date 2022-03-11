"""Run a single process concurrently across local CPUs."""

import argparse
from bison.tools.util import get_logger
from bison.common.constants import DATA_PATH

# from concurrent.futures import ProcessPoolExecutor
# import os
# from random import randint
# from time import sleep
#
# from bison.common.annotate import Annotator
# from bison.common.constants import DATA_PATH
#
# from bison.tools.util import get_logger, chunk_files
#
# # .............................................................................
# def intersect_csv_and_shapefiles(in_csv_filename, geodata1, geodata2,
#                                  ancillary_path, out_csv_filename, from_gbif):
#     """Intersect the records in the csv file with the two provided shapefiles.
#
#     Args:
#         csv_filename (str): Path to a CSV file of records.
#         shapefile_1_filename (str): Path to the first shapefile to check for
#             intersection.
#         shapefile_2_filename (str): Path to the second shapefile to check for
#             intersection.
#         out_csv_filename (str): Path for output CSV records.
#     """
#     pth, basefname = os.path.split(out_csv_filename)
#     logbasename, _ = os.path.splitext(basefname)
#     logfname = os.path.join(pth, '{}.log'.format(logbasename))
#     logger = get_logger(logbasename, logfname)
#     bf = BisonFiller(log=logger)
#     # Pass 4 of CSV transform, final step, point-in-polygon intersection
#     bf.update_point_in_polygons(
#         geodata1, geodata2, ancillary_path, in_csv_filename, out_csv_filename,
#         from_gbif=from_gbif)
#     # Do intersection here# # .............................................................................
# def intersect_csv_and_shapefiles(in_csv_filename, geodata1, geodata2,
#                                  ancillary_path, out_csv_filename, from_gbif):
#     """Intersect the records in the csv file with the two provided shapefiles.
#
#     Args:
#         csv_filename (str): Path to a CSV file of records.
#         shapefile_1_filename (str): Path to the first shapefile to check for
#             intersection.
#         shapefile_2_filename (str): Path to the second shapefile to check for
#             intersection.
#         out_csv_filename (str): Path for output CSV records.
#     """
#     pth, basefname = os.path.split(out_csv_filename)
#     logbasename, _ = os.path.splitext(basefname)
#     logfname = os.path.join(pth, '{}.log'.format(logbasename))
#     logger = get_logger(logbasename, logfname)
#     bf = BisonFiller(log=logger)
#     # Pass 4 of CSV transform, final step, point-in-polygon intersection
#     bf.update_point_in_polygons(
#         geodata1, geodata2, ancillary_path, in_csv_filename, out_csv_filename,
#         from_gbif=from_gbif)
#     # Do intersection here
#     sleep(randint(0, 10))
#     print(' - {}'.format(out_csv_filename))
#
# # .............................................................................
# def step_parallel(in_csv_filename, terrestrial_data, marine_data, ancillary_path,
#                   out_csv_filename, from_gbif=True):
#     """Main method for parallel execution of geo-referencing script"""
#     csv_filename_pairs, header = get_chunk_files(
#          in_csv_filename, out_csv_filename=out_csv_filename)
#
# #     in_csv_fn, out_csv_fn = csv_filename_pairs[0]
# #     intersect_csv_and_shapefiles(in_csv_fn, terrestrial_data,
# #                 marine_data, ancillary_path, out_csv_fn, False)
#
#     with ProcessPoolExecutor() as executor:
#         for in_csv_fn, out_csv_fn in csv_filename_pairs:
#             executor.submit(
#                 intersect_csv_and_shapefiles, in_csv_fn, terrestrial_data,
#                 marine_data, ancillary_path, out_csv_fn, from_gbif)
#
#     try:
#         outf = open(out_csv_filename, 'w', encoding='utf-8')
#         outf.write('{}'.format(header))
#         smfile_linecount = 0
#         for _, small_csv_fn in csv_filename_pairs:
#             curr_linecount = get_line_count(small_csv_fn) - 1
#             print('Appending {} records from {}'.format(
#                 curr_linecount, small_csv_fn))
#             # Do not count header
#             smfile_linecount += (curr_linecount)
#             lineno = 0
#             try:
#                 for line in open(small_csv_fn, 'r', encoding='utf-8'):
#                     # Skip header in each file
#                     if lineno == 0:
#                         pass
#                     else:
#                         outf.write('{}'.format(line))
#                     lineno += 1
#             except Exception as inner_err:
#                 print('Failed to write {} to merged file; {}'.format(small_csv_fn, inner_err))
#     except Exception as outer_err:
#         print('Failed to write to {}; {}'.format(out_csv_filename, outer_err))
#     finally:
#         outf.close()
#
#     lgfile_linecount = get_line_count(out_csv_filename) - 1
#     print('Total {} of {} records written to {}'.format(
#         lgfile_linecount, smfile_linecount, out_csv_filename))

#     sleep(randint(0, 10))
#     print(' - {}'.format(out_csv_filename))
#
# # .............................................................................
# def step_parallel(in_csv_filename, terrestrial_data, marine_data, ancillary_path,
#                   out_csv_filename, from_gbif=True):
#     """Main method for parallel execution of geo-referencing script"""
#     csv_filename_pairs, header = get_chunk_files(
#          in_csv_filename, out_csv_filename=out_csv_filename)
#
# #     in_csv_fn, out_csv_fn = csv_filename_pairs[0]
# #     intersect_csv_and_shapefiles(in_csv_fn, terrestrial_data,
# #                 marine_data, ancillary_path, out_csv_fn, False)
#
#     with ProcessPoolExecutor() as executor:
#         for in_csv_fn, out_csv_fn in csv_filename_pairs:
#             executor.submit(
#                 intersect_csv_and_shapefiles, in_csv_fn, terrestrial_data,
#                 marine_data, ancillary_path, out_csv_fn, from_gbif)
#
#     try:
#         outf = open(out_csv_filename, 'w', encoding='utf-8')
#         outf.write('{}'.format(header))
#         smfile_linecount = 0
#         for _, small_csv_fn in csv_filename_pairs:
#             curr_linecount = get_line_count(small_csv_fn) - 1
#             print('Appending {} records from {}'.format(
#                 curr_linecount, small_csv_fn))
#             # Do not count header
#             smfile_linecount += (curr_linecount)
#             lineno = 0
#             try:
#                 for line in open(small_csv_fn, 'r', encoding='utf-8'):
#                     # Skip header in each file
#                     if lineno == 0:
#                         pass
#                     else:
#                         outf.write('{}'.format(line))
#                     lineno += 1
#             except Exception as inner_err:
#                 print('Failed to write {} to merged file; {}'.format(small_csv_fn, inner_err))
#     except Exception as outer_err:
#         print('Failed to write to {}; {}'.format(out_csv_filename, outer_err))
#     finally:
#         outf.close()
#
#     lgfile_linecount = get_line_count(out_csv_filename) - 1
#     print('Total {} of {} records written to {}'.format(
#         lgfile_linecount, smfile_linecount, out_csv_filename))


# .............................................................................
if __name__ == '__main__':
    """Main method for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'csv_filename', type=str, help='Input record CSV file path.')
    parser.add_argument(
        'terrestrial_shapefile_path', type=str,
        help='Terrestrial shapefile for intersection.')
    parser.add_argument(
        'marine_shapefile_path', type=str,
        help='Marine shapefile for intersection.')
    parser.add_argument(
        'out_csv_path', type=str,
        help='File path for output recordds CSV file.')
    args = parser.parse_args()
    logger = get_logger(DATA_PATH, logname="test_annotate")
