"""Intersect a single CSV file with multiple shapefiles."""

import argparse
import os
from random import randint
from time import sleep

from common.bisonfill import BisonFiller
from common.tools import getLogger

# .............................................................................
def intersect_csv_and_shapefiles(in_csv_filename, geodata1, geodata2, 
                                 ancillary_path, out_csv_filename):
    """Intersect the records in the csv file with the two provided shapefiles.

    Args:
        csv_filename (str): Path to a CSV file of records.
        shapefile_1_filename (str): Path to the first shapefile to check for
            intersection.
        shapefile_2_filename (str): Path to the second shapefile to check for
            intersection.
        out_csv_filename (str): Path for output CSV records.
    """
    pth, basefname = os.path.split(in_csv_filename)
    logbasename, _ = os.path.splitext(basefname)
    logfname = os.path.join(pth, '{}.log'.format(logbasename))
    logger = getLogger(logbasename, logfname)
    bf = BisonFiller(in_csv_filename, log=logger)
    # Pass 4 of CSV transform, final step, point-in-polygon intersection
    bf.update_point_in_polygons(geodata1, geodata2, ancillary_path, 
                                out_csv_filename)
    # Do intersection here
    sleep(randint(0, 10))
    print(' - {}'.format(out_csv_filename))


# .............................................................................
def main():
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
    intersect_csv_and_shapefiles(
        args.csv_filename, args.terrestrial_shapefile_path,
        args.marine_shapefile_path, args.out_csv_path)


# .............................................................................
if __name__ == '__main__':
    main()
