"""Intersect a single CSV file with multiple shapefiles."""

import argparse
from random import randint
from time import sleep


# .............................................................................
def intersect_csv_and_shapefiles(csv_filename, shapefile_1_filename,
                                 shapefile_2_filename, out_csv_filename):
    """Intersect the records in the csv file with the two provided shapefiles.

    Args:
        csv_filename (str): Path to a CSV file of records.
        shapefile_1_filename (str): Path to the first shapefile to check for
            intersection.
        shapefile_2_filename (str): Path to the second shapefile to check for
            intersection.
        out_csv_filename (str): Path for output CSV records.
    """
    # Do intersection here
    sleep(randint(0, 10))
    print(' - {} {} {} {}'.format(
        csv_filename, shapefile_1_filename, shapefile_2_filename,
        out_csv_filename))


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
