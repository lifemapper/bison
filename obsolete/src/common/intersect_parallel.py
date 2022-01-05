"""Manage parallel intersections for BISON data."""

from concurrent.futures import ProcessPoolExecutor

from intersect_one_csv import intersect_csv_and_shapefiles

# .............................................................................
# Send these as arguments to the script or update constants
# .............................................................................
CSV_FILENAMES_IN_OUT = [
    ('in_csv_1.csv', 'out_csv_1.csv'),
    ('in_csv_2.csv', 'out_csv_2.csv'),
    ('in_csv_3.csv', 'out_csv_3.csv'),
    ('in_csv_4.csv', 'out_csv_4.csv'),
    ('in_csv_5.csv', 'out_csv_5.csv'),
    ('in_csv_6.csv', 'out_csv_6.csv'),
    ('in_csv_7.csv', 'out_csv_7.csv'),
    ('in_csv_8.csv', 'out_csv_8.csv'),
    ('in_csv_9.csv', 'out_csv_9.csv'),
    ('in_csv_10.csv', 'out_csv_10.csv')
    ]

TERRESTRIAL_SHAPEFILE = 'terrestrial.shp'
MARINE_SHAPEFILE = 'marine.shp'


# .............................................................................
def main():
    """Main method for script."""
    with ProcessPoolExecutor() as executor:
        for in_csv_fn, out_csv_fn in CSV_FILENAMES_IN_OUT:
            executor.submit(
                intersect_csv_and_shapefiles, in_csv_fn, TERRESTRIAL_SHAPEFILE,
                MARINE_SHAPEFILE, out_csv_fn)


# .............................................................................
if __name__ == '__main__':
    main()
