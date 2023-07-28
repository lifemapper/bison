"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
# import lmpy
import numpy
import os
import pandas
from osgeo import ogr

from bison.common.util import BisonKey


# .............................................................................
class PolygonMatrix(object):
    """Object for intersecting coordinates with a polygon shapefile."""
    def __init__(
            self, full_spatial_fname, fldname, parent_fldname=None, logger=None,
            is_disjoint=True):
        """Construct a geospatial index to intersect with a set of coordinates.

        Args:
            full_spatial_fname (str): full filename for a shapefile to construct index
            fldname (str): fieldname for polygon attribute of interest
            parent_fldname (dict): fieldname for parent name attribute of interest,
                ex: state name for county
            logger (object): logger for saving relevant processing messages
            is_disjoint (bool): True indicates the spatial features are disjoint;
                False indicates contiguous features.

        Raises:
            FileNotFoundError: if spatial_fname does not exist on the file system
        """
        # full_spatial_fname = os.path.join(DATA_PATH, spatial_fname)
        if not os.path.exists(full_spatial_fname):
            raise FileNotFoundError(f"{full_spatial_fname}")
        self._log = logger
        self._spatial_filename = full_spatial_fname
        self._fldname = fldname
        self._parent_fldname = parent_fldname
        self._is_disjoint = is_disjoint
        self._initialize_geospatial_data()

    # ...............................................
    def _get_location_key(self, feat):
        name = feat.GetField(self._fldname)
        if self._parent_fldname is not None:
            parent = feat.GetField(self._parent_fldname)
            key = BisonKey.get_compound_key(parent, name)
        else:
            key = name
        return key

    # ...............................................
    def _initialize_geospatial_data(self):
        """Init a lookup table for polygon features in the file and their attributes."""
        driver = ogr.GetDriverByName("ESRI Shapefile")

        dataset = driver.Open(self._spatial_filename, 0)
        lyr = dataset.GetLayer()
        # lookup table of parent_
        self._cells = {}
        # Zero-based
        for fid in range(0, lyr.GetFeatureCount()):
            try:
                feat = lyr.GetFeature(fid)
            except Exception as e:
                self._log.log(
                    f"Warning, unable to add FID {fid} for {self._spatial_filename}:"
                    f" {e}", refname=self.__class__.__name__
                )
            else:
                loc_key = self._get_location_key(feat)
                self._cells[loc_key] = fid
        self._dataframe = None

    # ...............................................
    def create_column_for_species(self, fid_count):
        """Add a column to the dataframe.

        Args:
            fid_count (dict): dictionary of fid (row index) and count

        Returns:
            column (list): ordered list of counts for each row index (fid).
        """
        counts = numpy.zeros(self.cell_count, dtype=numpy.int32)
        for fid, count in fid_count.items():
            counts[fid] = count
        return list(counts)

    # ...............................................
    def create_dataframe_from_cols(self, species_cols):
        """Add a column to the dataframe.

        Args:
            species_cols (dict): keys contain species(column) name, with
                a value of a list of counts for each row index (fid).
        """
        self._dataframe = pandas.DataFrame(species_cols)

    # ...............................................
    @property
    def matrix(self):
        """Return the pandas dataframe.

        Returns:
            The pandas.DataFrame for this instance.
        """
        return self._dataframe

    # ...............................................
    @property
    def row_count(self):
        """Return the number of rows in the dataframe.

        Returns:
            The count of rows for the DataFrame instance.
        """
        return self._dataframe.shape[0]

    # ...............................................
    @property
    def rows(self):
        """Return rows in the dataframe as a list.

        Returns:
            The row indexes for the DataFrame instance.
        """
        return self._dataframe.index.to_list()

    # ...............................................
    @property
    def column_count(self):
        """Return the number of columns in the dataframe.

        Returns:
            The count of columns for the DataFrame instance.
        """
        return self._dataframe.shape[1]

    # ...............................................
    @property
    def columns(self):
        """Return columns in the dataframe as a list.

        Returns:
            list of column names for the dataframe.
        """
        return self._dataframe.columns.to_list()

    # ...............................................
    @property
    def cell_count(self):
        """Return the number of cells/polygons (rows in the dataframe) in this instance.

        Returns:
            The count of cells/rows for this instance.
        """
        return len(self._cells.keys())

    # ...............................................
    @property
    def cell_attribute_lookup(self):
        """Return a dictionary of the spatial cells/polygons with unique_attribute: fid.

        Returns:
            Dictionary of the spatial cells/polygons, with the key as a unique feature
                attribute, and the value as the FID.
        """
        return self._cells

    def write_matrix(self, heatmatrix_filename):
        """Write the matrix to a zipped CSV file.

        Args:
            heatmatrix_filename (str): Full filename for output pandas.DataFrame.

        Raises:
            Exception: on failure to write matrix.
        """
        try:
            self._dataframe.to_csv(
                path_or_buf=heatmatrix_filename, sep=",", header=True, index=True,
                index_label=False, mode='w', encoding="utf-8", compression="zip"
            )
        except Exception:
            raise


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    pass
