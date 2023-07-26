"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
import os
import pandas
from logging import INFO, DEBUG, ERROR
from osgeo import ogr

from bison.common.constants import REGION
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
        self._cell_count = 0
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
                self.logit(
                    f"Warning, unable to add FID {fid} for {self.filename}: {e}")
            else:
                loc_key = self._get_location_key(feat)
                self._cells[loc_key] = fid

        self._dataframe = pandas.DataFrame()

    # ...............................................
    def add_column_for_species(self, colname, fid_count):
        """Add a column to the dataframe.

        Args:
            colname: species name for the new column
            fid_count (dict): dictionary of fid (row index) and count

        """
        counts = []
        for fid in self.cell_count:
            try:
                counts.append(fid_count[fid])
            except KeyError:
                counts.append(0)
        self._dataframe[colname] = counts

    # ...............................................
    @property
    def matrix(self):
        """Return the pandas dataframe"""
        return self._dataframe

    # ...............................................
    @property
    def cell_count(self):
        return len(self._cells.keys())

    # ...............................................
    @property
    def cell_attribute_lookup(self):
        return self._cells


# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    pass
