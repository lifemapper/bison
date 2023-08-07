"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
# import lmpy
import numpy
import os
import pandas
from osgeo import ogr

from bison.common.util import BisonKey, ready_filename


# .............................................................................
class GeoMatrix(object):
    """Object for intersecting coordinates with a polygon shapefile."""
    def __init__(
            self, matrix_filename=None, spatial_filename=None, fieldnames=(),
            logger=None):
        """Construct a Dataframe containing counts from a shapefile or matrix file.

        Args:
            matrix_filename (str): full filename for a zipped CSV file containing a
                pandas.DataFrame,
            spatial_filename (str): full filename for a shapefile of polygons to
                construct a matrix defined by one row per polygon, identified by a
                feature identifier (FID), and optionally, a string of one or more
                concatenated attributes, unique to that polygon.
            fieldnames (iterable): lisf of fieldname for polygon attributes of interest
            logger (object): logger for saving relevant processing messages

        Raises:
            FileNotFoundError: if spatial_filename does not exist on the file system
            FileNotFoundError: if matrix_filename does not exist on the file system
            Exception: on neither spatiol_filename or matrix_filename provided.
        """
        self._log = logger
        # Dictionary with FID (index): a string/list of additional row names
        self._row_indices = {}

        if matrix_filename:
            if os.path.exists(matrix_filename):
                self.read_matrix(matrix_filename)
            else:
                raise FileNotFoundError(f"{spatial_filename}")
        elif spatial_filename:
            if os.path.exists(spatial_filename):
                self._initialize_geospatial_data(spatial_filename, fieldnames)
            else:
                raise FileNotFoundError(f"{spatial_filename}")
        else:
            raise Exception("Must provide either spatial_filename or matrix_filename.")

    # ...............................................
    def _get_location_key(self, feat, fieldnames):
        values = [feat.GetField(fn) for fn in fieldnames]
        key = BisonKey.get_compound_key(*values)
        return key

    # ...............................................
    def _initialize_geospatial_data(self, spatial_filename, fieldnames):
        # Init a lookup table for polygon features in the file and their attributes.
        driver = ogr.GetDriverByName("ESRI Shapefile")

        dataset = driver.Open(spatial_filename, 0)
        lyr = dataset.GetLayer()
        # lookup table of attribute_value: fid
        # Zero-based
        for fid in range(0, lyr.GetFeatureCount()):
            try:
                feat = lyr.GetFeature(fid)
            except Exception as e:
                self._log.log(
                    f"Warning, unable to add FID {fid} for {spatial_filename}:"
                    f" {e}", refname=self.__class__.__name__
                )
            else:
                loc_key = self._get_location_key(feat, fieldnames)
                self._row_indices[fid] = loc_key
        self._dataframe = None

    # ...............................................
    def create_data_column(self, fid_count):
        """Add a column to the dataframe.

        Args:
            fid_count (dict): dictionary of fid (row index) and count

        Returns:
            column (list): ordered list of counts for each row index (fid).
        """
        counts = numpy.zeros(self.row_count, dtype=numpy.int32)
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
        row_value_index = []
        for fid in range(self._dataframe.shape[0]):
            row_value_index.append(self._row_indices[fid])
        # Convert the original index to a MultiIndex with the new_index as the second level
        self._dataframe.index = pandas.MultiIndex.from_arrays(
            [self._dataframe.index, row_value_index], names=["fid", "region"]
        )

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
        return len(self._row_indices)

    # ...............................................
    @property
    def rows(self):
        """Return rows in the dataframe as a list.

        Returns:
            The row indexes for the DataFrame instance.
        """
        if self._dataframe is not None:
            return self._dataframe.index.to_list()
        return []

    # ...............................................
    @property
    def column_count(self):
        """Return the number of columns in the dataframe.

        Returns:
            The count of columns for the DataFrame instance.
        """
        if self._dataframe is not None:
            return self._dataframe.shape[1]
        return 0

    # ...............................................
    @property
    def columns(self):
        """Return columns in the dataframe as a list.

        Returns:
            list of column names for the dataframe.
        """
        if self._dataframe is not None:
            return self._dataframe.columns.to_list()
        return []

    # ...............................................
    def row_lookup_by_fid(self):
        """Return a dictionary of the spatial cells/polygons with fid: attributes.

        Returns:
            Dictionary of the spatial cells, with the key as geospatial FID, and the
                value the concatenated attributes (used as additional row indices).
        """
        return self._row_indices

    # ...............................................
    def row_lookup_by_attribute(self):
        """Return a dictionary of the spatial cells with concatenated attributes: fid.

        Returns:
            Dictionary of the spatial cells, with the key as the concatenated
                attributes (used as additional row indices), and the value as FID.
        """
        lookup = {}
        for fid, attribute in self._row_indices.items():
            lookup[attribute] = fid
        return lookup

    # ...............................................
    def _binary_range(self, x, min_val, max_val):
        if x >= min_val:
            if max_val is not None:
                if x <= max_val:
                    return 1
            else:
                return 1
        return 0

    # ...............................................
    def convert_to_binary(self, min_val, max_val=None):
        """Convert the matrix of counts into a binary matrix based on a value range.

        Args:
            min_val (numeric): minimum value for which to code a cell 1.
            max_val (numeric): maximum value for which to code a cell 1.

        Returns:
            df (GeoMatrix): a new GeoMatrix with a binary DataFrame.
        """
        df = self._dataframe.applymap(self._binary_range)
        return df

    # ...............................................
    def write_matrix(self, heatmatrix_filename, overwrite=True):
        """Write the matrix to a zipped CSV file.

        Args:
            heatmatrix_filename (str): Full filename for output pandas.DataFrame.
            overwrite (bool): Flag indicating whether to overwrite existing matrix file(s).

        Raises:
            Exception: on failure to write matrix.
        """
        if ready_filename(heatmatrix_filename, overwrite=overwrite):
            try:
                self._dataframe.to_csv(
                    path_or_buf=heatmatrix_filename, sep=",", header=True, index=True,
                    # index_label=("fid", "region"),
                    mode='w', encoding="utf-8"
                )
            except Exception:
                raise
        else:
            self._log.log(f"File {heatmatrix_filename} already exists.")

    # ...............................................
    def read_matrix(self, matrix_filename):
        """Read a matrix into this datastructure from a CSV file.

        Args:
            matrix_filename (str): full filename for pandas Dataframe.
        """
        self._dataframe = pandas.read_csv(
            matrix_filename, sep=",", index=True, #index_label=["fid", "region"],
            header=True, memory_map=True)
            #index_col=0,



# .............................................................................
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    pass
