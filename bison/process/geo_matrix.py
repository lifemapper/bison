"""Class for a spatial index and tools for intersecting with a point and extracting attributes."""
import logging
import csv
import os
import numpy
import pandas
from osgeo import ogr
import sys

from bison.common.log import Logger
from bison.common.util import BisonKey, ready_filename


# .............................................................................
class SiteMatrix(object):
    """Object for intersecting coordinates with a polygon shapefile."""
    def __init__(
            self, dataframe=None, matrix_filename=None, spatial_filename=None,
            fieldnames=None, logger=None):
        """Construct a Dataframe containing counts from a shapefile or matrix file.

        Args:
            dataframe (pandas.DataFrame): dataframe for this object, where rows
                represent sites and columns represent species.  The dataframe must have
                a MultiIndex for rows containing fid and location, and an Index for
                columns.  Species names or codes for column headers are optional but
                preferred.
            matrix_filename (str): full filename for a zipped CSV file containing a
                pandas.DataFrame,
            spatial_filename (str): full filename for a shapefile of polygons to
                construct a matrix defined by one row per polygon, identified by a
                feature identifier (FID), and optionally, a string of one or more
                concatenated attributes, unique to that polygon.
            fieldnames (iterable): lisf of fieldname for polygon attributes of interest
            logger (object): logger for saving relevant processing messages

        Raises:
            Exception: on dataframe without a MultiIndex of length 2.
            FileNotFoundError: if matrix_filename does not exist on the file system
            FileNotFoundError: if spatial_filename does not exist on the file system
            Exception: if no fieldnames accompanying a spatial_filename.
            Exception: if none of spatiol_filename, matrix_filename, or dataframe
                provided.
        """
        if logger is None:
            logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
        self._log = logger
        # # Dictionary with FID (index): a string/list of additional row names
        # self._row_indices = {}
        self._df = None
        self._matrix_filename = matrix_filename
        self._spatial_filename = spatial_filename
        self._fieldnames = fieldnames
        self._min_presence = 0
        self._max_presence = None
        self._row_indices = {}

        if dataframe is not None:
            if not (
                    type(dataframe.index) is pandas.core.indexes.multi.MultiIndex and
                    len(dataframe.index.names) == 2
            ):
                # output matrix may only contain species columns
                self._log.log(
                    "Site index does not contain a MultiIndex with FID and location")
            if type(dataframe.columns) is pandas.core.indexes.range.RangeIndex:
                # output matrix may only contain site rows
                self._log.log(
                    "Species columns contain a RangeIndex, not species names.",
                    refname=self.__class__.__name__, log_level=logging.WARNING)
            self._df = dataframe
        elif matrix_filename:
            if os.path.exists(matrix_filename):
                self._read_matrix(matrix_filename)
            else:
                raise FileNotFoundError(f"{spatial_filename}")
        elif spatial_filename:
            if os.path.exists(spatial_filename):
                if fieldnames is None:
                    raise Exception(
                        "SiteMatrix expects fieldnames with a spatial dataset")
                # Save a dictionary of rows/sites - {fid: location_name}
                self._row_indices = self._get_row_indices(spatial_filename, fieldnames)
            else:
                raise FileNotFoundError(f"{spatial_filename}")
        else:
            raise Exception(
                "Must provide either spatial_filename and fieldnames, matrix_filename, "
                "or dataframe.")

    # ...............................................
    def _get_location_key(self, feat, fieldnames):
        values = [feat.GetField(fn) for fn in fieldnames]
        key = BisonKey.get_compound_key(*values)
        return key

    # ...............................................
    def _get_row_indices(self, spatial_filename, fieldnames):
        row_indices = {}
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
                row_indices[fid] = loc_key
        return row_indices

    # ...............................................
    @property
    def dataframe(self):
        """Get the data in this object.

        Returns:
            (pandas.DataFrame) in this object.
        """
        return self._df

    # ...............................................
    def create_data_column(self, fid_count):
        """Add a column to the dataframe.

        Args:
            fid_count (dict): dictionary of fid (row index) and count

        Returns:
            column (list): ordered list of counts for each row index (fid).
        """
        counts = []
        fids = list(fid_count.keys())
        fids.sort()
        for fid in fids:
            counts.append(fid_count[fid])
        return counts

    # ...............................................
    def create_dataframe_from_cols(self, species_cols):
        """Add a column to the dataframe.

        Args:
            species_cols (dict): keys contain species(column) name, with
                a value of a list of counts for each row index (fid).

        Raises:
            Exception: on self not containing row indices defining site FIDs and
                locations.

        Note:
            Function assumes that the object has been initialized with spatial data
                defining the rows (sites) with FIDs and locations from a vector dataset.
        """
        if not self._row_indices:
            raise Exception(
                "SiteMatrix must first be initialized with a vector dataset "
                "to define rows indices as FIDs and location_names.")
        self._df = pandas.DataFrame(species_cols)
        row_value_index = []
        for fid in range(self._df.shape[0]):
            row_value_index.append(self._row_indices[fid])
        # Convert the original index to a MultiIndex with the new_index as the second level
        self._df.index = pandas.MultiIndex.from_arrays(
            [self._df.index, row_value_index], names=["fid", "region"]
        )

    # ...............................................
    @property
    def matrix(self):
        """Return the pandas dataframe.

        Returns:
            The pandas.DataFrame for this instance.
        """
        return self._df

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
        if self._df is not None:
            return self._df.index.to_list()
        return []

    # ...............................................
    @property
    def column_count(self):
        """Return the number of columns in the dataframe.

        Returns:
            The count of columns for the DataFrame instance.
        """
        if self._df is not None:
            return self._df.shape[1]
        return 0

    # ...............................................
    @property
    def data(self):
        """Return the data of the dataframe.

        Returns:
            The ndarray data for the DataFrame instance.
        """
        if self._df is not None:
            return self._df.data
        return None

    # ...............................................
    @property
    def columns(self):
        """Return columns in the dataframe as a list.

        Returns:
            list of column names for the dataframe.
        """
        if self._df is not None:
            return self._df.columns.to_list()
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
    def _binary_range(self, x):
        # Note: min_val < x <= max_val
        if x > self._min_presence:
            if self._max_presence is not None:
                if x <= self._max_presence:
                    return 1
            else:
                return 1
        return 0

    # ...............................................
    def convert_to_binary(self, min_val=0, max_val=None):
        """Convert the matrix of counts into a binary matrix based on a value range.

        Args:
            min_val (numeric): value must be > min_val to code a cell 1.
            max_val (numeric): value must be <= to max_val to code a cell 1.

        Note: min_val < x <= max_val

        Returns:
            mtx (SiteMatrix): a new SiteMatrix with a binary DataFrame.
        """
        self._min_presence = min_val
        self._max_presence = max_val
        bin_df = self._df.applymap(self._binary_range)
        mtx = SiteMatrix(dataframe=bin_df)

        return mtx

    # ...............................................
    @classmethod
    def concat_columns(cls, series_list):
        """Concatenate multiple columns of values for sites into a matrix.

        Args:
            series_list (list of pandas.Series): Sequence of series representing values
                for sites.

        Returns:
            mtx (SiteMatrix): containing one or more columns.
        """
        df = pandas.concat(series_list, axis=1)
        mtx = SiteMatrix(dataframe=df)
        return mtx

    # ...............................................
    @classmethod
    def concat_rows(cls, series_list):
        """Concatenate multiple columns of values for sites into a matrix.

        Args:
            series_list (list of pandas.Series): Sequence of series representing values
                for species.

        Returns:
            mtx (SiteMatrix): containing one or more rows.
        """
        df = pandas.concat(series_list, axis=1)
        mtx = SiteMatrix(dataframe=df.T)
        return mtx

    # ...............................................
    def write_matrix(self, filename, overwrite=True):
        """Write the matrix to a CSV file.

        Args:
            filename (str): Full filename for output pandas.DataFrame.
            overwrite (bool): Flag indicating whether to overwrite existing matrix file(s).

        Raises:
            Exception: on failure to write matrix.
        """
        if ready_filename(filename, overwrite=overwrite):
            try:
                self._df.to_csv(
                    path_or_buf=filename, sep=",", header=True, index=True,
                    mode='w', encoding="utf-8"
                )
            except Exception:
                raise
        else:
            self._log.log(f"File {filename} already exists.")

    # ...............................................
    def _read_matrix(self, matrix_filename):
        """Read a matrix into this datastructure from a CSV file.

        Args:
            matrix_filename (str): full filename for pandas Dataframe.
        """
        self._df = pandas.read_csv(
            matrix_filename, sep=",", index_col=["fid", "region"],
            memory_map=True)
        for fid, loc_key in self._df.index:
            self._row_indices[fid] = loc_key

    # ...............................................
    @property
    def num_species(self):
        """Get the number of species with at least one site present.

        Returns:
            int: The number of species that are present somewhere.

        Note:
            Also used as gamma diversity (species richness over entire landscape)
        """
        count = 0
        if self._df is not None:
            count = int(self._df.any(axis=0).sum())
        return count

    # ...............................................
    @property
    def num_sites(self):
        """Get the number of sites with presences.

        Returns:
            int: The number of sites that have present species.
        """
        count = 0
        if self._df is not None:
            count = int(self._df.any(axis=1).sum())
        return count

    # ...............................................
    def alpha(self):
        """Calculate alpha diversity, the number of species in each site.

        Returns:
            A series of alpha diversity values for each site.
        """
        alpha_series = None
        if self._df is not None:
            alpha_series = self._df.sum(axis=1)
            alpha_series.name = "alpha_diversity"
        return alpha_series

    # ...............................................
    def alpha_proportional(self):
        """Calculate proportional alpha diversity - percentage of species in each site.

        Returns:
            A series of proportional alpha diversity values for each site.
        """
        alpha_pr_series = None
        if self._df is not None:
            alpha_pr_series = self._df.sum(axis=1) / float(self.num_species)
            alpha_pr_series.name = "alpha_proportional_diversity"
        return alpha_pr_series

    # ...............................................
    def beta(self):
        """Calculate beta diversity for each site, Whitaker's ratio: gamma/alpha.

        Returns:
            beta_series (pandas.Series): ratio of gamma to alpha for each site.
        """
        beta_series = None
        if self._df is not None:
            beta_series = float(self.num_species) / self._df.sum(axis=1)
            beta_series.replace([numpy.inf, -numpy.inf], 0, inplace=True)
            beta_series.name = "beta_diversity"
        return beta_series

    # ...............................................
    def omega(self):
        """Calculate the range size (number of counties) per species.

        Returns:
            omega_series (pandas.Series): A row of range sizes for each species.
        """
        omega_series = None
        if self._df is not None:
            omega_series = self._df.sum(axis=0)
            omega_series.name = "omega"
        return omega_series

    # ...............................................
    def omega_proportional(self):
        """Calculate the mean proportional range size of each species.

        Returns:
            beta_series (pandas.Series): A row of the proportional range sizes for
                each species.
        """
        omega_pr_series = None
        if self._df is not None:
            omega_pr_series = self._df.sum(axis=0) / float(self.num_sites)
        omega_pr_series.name = "omega_proportional"
        return omega_pr_series

    # # ...............................................
    # def schluter_species_variance_ratio(self):
    #     """Calculate Schluter's species variance ratio.
    #
    #     Returns:
    #         float: The Schluter species variance ratio for the PAM.
    #     """
    #     sigma_species_, _hdrs = sigma_species(pam)
    #     return float(sigma_species_.sum() / sigma_species_.trace())

    # ...............................................
    # def schluter_site_variance_ratio(self):
    #     """Calculate Schluter's site variance ratio.
    #
    #     Returns:
    #         float: The Schluter site variance ratio for the PAM.
    #     """
    #     sigma_sites_, _hdrs = sigma_sites(pam)
    #     return float(sigma_sites_.sum() / sigma_sites_.trace())

    # ...............................................
    def whittaker(self):
        """Calculate Whittaker's beta diversity metric for a PAM.

        Returns:
            float: Whittaker's beta diversity for the PAM.
        """
        val = float(self.num_species / self.omega_proportional().sum())
        return "whittaker_beta_diversity", val

    # ...............................................
    def lande(self):
        """Calculate Lande's beta diversity metric for a PAM.

        Returns:
            float: Lande's beta diversity for the PAM.
        """
        val = float(
            self.num_species
            - (self._df.sum(axis=0).astype(float) / self.num_sites).sum()
        )
        return "Lande_beta_diversity", val

    # ...............................................
    def legendre(self):
        """Calculate Legendre's beta diversity metric for a PAM.

        Returns:
            float: Legendre's beta diversity for the PAM.
        """
        val = float(
            self.omega().sum() - (float((self.omega() ** 2).sum()) / self.num_sites)
        )
        return "Legendre_beta_diversity", val

    # # ...............................................
    # def c_score(self):
    #     """Calculate the checker board score for the PAM.
    #
    #     Returns:
    #         float: The checkerboard score for the PAM.
    #     """
    #     temp = 0.0
    #     # Cache these so we don't recompute
    #     omega_ = self.omega()  # Cache so we don't waste computations
    #     num_species_ = self.num_species
    #
    #     for i in range(num_species_):
    #         for j in range(i, num_species_):
    #             num_shared = len(
    #                 pandas.where(self._df.sum(self._df[:, [i, j]], axis=1) == 2)[0]
    #             )
    #             p_1 = omega_[i] - num_shared
    #             p_2 = omega_[j] - num_shared
    #             temp += p_1 * p_2
    #     val = 2 * temp / (num_species_ * (num_species_ - 1))
    #     return "checker_board_score", val

    # ...............................................
    def calc_write_pam_measures(self, filename, overwrite=True):
        """Calculate all matrix-level measures for the PAM and write to file.

        Args:
            filename (str): Full filename for output pandas.DataFrame.
            overwrite (bool): Flag indicating whether to overwrite existing file.

        Raises:
            Exception: on failure to open file.
            Exception: on failure to get CSVWriter or write a row.
        """
        stats = []
        stats.append(("gamma_diversity", self.num_species))
        stats.append(self.whittaker())
        stats.append(self.lande())
        stats.append(self.legendre())
        # stats.append(self.c_score())
        columns = [stat[0] for stat in stats]
        vals = [stat[1] for stat in stats]

        if ready_filename(filename, overwrite=overwrite):
            csv.field_size_limit(sys.maxsize)
            try:
                f = open(filename, "w", newline="", encoding="utf-8")
            except Exception:
                raise
            else:
                try:
                    writer = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(columns)
                    writer.writerow(vals)
                except Exception:
                    raise
                finally:
                    f.close()
        else:
            self._log.log(f"File {filename} already exists.")


# # .............................................................................
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     """Main script to execute all elements of the summarize-GBIF BISON workflow."""
#     pass
#

"""
# diversity stats
metrics = [
            ('c-score', stats.c_score),
            ('lande', stats.lande),
            ('legendre', stats.legendre),
            ('num sites', stats.num_sites),
            ('num species', stats.num_species),
            ('whittaker', stats.whittaker),
        ]
# site stats
metrics = [
            ('alpha', stats.alpha),
            ('alpha proportional', stats.alpha_proportional),
            ('phi', stats.phi),
            ('phi average proportional', stats.phi_average_proportional),
        ]
"""
