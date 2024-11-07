"""Matrix to summarize each of 2 dimensions of data by counts of the other and a third."""
from collections import OrderedDict
import pandas as pd

from bison.common.constants import (
    COUNT_FLD, CSV_DELIMITER, SNKeys, TOTAL_FLD, SUMMARY
)
from bison.spnet.aggregate_data_matrix import _AggregateDataMatrix


# .............................................................................
class SummaryMatrix(_AggregateDataMatrix):
    """Class for holding summary counts of each of 2 dimensions of data."""

    # ...........................
    def __init__(
            self, summary_df, table_type, data_datestr, logger=None):
        """Constructor for occurrence/species counts by region/analysis_dim comparisons.

        Args:
            summary_df (pandas.DataFrame): DataFrame with a row for each element in
                category, and 2 columns of data.  Rows headers and column headers are
                labeled.
                * Column 1 contains the count of the number of columns in  that row
                * Column 2 contains the total of values in that row.
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            logger (object): An optional local logger to use for logging output
                with consistent options

        Note: constructed from records in table with datatype "counts" in
            bison.common.constants.SUMMARY.DATATYPES,
            i.e. county-x-riis_counts where each record has
                county, riis_status, occ_count, species_count;
                counts of occurrences and species by riis_status for a county
            OR
            county_counts where each record has
                county, occ_count, species_count;
                counts of occurrences and species for a county
        """
        self._df = summary_df
        _AggregateDataMatrix.__init__(self, table_type, data_datestr, logger=logger)

    # ...........................
    @classmethod
    def init_from_sparse_matrix(cls, sp_mtx, axis=0, logger=None):
        """Summarize a matrix into counts of one axis and values for the other axis.

        Args:
            sp_mtx (sppy.aws.SparseMatrix): A sparse matrix with count
                values for one dimension, (i.e. region) rows (axis 0), by the species
                dimension, columns (axis 1), to use for computations.
            axis (int): Summarize rows (0) or columns (1).
            logger (object): logger for saving relevant processing messages

        Returns:
            sparse_coo (pandas.DataFrame): DataFrame summarizing rows by the count and
                value of columns, or columns by the count amd value of rows.

        Note:
            The input dataframe must contain only one input record for any x and y value
                combination, and each record must contain another value for the
                dataframe contents.
        """
        # Total/Count down axis 0/row, rows for a column
        #             across axis 1/column, columns for a row (aka species)
        # Axis 0 produces a matrix shape (col_count, 1),
        #   1 row of values, total for each column
        #
        # Axis 1 produces matrix shape (row_count, 1),
        #   1 column of values, total for each row
        totals = sp_mtx.get_totals(axis=axis)
        counts = sp_mtx.get_counts(axis=axis)
        data = {COUNT_FLD: counts, TOTAL_FLD: totals}
        input_table_meta = SUMMARY.get_table(sp_mtx.table_type)
        dim0 =  input_table_meta["dimension0"]
        dim1 =  input_table_meta["dimension1"]

        # Axis 0 summarizes each column/species (down axis 0) of sparse matrix
        if axis == 0:
            index = sp_mtx.column_category.categories
            table_type = SUMMARY.get_table_type("summary", dim0, dim1)
        # Axis 1 summarizes each row (across axis 1) of sparse matrix
        elif axis == 1:
            index = sp_mtx.row_category.categories
            table_type = SUMMARY.get_table_type("summary", dim1, dim0)

        # summary fields = columns, sparse matrix axis = rows
        sdf = pd.DataFrame(data=data, index=index)

        summary_matrix = SummaryMatrix(
            sdf, table_type, sp_mtx.data_datestr, logger=logger)
        return summary_matrix

    # ...............................................
    @property
    def num_items(self):
        """Get the number of rows (each with measurements).

        Returns:
            int: The count of rows
        """
        return self._df.shape[0]

    # ...............................................
    @property
    def num_measures(self):
        """Get the number of columns (measurements).

        Returns:
            int: The count of columns
        """
        return self._df.shape[1]

    # ...............................................
    def get_random_row_labels(self, count):
        """Get random values from the labels on axis 0 of matrix.

        Args:
            count (int): number of values to return

        Returns:
            labels (list): random row headers
        """
        import random
        size = len(self._df.index)
        # Get a random sample of category indexes (0-based)
        idxs = random.sample(range(size), count)
        labels = [self._df.index[i] for i in idxs]
        return labels

    # .............................................................................
    def compress_to_file(self, local_path="/tmp"):
        """Compress this SparseMatrix to a zipped npz and json file.

        Args:
            local_path (str): Absolute path of local destination path

        Returns:
            zip_fname (str): Local output zip filename.

        Raises:
            Exception: on failure to write dataframe to CSV file.
            Exception: on failure to serialize or write metadata as JSON.
            Exception: on failure to write matrix and metadata files to zipfile.
        """
        # Always delete local files before compressing this data.
        [mtx_fname, meta_fname, zip_fname] = self._remove_expected_files(
            local_path=local_path)

        # Save matrix to csv locally
        try:
            self._df.to_csv(mtx_fname, sep=CSV_DELIMITER)
        except Exception as e:
            msg = f"Failed to write {mtx_fname}: {e}"
            raise Exception(msg)

        # Save table data and categories to json locally
        metadata = SUMMARY.get_table(self._table_type)
        try:
            self._dump_metadata(metadata, meta_fname)
        except Exception:
            raise

        # Compress matrix with metadata
        try:
            self._compress_files([mtx_fname, meta_fname], zip_fname)
        except Exception:
            raise

        return zip_fname

    # .............................................................................
    @classmethod
    def uncompress_zipped_data(
            cls, zip_filename, local_path="/tmp", overwrite=False):
        """Uncompress a zipped SparseMatrix into a coo_array and row/column categories.

        Args:
            zip_filename (str): Filename of output data to write to S3.
            local_path (str): Absolute path of local destination path
            overwrite (bool): Flag indicating whether to use existing files unzipped
                from the zip_filename.

        Returns:
            dataframe (pandas.DataFrame): dataframe containing summary matrix data.
            meta_dict (dict): metadata for the matrix
            table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
            data_datestr (str): date string in format YYYY_MM_DD

        Raises:
            Exception: on failure to uncompress files.
            Exception: on failure to load data from uncompressed files.
        """
        try:
            mtx_fname, meta_fname, table_type, data_datestr = cls._uncompress_files(
                zip_filename, local_path, overwrite=overwrite)
        except Exception:
            raise

        # Read matrix data from local files
        try:
            dataframe, meta_dict = cls.read_data(mtx_fname, meta_fname)
        except Exception:
            raise

        return dataframe, meta_dict, table_type, data_datestr

    # .............................................................................
    @classmethod
    def read_data(cls, mtx_filename, meta_filename):
        """Read SummaryMatrix data files into a dataframe and metadata dictionary.

        Args:
            mtx_filename (str): Filename of pandas.DataFrame data in csv format.
            meta_filename (str): Filename of JSON summary matrix metadata.

        Returns:
            dataframe (pandas.DataFrame): dataframe containing summary matrix data.
            meta_dict (dict): metadata for the matrix
            table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
            data_datestr (str): date string in format YYYY_MM_DD

        Raises:
            Exception: on unable to load CSV file
            Exception: on unable to load JSON metadata
        """
        # Read dataframe from local CSV file
        try:
            dataframe = pd.read_csv(mtx_filename, sep=CSV_DELIMITER, index_col=0)
        except Exception as e:
            raise Exception(f"Failed to load {mtx_filename}: {e}")
        # Read JSON dictionary as string
        try:
            meta_dict = cls.load_metadata(meta_filename)
        except Exception:
            raise

        return dataframe, meta_dict

    # ...............................................
    def get_measures(self, summary_key):
        """Get a dictionary of statistics for the summary row with this label.

        Args:
            summary_key: label on the row to gather stats for.

        Returns:
            stats (dict): quantitative measures of the item.
        """
        # Get measurements (pandas series)
        measures = self._df.loc[summary_key]
        stats = {
            self._keys[SNKeys.ONE_LABEL]: summary_key,
            self._keys[SNKeys.ONE_COUNT]: measures.loc[COUNT_FLD],
            self._keys[SNKeys.ONE_TOTAL]: measures.loc[TOTAL_FLD]
        }
        return stats

    # ...........................
    def rank_measures(self, sort_by, order="descending", limit=10):
        """Order records by sort_by field and return the top or bottom limit records.

        Args:
            sort_by (str): field containing measurement to sort on
                (options: SUMMARY_FIELDS.COUNT, SUMMARY_FIELDS.TOTAL).
            order (str): return records, sorted from top (descending) or bottom
                (ascending).
            limit (int): number of records to return.

        Returns:
            ordered_rec_dict (OrderedDict): records containing all fields, sorted by the
                sort_by field.

        Raises:
            Exception: on sort field does not exist in data.
        """
        measure_flds = self._keys["fields"].copy()
        try:
            measure_flds.remove(sort_by)
        except ValueError:
            raise Exception(
                f"Field {sort_by} does not exist; sort by one of {self._keys['fields']}")
        # Get largest and down
        if order == "descending":
            sorted_df = self._df.nlargest(limit, sort_by, keep="all")
        # Get smallest and up
        elif order == "ascending":
            sorted_df = self._df.nsmallest(limit, sort_by, keep="all")
        else:
            raise Exception(
                f"Order {sort_by} does not exist, use 'ascending' or 'descending')")
        # Returns dict with each measurement in a separate dictionary, so re-arrange
        rec_dict = sorted_df.to_dict()
        ordered_rec_dict = OrderedDict()
        # Create records from the sorted measurement first, in order returned
        for k, v in rec_dict[sort_by]:
            ordered_rec_dict[k] = {sort_by: v}
            # measure_flds now contains all fields except sort_by
            #   add remaining fields and values to the new ordered records
            for other_fld in measure_flds:
                ordered_rec_dict[k][other_fld] = rec_dict[other_fld][k]
        return ordered_rec_dict
