"""Matrix to summarize 2 dimensions of data by counts of a third in a sparse matrix."""
import json
from logging import ERROR, INFO
from numpy import integer as np_int, floating as np_float, ndarray
import os
from zipfile import ZipFile

from bison.common.constants import SNKeys, SUMMARY

# from sppy.tools.s2n.constants import (SNKeys, Summaries)
# from sppy.tools.util.logtools import logit


# .............................................................................
class _AggregateDataMatrix:
    """Class for managing computations for counts of species x aggregator."""

    # ...........................
    def __init__(self, table_type, data_datestr, logger=None):
        """Constructor for species by dataset comparisons.

        Args:
            table_type (sppy.tools.s2n.SUMMARY_TABLE_TYPES): type of aggregated data
            data_datestr (str): date of the source data in YYYY_MM_DD format.
            logger (object): An optional local logger to use for logging output
                with consistent options

        Note: in the first implementation, because species are generally far more
            numerous, rows are always species, columns are datasets.  This allows
            easier exporting to other formats (i.e. Excel), which allows more rows than
            columns.

        Note:
            All filenames have the same basename with extensions indicating which data
                they contain. The filename contains a string like YYYY-MM-DD which
                indicates which GBIF data dump the statistics were built upon.
        """
        self._table_type = table_type
        self._data_datestr = data_datestr
        self._table = SUMMARY.get_table(table_type, datestr=data_datestr)
        self._keys = SNKeys.get_keys_for_table(table_type)
        self._logger = logger
        self._report = {}

    # ...........................
    @property
    def table_type(self):
        return self._table_type

    # ...........................
    @property
    def data_datestr(self):
        return self._data_datestr

    # # ...............................................
    # def _logme(self, msg, refname="", log_level=INFO):
    #     logit(self._logger, msg, refname=refname, log_level=log_level)

    # ...............................................
    def _get_input_files(self, local_path="/tmp"):
        """Return the files that comprise local input data, optionally delete.

        Args:
            local_path (str): Absolute path of local destination path

        Returns:
            mtx_fname (str): absolute path for local matrix data file.
            meta_fname (str): absolute path for local metadata file.
            zip_fname (str): absolute path for local compressed file.
        """
        basename = self._table["fname"]
        mtx_ext = self._table["matrix_extension"]
        mtx_fname = f"{local_path}/{basename}{mtx_ext}"
        meta_fname = f"{local_path}/{basename}.json"
        zip_fname = f"{local_path}/{basename}.zip"
        return mtx_fname, meta_fname, zip_fname

    # ......................................................
    @staticmethod
    def convert_np_vals_for_json(obj):
        """Encode numpy values (from matrix operations) for JSON output.

        Args:
            obj: a simple numpy object, value or array

        Returns:
            an object serializable by JSON

        Note:
            from https://stackoverflow.com/questions/27050108/convert-numpy-type-to-python
        """
        if isinstance(obj, np_int):
            return int(obj)
        elif isinstance(obj, np_float):
            return float(obj)
        elif isinstance(obj, ndarray):
            return obj.tolist()
        else:
            return obj

    # ...............................................
    @classmethod
    def _dump_metadata(self, metadata, meta_fname):
        """Write metadata to a json file, deleting it first if exists.

        Args:
            metadata (dict): metadata about matrix
            meta_fname (str): local output filename for JSON metadata.

        Raises:
            Exception: on failure to serialize metadata as JSON.
            Exception: on failure to write metadata json string to file.
        """
        if os.path.exists(meta_fname):
            os.remove(meta_fname)
            print(f"Removed file {meta_fname}.")

        try:
            metastr = json.dumps(metadata)
        except Exception as e:
            raise Exception(f"Failed to serialize metadata as JSON: {e}")
        try:
            with open(meta_fname, 'w') as outf:
                outf.write(metastr)
        except Exception as e:
            raise Exception(f"Failed to write metadata to {meta_fname}: {e}")

    # ...............................................
    @classmethod
    def load_metadata(cls, meta_filename):
        """Read JSON metadata for a matrix.

        Args:
            meta_filename (str): Filename of metadata to read .

        Returns:
            meta_dict (dict): metadata for a matrix

        Raises:
            Exception: on failure to read file.
            Exception: on failure load JSON metadata into a dictionary
        """
        # Read JSON dictionary as string
        try:
            with open(meta_filename) as metaf:
                meta_str = metaf.read()
        except Exception as e:
            raise Exception(f"Failed to load {meta_filename}: {e}")
        # Load metadata from string
        try:
            meta_dict = json.loads(meta_str)
        except Exception as e:
            raise Exception(f"Failed to load {meta_filename}: {e}")

        return meta_dict

    # ...............................................
    @classmethod
    def _check_for_existing_files(cls, expected_files, overwrite):
        deleted_files = []
        # Are local files already present?
        files_present = [fname for fname in expected_files if os.path.exists(fname)]
        # Delete if overwrite is true or if not all expected files are present
        if overwrite is True or len(files_present) < len(expected_files):
            for fname in files_present:
                os.remove(fname)
                deleted_files.append(fname)
            print(f"Removed files {', '.join(deleted_files)}.")
        # Who remains?
        files_present = [fname for fname in expected_files if os.path.exists(fname)]
        all_exist = len(files_present) == len(expected_files)
        return all_exist, deleted_files

    # ...............................................
    def _compress_files(self, input_fnames, zip_fname):
        if os.path.exists(zip_fname):
            os.remove(zip_fname)
            self._logme(f"Removed file {zip_fname}.")

        try:
            with ZipFile(zip_fname, 'w') as zip:
                for fname in input_fnames:
                    zip.write(fname, os.path.basename(fname))
        except Exception as e:
            msg = f"Failed to write {zip_fname}: {e}"
            self._logme(msg, log_level=ERROR)
            raise Exception(msg)

    # .............................................................................
    def _remove_expected_files(self, local_path="/tmp"):
        # Always delete local files before compressing this data.
        overwrite = True
        [mtx_fname, meta_fname, zip_fname] = self._get_input_files(local_path=local_path)
        all_exist, deleted_files = self._check_for_existing_files(
            [mtx_fname, meta_fname, zip_fname], overwrite)
        if deleted_files:
            self._logme(f"Deleted existing files {','.join(deleted_files)}.")
        return [mtx_fname, meta_fname, zip_fname]

    # .............................................................................
    @classmethod
    def _uncompress_files(cls, zip_filename, local_path, overwrite=False):
        """Uncompress a zipped SparseMatrix into a coo_array and row/column categories.

        Args:
            zip_filename (str): Filename of output data to write to S3.
            local_path (str): Absolute path of local destination path
            overwrite (bool): Flag indicating whether to use or delete existing files
                prior to unzipping.

        Returns:
            sparse_coo (scipy.sparse.coo_array): Sparse Matrix containing data.
            row_categ (pandas.api.types.CategoricalDtype): row categories
            col_categ (pandas.api.types.CategoricalDtype): column categories
            table_type (aws.aws_constants.SUMMARY_TABLE_TYPES): type of table data
            data_datestr (str): date string in format YYYY_MM_DD

        Raises:
            Exception: on missing input zipfile
            Exception: on failure to parse filename
            Exception: on missing expected file from zipfile
        """
        if not os.path.exists(zip_filename):
            raise Exception(f"Missing file {zip_filename}")
        basename = os.path.basename(zip_filename)
        fname, _ext = os.path.splitext(basename)
        try:
            table_type, data_datestr = Summaries.get_tabletype_datestring_from_filename(
                zip_filename)
        except Exception:
            raise

        table = Summaries.get_table(table_type)
        mtx_ext = table["matrix_extension"]
        # Expected files from archive
        mtx_fname = f"{local_path}/{fname}{mtx_ext}"
        meta_fname = f"{local_path}/{fname}.json"

        # Are local files already present?
        expected_files = [mtx_fname, meta_fname]
        all_exist, _deleted_files = cls._check_for_existing_files(
            expected_files, overwrite)

        if all_exist and overwrite is False:
            print(f"Expected files {', '.join(expected_files)} already exist.")
        else:
            # Unzip to local dir
            with ZipFile(zip_filename, mode="r") as archive:
                archive.extractall(f"{local_path}/")
            for fn in [mtx_fname, meta_fname]:
                if not os.path.exists(fn):
                    raise Exception(f"Missing expected file {fn}")

        return mtx_fname, meta_fname, table_type, data_datestr
