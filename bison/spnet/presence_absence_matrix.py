"""Matrix of sites as rows, species as columns, values are presence or absence (1/0)."""
from copy import deepcopy
from logging import ERROR
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
from scipy import sparse

from aws_scripts.bison_matrix_stats import datestr
from bison.common.constants import ANALYSIS_DIM, SNKeys, TMP_PATH
from bison.spnet.aggregate_data_matrix import _AggregateDataMatrix
from bison.spnet.sparse_matrix import SparseMatrix


# .............................................................................
class PAM(SparseMatrix):
    """Class for analyzing presence/absence of aggregator0 x species (aggregator1)."""

    # ...........................
    def __init_(
            self, binary_sparse_mtx, min_presence_count, table_type, datestr,
            row_category, column_category, dim0, dim1):
        """Constructor for species by region/analysis_dim comparisons.

        Args:
            binary_sparse_mtx (scipy.sparse.coo_array): A 2d sparse array with
                presence (1) or absence (0) values for one dimension (i.e. region) rows
                (axis 0) by the species dimension columns (axis 1) to use for analyses.
            table_type (sppy.tools.s2n.constants.SUMMARY_TABLE_TYPES): type of
                aggregated data
            datestr (str): date of the source data in YYYY_MM_DD format.
            row_category (pandas.api.types.CategoricalDtype): ordered row labels used
                to identify axis 0/rows.
            column_category (pandas.api.types.CategoricalDtype): ordered column labels
                used to identify axis 1/columns.
            dim0 (bison.common.constants.ANALYSIS_DIM): dimension for axis 0, rows
            dim1 (bison.common.constants.ANALYSIS_DIM): dimension for axis 1, columns,
                always species dimension in specnet PAM matrices

        Raises:
            Exception: on values
        """
        # Check PAM is boolean (0/1)
        tmp = binary_sparse_mtx > 1
        if tmp.getnnz() > 0:
            raise Exception("Only 0 and 1 are allowed in a Presence-Absence Matrix")
        # Check PAM is numpy.int8
        if binary_sparse_mtx.dtype != np.int8:
            binary_sparse_mtx = binary_sparse_mtx.astype(np.int8)

        self._pam = self._remove_zeros(binary_sparse_mtx)
        self._min_presence = min_presence_count
        SparseMatrix.__init__(binary_sparse_mtx, table_type, datestr, row_category,
            column_category, dim0, dim1, "presence")

    # ...........................
    @classmethod
    def init_from_sparse_matrix(cls, sparse_mtx, min_presence_count):
        """Create a sparse matrix of rows by columns containing values from a table.

        Args:
            sparse_mtx (bison.spnet.sparse_matrix.SparseMatrix): Matrix of occurrence
                counts for sites (or other dimension), rows, by species, columns.
            min_presence_count (int): Minimum occurrence count for a species to be
                considered present at that site.

        Returns:
            pam (bison.spnet.presence_absence_matrix.PAM): matrix of sites (rows, axis=0) by
                species (columnns, axis=1), with binary values indicating presence/absence.

        Raises:
            Exception: on
        """
        bool_sp_array = sparse_mtx._coo_array >= min_presence_count
        pam_sp_array = bool_sp_array.astype(np.int8)
        pam = PAM(
            pam_sp_array, min_presence_count, sparse_mtx.table_type, sparse_mtx.datestr,
            sparse_mtx.row_category, sparse_mtx.column_category,
            sparse_mtx.y_dimension, sparse_mtx.x_dimension)
        return pam

    # ...........................
    @classmethod
    def _remove_zeros(cls, coo):
        """Remove any all-zero rows or columns.

        Args:
            coo (scipy.sparse.coo_array): binary sparse array in coo format

        Returns:
            compressed_coo (scipy.sparse.coo_array): sparse array with no rows or
                columns containing all zeros.
        """
        # Get non-zero column/row indices
        cidx = sparse.find(coo)[1]
        ridx = sparse.find(coo)[0]
        # Which of non-zero indices are present in all indices (shape),
        # then negate to get zero indices
        zero_col_idx = 1 - np.isin(np.arange(coo.shape[1]), cidx)
        zero_row_idx = 1 - np.isin(np.arange(coo.shape[0]), ridx)
        zero_cols = list(zero_col_idx)
        zero_rows = list(zero_row_idx)

        # WARNING: Indices of altered axes are reset in the returned matrix
        csr = coo.tocsr()
        if len(zero_rows) > 0 and len(zero_cols) > 0:
            row_mask = np.ones(csr.shape[0], dtype=bool)
            row_mask[zero_rows] = False
            col_mask = np.ones(csr.shape[1], dtype=bool)
            col_mask[zero_cols] = False
            compressed_csr = csr[row_mask][:, col_mask]

        elif len(zero_rows) > 0:
            mask = np.ones(csr.shape[0], dtype=bool)
            mask[zero_rows] = False
            compressed_csr = csr[mask]
        elif len(zero_cols) > 0:
            mask = np.ones(csr.shape[1], dtype=bool)
            mask[zero_cols] = False
            compressed_csr = csr[:, mask]
        else:
            compressed_csr = csr

        compressed_coo = compressed_csr.tocoo()
        return compressed_coo

    # ...........................
    @property
    def num_species(self):
        self._pam.shape[1]