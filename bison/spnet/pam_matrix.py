"""Matrix of sites as rows, species as columns, values are presence or absence (1/0)."""
import numpy as np
from scipy import sparse

from bison.spnet.heatmap_matrix import HeatmapMatrix


# .............................................................................
class PAM(HeatmapMatrix):
    """Class for analyzing presence/absence of aggregator0 x species (aggregator1)."""

    # ...........................
    def __init__(
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

        self._pam, compressed_row_categ, compressed_col_categ = self._remove_zeros(
            binary_sparse_mtx, row_category, column_category)
        self._min_presence = min_presence_count
        val_fld = "presence"

        HeatmapMatrix.__init__(
            binary_sparse_mtx, table_type, datestr, compressed_row_categ,
            compressed_col_categ, dim0, dim1, val_fld)

    # ...........................
    @classmethod
    def init_from_sparse_matrix(cls, sparse_mtx, min_presence_count):
        """Create a sparse matrix of rows by columns containing values from a table.

        Args:
            sparse_mtx (bison.spnet.heatmap_matrix.HeatmapMatrix): Matrix of occurrence
                counts for sites (or other dimension), rows, by species, columns.
            min_presence_count (int): Minimum occurrence count for a species to be
                considered present at that site.

        Returns:
            pam (bison.spnet.presence_absence_matrix.PAM): matrix of sites (rows, axis=0) by
                species (columnns, axis=1), with binary values indicating presence/absence.

        Raises:
            Exception: on
        """
        # Apply minimum value filter; converts to CSR format
        bool_csr_array = sparse_mtx._coo_array >= min_presence_count
        pam_csr_array = bool_csr_array.astype(np.int8)
        # Go back to COO format
        new_coo = pam_csr_array.tocoo()

        pam = PAM(
            new_coo, min_presence_count, sparse_mtx.table_type, sparse_mtx.datestr,
            sparse_mtx.row_category, sparse_mtx.column_category,
            sparse_mtx.y_dimension, sparse_mtx.x_dimension)
        return pam

    # ...........................
    @classmethod
    def _remove_zeros(cls, coo, row_categ, col_categ):
        """Remove any all-zero rows or columns.

        Args:
            coo (scipy.sparse.coo_array): binary sparse array in coo format

        Returns:
            compressed_coo (scipy.sparse.coo_array): sparse array with no rows or
                columns containing all zeros.
        """
        from pandas.api.types import CategoricalDtype
        # Get indices of col/rows that contain at least one non-zero element, with dupes
        nz_cidx = sparse.find(coo)[1]
        nz_ridx = sparse.find(coo)[0]

        # Get a bool array with elements T if position holds a nonzero
        nonzero_cidx = np.isin(np.arange(coo.shape[1]), nz_cidx)
        nonzero_ridx = np.isin(np.arange(coo.shape[0]), nz_ridx)

        # WARNING: Indices of altered axes are reset in the returned matrix
        # TODO: Modify categories associated with indices
        # Find cols (category and index) with all zeros
        zero_col_idx =  []
        nonzero_col_labels = []
        for zidx in range(len(nonzero_cidx)):
            # If position does not contain a non-zero, mark index/category for deletion
            #   Directly address this 1d boolean numpy.ndarray
            if nonzero_cidx[zidx] is True:
                # Save labels with non-zero elements
                label = cls._get_category_from_code(zidx, col_categ)
                nonzero_col_labels.append(label)
            else:
                # Save indexes with all zero elements
                zero_col_idx.append(zidx)
        new_col_categ = CategoricalDtype(nonzero_col_labels, ordered=True)

        # Find rows (category and index) with all zeros
        zero_row_idx =  []
        nonzero_row_labels = []
        for zidx in range(len(nonzero_ridx)):
            # If true (1) that this position contains a zero in the coo
            if nonzero_ridx[zidx] is True:
                # Save labels with non-zero elements
                label = cls._get_category_from_code(zidx, row_categ)
                nonzero_row_labels.append(label)
            else:
                # Save indexes with all zero elements
                zero_row_idx.append(zidx)
        new_row_categ = CategoricalDtype(nonzero_row_labels, ordered=True)

        # Mask with indices to remove data
        csr = coo.tocsr()
        if len(zero_row_idx) > 0 and len(zero_col_idx) > 0:
            row_mask = np.ones(csr.shape[0], dtype=bool)
            row_mask[zero_row_idx] = False
            col_mask = np.ones(csr.shape[1], dtype=bool)
            col_mask[zero_col_idx] = False
            compressed_csr = csr[row_mask][:, col_mask]

        elif len(zero_row_idx) > 0:
            mask = np.ones(csr.shape[0], dtype=bool)
            mask[zero_row_idx] = False
            compressed_csr = csr[mask]

        elif len(zero_col_idx) > 0:
            mask = np.ones(csr.shape[1], dtype=bool)
            mask[zero_col_idx] = False
            compressed_csr = csr[:, mask]

        else:
            compressed_csr = csr

        compressed_coo = compressed_csr.tocoo()
        return compressed_coo, new_row_categ, new_col_categ

    # ...........................
    @property
    def num_species(self):
        self._pam.shape[1]