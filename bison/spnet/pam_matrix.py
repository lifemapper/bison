"""Matrix of sites as rows, species as columns, values are presence or absence (1/0)."""
from copy import deepcopy
import numpy as np
import pandas as pd

from bison.spnet.heatmap_matrix import HeatmapMatrix


# TODO: Is this an ephemeral data structure used only for computing stats?
#       If we want to save it, we must add compress_to_file,
#       uncompress_zipped_data, read_data.
#       If we only save computations, must save input HeatmapMatrix metadata
#       and min_presence_count.
#       Note table_type and metadata in bison.common.constants.SUMMARY
# .............................................................................
class PAM(HeatmapMatrix):
    """Class for analyzing presence/absence of aggregator0 x species (aggregator1)."""
    # site_pam_dist_mtx_stats = [('pearson_correlation', pearson_correlation)]

    # ...........................
    def __init__(
            self, binary_coo_array, min_presence_count, table_type, datestr,
            row_category, column_category, dim0, dim1):
        """Constructor for species by region/analysis_dim comparisons.

        Args:
            binary_coo_array (scipy.sparse.coo_array): A 2d sparse array with
                presence (1) or absence (0) values for one dimension (i.e. region) rows
                (axis 0) by the species dimension columns (axis 1) to use for analyses.
            min_presence_count (int): minimum value to be considered presence.
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
            Exception: on values other than 0 or 1.

        Note:
            By definition, a Presence-Absence Matrix is site x species.  This
                implementation defines `site` as any type of geographic (state, county,
                Indian lands, Protected Areas) or other classification (dataset,
                organization, US-RIIS status) where every occurrence contains at most
                one `site` value. Some statistics may assume that all occurrences will
                contain a site value, but this implementation does not enforce that
                assumption.
        """
        # Check PAM is binary (0/1)
        tmp = binary_coo_array > 1
        if tmp.getnnz() > 0:
            raise Exception("Only 0 and 1 are allowed in a Presence-Absence Matrix")
        # Check PAM is numpy.int8
        if binary_coo_array.dtype != np.int8:
            binary_coo_array = binary_coo_array.astype(np.int8)

        cmp_pam_coo_array, cmp_row_categ, cmp_col_categ = self._remove_zeros(
            binary_coo_array, row_category, column_category)
        self._min_presence = min_presence_count
        val_fld = "presence"

        HeatmapMatrix.__init__(
            self, cmp_pam_coo_array, table_type, datestr, cmp_row_categ, cmp_col_categ,
            dim0, dim1, val_fld)

    # # ...........................
    # @classmethod
    # def init_from_heatmap(cls, heatmap, min_presence_count):
    #     """Create a sparse matrix of rows by columns containing values from a table.
    #
    #     Args:
    #         heatmap (bison.spnet.heatmap_matrix.HeatmapMatrix): Matrix of occurrence
    #             counts for sites (or other dimension), rows, by species, columns.
    #         min_presence_count (int): Minimum occurrence count for a species to be
    #             considered present at that site.
    #
    #     Returns:
    #         pam (bison.spnet.presence_absence_matrix.PAM): matrix of
    #             sites (rows, axis=0) by species (columnns, axis=1), with binary values
    #             indicating presence/absence.
    #     """
    #     # Apply minimum value filter; converts to CSR format
    #     bool_csr_array = heatmap._coo_array >= min_presence_count
    #     pam_csr_array = bool_csr_array.astype(np.int8)
    #     # Go back to COO format
    #     pam_coo_array = pam_csr_array.tocoo()
    #
    #     pam = PAM(
    #         pam_coo_array, min_presence_count, heatmap.table_type, heatmap.datestr,
    #         heatmap.row_category, heatmap.column_category,
    #         heatmap.y_dimension, heatmap.x_dimension)
    #     return pam

    # # ...........................
    # @classmethod
    # def init_from_heatmap1(cls, heatmap, min_presence_count):
    #     """Create a sparse matrix of rows by columns containing values from a table.
    #
    #     Args:
    #         heatmap (bison.spnet.heatmap_matrix.HeatmapMatrix): Matrix of occurrence
    #             counts for sites (or other dimension), rows, by species, columns.
    #         min_presence_count (int): Minimum occurrence count for a species to be
    #             considered present at that site.
    #
    #     Returns:
    #         pam (bison.spnet.presence_absence_matrix.PAM): matrix of
    #             sites (rows, axis=0) by species (columnns, axis=1), with binary values
    #             indicating presence/absence.
    #     """
    #     # Apply minimum value filter; converts to CSR format
    #     bool_csr_array = heatmap._coo_array >= min_presence_count
    #     pam_csr_array = bool_csr_array.astype(np.int8)
    #     # Go back to COO format
    #     pam_coo_array = pam_csr_array.tocoo()
    #
    #     pam = PAM(
    #         pam_coo_array, min_presence_count, heatmap.table_type, heatmap.datestr,
    #         heatmap.row_category, heatmap.column_category,
    #         heatmap.y_dimension, heatmap.x_dimension)
    #     return pam
    #
    # ...........................
    @classmethod
    def init_from_heatmap(cls, heatmap, min_presence_count):
        """Create a sparse matrix of rows by columns containing values from a table.

        Args:
            heatmap (bison.spnet.heatmap_matrix.HeatmapMatrix): Matrix of occurrence
                counts for sites (or other dimension), rows, by species, columns.
            min_presence_count (int): Minimum occurrence count for a species to be
                considered present at that site.

        Returns:
            pam (bison.spnet.presence_absence_matrix.PAM): matrix of
                sites (rows, axis=0) by species (columnns, axis=1), with binary values
                indicating presence/absence.
        """
        filtered_heatmap = heatmap.filter(
            min_count=min_presence_count)

        # Convert to boolean (all True because pre-filtered)
        bool_csr_array = filtered_heatmap._coo_array > 0
        # Convert to binary
        pam_csr_array = bool_csr_array.astype(np.int8)
        # Go back to COO format
        pam_coo_array = pam_csr_array.tocoo()

        pam = PAM(
            pam_coo_array, min_presence_count, heatmap.table_type, heatmap.datestr,
            heatmap.row_category, heatmap.column_category,
            heatmap.y_dimension, heatmap.x_dimension)
        return pam

    # # ...........................
    # @classmethod
    # def _remove_zeros(cls, coo, row_categ, col_categ):
    #     """Remove any all-zero rows or columns.
    #
    #     Args:
    #         coo (scipy.sparse.coo_array): binary sparse array in coo format
    #
    #     Returns:
    #         compressed_coo (scipy.sparse.coo_array): sparse array with no rows or
    #             columns containing all zeros.
    #         row_category (pandas.api.types.CategoricalDtype): ordered row labels used
    #             to identify axis 0/rows in the new compressed matrix.
    #         column_category (pandas.api.types.CategoricalDtype): ordered column labels
    #             used to identify axis 1/columns in the new compressed matrix.
    #     """
    #     # Get indices of col/rows that contain at least one non-zero element, with dupes
    #     nz_cidx = sparse.find(coo)[1]
    #     nz_ridx = sparse.find(coo)[0]
    #
    #     # Get a bool array with elements T if position holds a nonzero
    #     nonzero_cidx = np.isin(np.arange(coo.shape[1]), nz_cidx)
    #     nonzero_ridx = np.isin(np.arange(coo.shape[0]), nz_ridx)
    #
    #     # WARNING: Indices of altered axes are reset in the returned matrix
    #     # TODO: Modify categories associated with indices
    #     # Find cols (category and index) with all zeros
    #     zero_col_idx =  []
    #     nonzero_col_labels = []
    #     for zidx in range(len(nonzero_cidx)):
    #         # If position does not contain a non-zero, mark index/category for deletion
    #         #   Directly address this 1d boolean numpy.ndarray
    #         if nonzero_cidx[zidx] == True:
    #             # Save labels with non-zero elements
    #             label = cls._get_category_from_code(zidx, col_categ)
    #             nonzero_col_labels.append(label)
    #         else:
    #             # Save indexes with all zero elements
    #             zero_col_idx.append(zidx)
    #     cmp_col_categ = CategoricalDtype(nonzero_col_labels, ordered=True)
    #
    #     # Find rows (category and index) with all zeros
    #     zero_row_idx =  []
    #     nonzero_row_labels = []
    #     for zidx in range(len(nonzero_ridx)):
    #         # If true (1) that this position contains a zero in the coo
    #         if nonzero_ridx[zidx] == True:
    #             # Save labels with non-zero elements
    #             label = cls._get_category_from_code(zidx, row_categ)
    #             nonzero_row_labels.append(label)
    #         else:
    #             # Save indexes with all zero elements
    #             zero_row_idx.append(zidx)
    #     cmp_row_categ = CategoricalDtype(nonzero_row_labels, ordered=True)
    #
    #     # Mask with indices to remove data
    #     csr = coo.tocsr()
    #     if len(zero_row_idx) > 0 and len(zero_col_idx) > 0:
    #         row_mask = np.ones(csr.shape[0], dtype=bool)
    #         row_mask[zero_row_idx] = False
    #         col_mask = np.ones(csr.shape[1], dtype=bool)
    #         col_mask[zero_col_idx] = False
    #         compressed_csr = csr[row_mask][:, col_mask]
    #
    #     elif len(zero_row_idx) > 0:
    #         mask = np.ones(csr.shape[0], dtype=bool)
    #         mask[zero_row_idx] = False
    #         compressed_csr = csr[mask]
    #
    #     elif len(zero_col_idx) > 0:
    #         mask = np.ones(csr.shape[1], dtype=bool)
    #         mask[zero_col_idx] = False
    #         compressed_csr = csr[:, mask]
    #
    #     else:
    #         compressed_csr = csr
    #
    #     cmp_coo = compressed_csr.tocoo()
    #     return cmp_coo, cmp_row_categ, cmp_col_categ

    # ...........................
    @property
    def pam(self):
        """Return binary sparse_array.

        Returns:
            (scipy.sparse.coo_array): Binary sparse_array (PAM) for the object.
        """
        return self.sparse_array

    # ...........................
    @property
    def num_species(self):
        """Return number of species in the array (on the x/1 axis).

        Returns:
            (int): Number of species (values on the x/1 axis)
        """
        return self._coo_array.shape[1]

    # ...........................
    @property
    def num_sites(self):
        """Return number of `sites` in the array (on the y/0 axis).

        Returns:
            (int): Number of `sites` (values on the y/0 axis)
        """
        return self._coo_array.shape[0]

    # ...........................
    def calc_diversity_stats(self):
        """Calculate diversity statistics.

        Returns:
            diversity_matrix (pandas.DataFrame): a matrix with 1 column for each
                statistic, and one row containing the values for each statistic.
        """
        diversity_stats = [
            # ('c-score', self.c_score),
            ('lande', self.lande),
            ('legendre', self.legendre),
            ('num sites', self.num_sites),
            ('num species', self.num_species),
            ('whittaker', self.whittaker),
            ]
        data = {}
        for name, func in diversity_stats:
            data[name] = func()
        diversity_matrix = pd.DataFrame(data=data, index=["value"])
        return diversity_matrix

    # ...........................
    def calc_site_stats(self):
        """Calculate site-based statistics.

        Returns:
            site_stats_matrix (pandas.DataFrame): a matrix with 1 column for each
                statistic, and one row for each site.
        """
        site_matrix_stats = [
            ('alpha', self.alpha),
            ('alpha proportional', self.alpha_proportional),
            ('phi', self.phi),
            ('phi average proportional', self.phi_average_proportional),
        ]
        site_index = self.row_category.categories
        data = {}
        for name, func in site_matrix_stats:
            data[name] = func()

        site_stats_matrix = pd.DataFrame(data=data, index=site_index)
        return site_stats_matrix

    # ...........................
    def calc_species_stats(self):
        """Calculate species-based statistics.

        Returns:
            site_stats_matrix (pandas.DataFrame): a matrix with 1 column for each
                statistic, and one row for each species.
        """
        species_matrix_stats = [
            ('omega', self.omega, None),
            ('omega_proportional', self.omega_proportional, 'omega'),
            ('psi', self.psi, None),
            ('psi_average_proportional', self.psi_average_proportional, 'psi'),
        ]
        species_index = self._col_categ.categories
        data = {}
        for name, func, input_name in species_matrix_stats:
            try:
                input_data = data[input_name]
            except:
                data[name] = func()
            else:
                data[name] = func(input_data)

        species_stats_matrix = pd.DataFrame(data, index=species_index)
        return species_stats_matrix

    # # ...........................
    # TODO: test matrices created in these stats (sparse or dense)
    # def calc_covariance_stats(self):
    #     """Calculate covariance statistics matrices.
    #
    #     Returns:
    #         list of tuple: A list of metric name, matrix tuples for covariance stats.
    #     """
    #     covariance_stats = [
    #         ('sigma sites', self.sigma_sites),
    #         ('sigma species', self.sigma_species)
    #     ]
    #     stats_matrices = []
    #     for name, func in covariance_stats:
    #         mtx, headers = func()
    #         mtx.set_headers(headers)
    #         stats_matrices.append((name, mtx))
    #     return stats_matrices

    # .............................................................................
    # Diversity metrics
    # .............................................................................
    # TODO: test the matrices created by sigma functions within these diversity stats
    def schluter_species_variance_ratio(self):
        """Calculate Schluter's species variance ratio.

        Returns:
            float: The Schluter species variance ratio for the PAM.
        """
        sigma_species_, _hdrs = self.sigma_species()
        return float(sigma_species_.sum() / sigma_species_.trace())

    # .............................................................................
    def schluter_site_variance_ratio(self):
        """Calculate Schluter's site variance ratio.
        Returns:
            float: The Schluter site variance ratio for the PAM.
        """
        sigma_sites_, _hdrs = self.sigma_sites()
        return float(sigma_sites_.sum() / sigma_sites_.trace())

    # .............................................................................
    def whittaker(self):
        """Calculate Whittaker's beta diversity metric for a PAM.

        Returns:
            float: Whittaker's beta diversity for the PAM.
        """
        omega_prop = self.omega_proportional()
        return float(self.num_species / omega_prop.sum())

    # .............................................................................
    def lande(self):
        """Calculate Lande's beta diversity metric for a PAM.

        Returns:
            float: Lande's beta diversity for the PAM.
        """
        omega_fl = self.omega().astype(float)
        return float(
            self.num_species - (omega_fl / self.num_sites).sum()
        )

    # .............................................................................
    def legendre(self):
        """Calculate Legendre's beta diversity metric for a PAM.

        Returns:
            float: Legendre's beta diversity for the PAM.
        """
        omega_ = self.omega
        return float(omega_.sum() - (float((omega_ ** 2).sum()) / self.num_sites))

    # # ...........................
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
    #             num_shared = len(np.where(np.sum(self.pam[:, [i, j]], axis=1) == 2)[0])
    #             p_1 = omega_[i] - num_shared
    #             p_2 = omega_[j] - num_shared
    #             temp += p_1 * p_2
    #     return 2 * temp / (num_species_ * (num_species_ - 1))

    # .............................................................................
    # Species metrics
    # .............................................................................
    def omega(self):
        """Calculate the range `size` per species.

        Returns:
            sp_range_size_vct (numpy.ndarray): 1D ndarray of range `sizes`
                (each site counts as 1), one element for each species (axis 1) of PAM.

        Note:
            function assumes all `sites` (analysis dimension) are equal size.
        """
        sp_range_size_vct = self._coo_array.sum(axis=0)
        return sp_range_size_vct

    # ...........................
    def omega_proportional(self, omega_vct):
        """Calculate the mean proportional range size of each species.

        Returns:
            Matrix: A row of the proportional range sizes for each species in the PAM.
        """
        if omega_vct is None:
            omega_vct = self.omega()
        return omega_vct.astype(float) / self.num_sites

    # .............................................................................
    def psi(self):
        """Calculate the range richness of each species.

        Returns:
            psi_vct (numpy.ndarray): 1D array of range richness for the sites that each species is present in.
        """
        pam = self._coo_array.todense(order='C')
        sp_range_richness_vct = self._coo_array.sum(axis=1).dot(pam)
        return sp_range_richness_vct

    # .............................................................................
    def psi_average_proportional(self, psi_vct):
        """Calculate the mean proportional species diversity.

        Args:
            psi_vct (numpy.ndarray):
        Returns:
            Matrix: A row of proportional range richness for the sites that each species
                the PAM is present.
        """
        if psi_vct is None:
            psi_vct =  self.psi()
        sp_range_size_vector = self.num_species * self.omega()
        psi_avg_prop = psi_vct.astype(float) / sp_range_size_vector
        return psi_avg_prop

    # .............................................................................
    # Site-based statistics
    # .............................................................................
    def alpha(self):
        """Calculate alpha diversity, the number of species in each site.

        Returns:
            sp_count_vct (numpy.ndarray): 1D ndarray of species count for each
                site in the PAM.
        """
        sp_count_vct = self._coo_array.sum(axis=1)
        return sp_count_vct

    # .............................................................................
    def alpha_proportional(self, alpha_vct):
        """Calculate proportional alpha diversity.

        Args:
            alpha_vct (numpy.ndarray): 1D array of species count per site.

        Returns:
            alpha_prop_vct (numpy.ndarray): 1D array, row, of proportional alpha
                diversity values for each site in the PAM.
        """
        if alpha_vct is None:
            alpha_vct = self.alpha()
        alpha_prop_vct = alpha_vct.astype(float) / self.num_species
        return alpha_prop_vct

    # # .............................................................................
    # def phi(self):
    #     """Calculate phi, the range size per site.
    #
    #     Returns:
    #         Matrix: A column of the sum of the range sizes for the species present at each
    #             site in the PAM.
    #     """
    #     omega_vct = self.omega()
    #     phi_mtx = self._coo_array.dot(omega_vct)
    #     return phi_mtx
    #
    # # .............................................................................
    # def phi_average_proportional(self, phi_vct):
    #     """Calculate proportional range size per site.
    #
    #     Returns:
    #         Matrix: A column of the proportional value of the sum of the range sizes for
    #             the species present at each site in the PAM.
    #     """
    #     if phi_vct is None:
    #         phi_vct  = self.phi()
    #     omega_fl = self.omega().astype(float)
    #     alpha_ = self.alpha()
    #     return self._coo_array.dot(omega_fl) / (self.num_sites * alpha_)

    # .............................................................................
    # Covariance metrics
    # .............................................................................
    # TODO: test the type of matrix returned by these sigma functions
    def sigma_sites(self):
        """Compute the site sigma metric for a PAM.

        Returns:
            Matrix: Matrix of covariance of composition of sites.
        """
        pam = self._coo_array.todense(order='C')
        site_by_site = pam.dot(pam.T).astype(float)
        alpha_prop = self.alpha_proportional()
        mtx = (site_by_site / self.num_species()) - np.outer(alpha_prop, alpha_prop)
        # Output is sites x sites, so use site headers for column headers too
        headers = {
            "0": deepcopy(self.row_category),
            "1": deepcopy(self.column_category)
        }
        return mtx, headers

    # .............................................................................
    def sigma_species(self):
        """Compute the species sigma metric for a PAM.

        Returns:
            Matrix: Matrix of covariance of composition of species.
        """
        pam = self._coo_array.todense(order='C')
        species_by_site = pam.T.dot(pam).astype(float)
        omega_prop = self.omega_proportional()
        mtx = (species_by_site / self.num_sites()) - np.outer(omega_prop, omega_prop)
        # Output is species x species, so use species headers for row headers too
        headers = {
            "0": deepcopy(self.row_category),
            "1": deepcopy(self.column_category)
        }
        return mtx, headers
