"""Matrix of sites as rows, species as columns, values are presence or absence (1/0)."""
from copy import deepcopy
from logging import ERROR
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import random
import scipy.sparse

from bison.common.constants import ANALYSIS_DIM, SNKeys, TMP_PATH
from bison.spnet.sparse_matrix import SparseMatrix


# .............................................................................
class PAM(SparseMatrix):
    """Class for managing computations for counts of aggregator0 x aggregator1."""

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
    pam = None
