"""Matrix to summarize each of 2 dimensions of data by counts of the other and a third."""
# from collections import OrderedDict
# import pandas as pd
#
# from bison.common.constants import (
#     COUNT_FLD, CSV_DELIMITER, SNKeys, TOTAL_FLD, SUMMARY
# )
from bison.spnet.aggregate_data_matrix import _AggregateDataMatrix


# .............................................................................
class HeatmapMatrix(_AggregateDataMatrix):
    """Class for holding individual counts of data for a grid of regions."""

    # ...........................
    def __init__(
            self, summary_df, table_type, datestr, logger=None):
        """Constructor for occurrence/species counts by region/analysis_dim comparisons.

        Args:
            summary_df (pandas.DataFrame): DataFrame with a row for each element in
                category, and 2 columns of data.  Rows headers and column headers are
                labeled.
                * Column 1 contains the count of the number of columns in  that row
                * Column 2 contains the total of values in that row.
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data
            datestr (str): date of the source data in YYYY_MM_DD format.
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
        _AggregateDataMatrix.__init__(self, table_type, datestr, logger=logger)
