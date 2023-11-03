"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.constants import PARAMETERS, REPORT
from bison.common.log import Logger
from bison.common.util import count_lines_with_cat
from bison.tools._config_parser import process_arguments_from_file
from bison.provider.riis_data import RIIS

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/config/test_process_gbif.json"
params = process_arguments_from_file(config_filename, PARAMETERS)
small_riis_filename = "/volumes/bison/tests/input/US-RIIS_MasterList_top100_2021.csv"

# .............................................................................
class Test_annotate_riis:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    def test_read_small_riis(self):
        """Test reading an original RIIS file by checking counts."""
        riis = RIIS(small_riis_filename, logger)
        # Read original species data
        riis.read_riis()
        line_count = count_lines_with_cat(small_riis_filename)
        print("Find counts: riis.by_taxon, by_riis_id, line_count")
        # assert (len(riis.by_taxon) == 0)
        # assert (len(riis.by_riis_id) == 0)
        assert (line_count == 101)

    # .....................................
    def test_resolve_small_riis(self):
        """Test resolving taxa in a small RIIS file by checking summary report."""
        riis = RIIS(small_riis_filename, self._logger)
        report = riis.resolve_riis_to_gbif_taxa(overwrite=True)
        assert(report[REPORT.SUMMARY])
        for key in (
                REPORT.RIIS_IDENTIFIER, REPORT.RIIS_TAXA, REPORT.RIIS_RESOLVE_FAIL,
                REPORT.TAXA_RESOLVED, REPORT.RECORDS_UPDATED, REPORT.RECORDS_OUTPUT):
            assert(key in report[REPORT.SUMMARY])
