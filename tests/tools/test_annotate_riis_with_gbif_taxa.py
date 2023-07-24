"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.constants import REPORT
from bison.common.log import Logger
from bison.common.util import count_lines_with_cat
from bison.provider.riis_data import RIIS
from tests.tools.test_setup import get_test_parameters

script_name = os.path.splitext(os.path.basename(__file__))[0]


# .............................................................................
class Test_annotate_riis:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    def __init__(self):
        """Constructor."""
        self._logger = Logger(script_name)

    # .....................................
    def test_read_small_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        riis = RIIS(fn_args["_test_small_riis_filename"], self._logger)
        # Read original species data
        riis.read_riis()
        actual_line_count = count_lines_with_cat(fn_args["_test_small_riis_filename"])
        assert len(riis.by_taxon) == fn_args["_test_small_riis_name_count"]
        assert len(riis.by_riis_id) == fn_args["_test_small_riis_record_count"]
        assert (actual_line_count == fn_args["_test_small_riis_record_count"] + 1)

    # .....................................
    def test_read_original_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        riis = RIIS(fn_args["riis_filename"], self._logger)
        # Read original species data
        riis.read_riis()
        actual_record_count = fn_args["_test_riis_record_count"]
        actual_line_count = count_lines_with_cat(fn_args["riis_filename"])
        assert len(riis.by_taxon) == fn_args["_test_riis_name_count"]
        # TODO: Why does riis.by_riis_id contain same number of records as lines in the file?
        assert len(riis.by_riis_id) == actual_record_count
        assert (actual_line_count == actual_record_count + 1)

    # .....................................
    def test_resolve_small_riis(self):
        """Test resolving taxa in a small RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        riis = RIIS(fn_args["_test_small_riis_filename"], self._logger)
        # Update species data
        report = riis.resolve_riis_to_gbif_taxa(overwrite=True)
        assert report[REPORT.TAXA_RESOLVED] == fn_args["_test_small_riis_name_count"]
        assert report[REPORT.RECORDS_UPDATED] == fn_args["_test_small_riis_record_count"]
