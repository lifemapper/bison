"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.log import Logger
from bison.common.util import count_lines
from bison.providers.riis_data import RIIS
from tests.tools.test_setup import get_test_parameters

script_name = os.path.splitext(os.path.basename(__file__))[0]


# .............................................................................
class Test_annotate_riis:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    # .....................................
    def test_read_small_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        riis = RIIS(fn_args["_test_small_riis_filename"], logger, is_annotated=False)
        # Read original species data
        riis.read_riis()
        assert len(riis.by_taxon) == fn_args["_test_small_riis_name_count"]
        assert len(riis.by_riis_id) == fn_args["_test_small_riis_record_count"]
        assert (
            count_lines(fn_args["_test_small_riis_filename"])
            == fn_args["_test_small_riis_record_count"] + 1)

    # .....................................
    def test_read_original_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        riis = RIIS(fn_args["riis_filename"], logger, is_annotated=False)
        # Read original species data
        riis.read_riis()
        assert len(riis.by_taxon) == fn_args["_test_riis_name_count"]
        assert len(riis.by_riis_id) == fn_args["_test_riis_record_count"]
        assert (
            count_lines(fn_args["riis_filename"])
            == fn_args["_test_riis_record_count"] + 1)

    # .....................................
    def test_resolve_small_riis(self):
        """Test resolving taxa in a small RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        riis = RIIS(fn_args["_test_small_riis_filename"], logger, is_annotated=False)
        # Update species data
        name_count, rec_count = riis.resolve_riis_to_gbif_taxa(
                fn_args["_test_small_annotated_riis_filename"], overwrite=True)
        assert name_count == fn_args["_test_small_riis_gbiftaxa_count"]
        assert rec_count == fn_args["_test_small_riis_record_count"]

    # .....................................
    def test_read_annotated_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        riis = RIIS(fn_args["_test_annotated_riis_filename"], logger, is_annotated=True)
        # Read original species data
        riis.read_riis()
        assert len(riis.by_taxon) == fn_args["_test_annotated_riis_name_count"]
        assert len(riis.by_riis_id) == fn_args["_test_annotated_riis_record_count"]
        assert (
            count_lines(fn_args["_test_annotated_riis_filename"])
            == fn_args["_test_riis_record_count"] + 1)
