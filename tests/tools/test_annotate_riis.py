"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.util import count_lines
from bison.providers.riis_data import NNSL
from tests.tools.test_setup import get_parameters

script_name = os.path.splitext(os.path.basename(__file__))[0]


# .............................................................................
class Test_annotate_riis:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    # .....................................
    def test_read_small_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_parameters(script_name)
        nnsl = NNSL(fn_args["small_riis_filename"], is_annotated=False, logger=None)
        # Read original species data
        nnsl.read_riis()
        assert len(nnsl.by_taxon) == fn_args["test_riis_name_count"]
        assert len(nnsl.by_riis_id) == fn_args["test_riis_record_count"]
        assert (
            count_lines(fn_args["riis_filename"])
            == fn_args["test_riis_record_count"] + 1)

    # .....................................
    def test_read_full_riis(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_parameters(script_name)
        nnsl = NNSL(fn_args["riis_filename"], is_annotated=False, logger=None)
        # Read original species data
        nnsl.read_riis()
        assert len(nnsl.by_taxon) == fn_args["test_riis_name_count"]
        assert len(nnsl.by_riis_id) == fn_args["test_riis_record_count"]
        assert (
            count_lines(fn_args["riis_filename"])
            == fn_args["test_riis_record_count"] + 1)

    # .....................................
    def test_resolve_riis(self):
        """Test resolving taxa in a small RIIS file by checking counts."""
        fn_args = get_parameters(script_name)
        nnsl = NNSL(fn_args["riis_filename"], is_annotated=False, logger=None)
        # Update species data
        name_count, rec_count = nnsl.resolve_riis_to_gbif_taxa(
                fn_args["annotated_riis_filename"], overwrite=True)
        assert name_count == fn_args["test_riis_name_count"]
        assert rec_count == fn_args["test_riis_record_count"]
