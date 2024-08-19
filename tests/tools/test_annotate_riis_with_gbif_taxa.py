"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.util import BisonNameOp
from obsolete.src.common.constants2 import PARAMETERS, REPORT
from bison.common.log import Logger
from obsolete.src.common.util import count_lines_with_cat
from obsolete.src.tools._config_parser import process_arguments_from_file
from bison.provider.riis_data import RIIS

logger = Logger(os.path.splitext(os.path.basename(__file__))[0])
config_filename = "/volumes/bison/tests/config/test_process_gbif.json"
params = process_arguments_from_file(config_filename, PARAMETERS)


# .............................................................................
class Test_annotate_riis:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    def test_read_small_riis(self):
        """Test reading an original RIIS file by checking counts."""
        tst_riis_filename = params["_test_riis_filename"]
        riis = RIIS(tst_riis_filename, logger)
        # Read original species data
        riis.read_riis()
        line_count = count_lines_with_cat(tst_riis_filename)
        print("Find counts: riis.by_taxon, by_riis_id, line_count")
        # assert (len(riis.by_taxon) == 0)
        # assert (len(riis.by_riis_id) == 0)
        assert (line_count == 101)

    # .....................................
    def test_resolve_small_riis(self):
        """Test resolving taxa in a small RIIS file by checking summary report."""
        tst_riis_filename = params["_test_riis_filename"]
        riis = RIIS(tst_riis_filename, logger)
        annotated_filename = BisonNameOp.get_annotated_riis_filename(tst_riis_filename)

        report = riis.resolve_riis_to_gbif_taxa(annotated_filename, overwrite=True)
        assert(report[REPORT.SUMMARY])
        for key in (
                REPORT.RIIS_IDENTIFIER, REPORT.RIIS_TAXA, REPORT.RIIS_RESOLVE_FAIL,
                REPORT.TAXA_RESOLVED, REPORT.RECORDS_UPDATED, REPORT.RECORDS_OUTPUT):
            assert(key in report[REPORT.SUMMARY])
