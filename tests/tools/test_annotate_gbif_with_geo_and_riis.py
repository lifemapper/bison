"""Tests for the annotate_riis tool and dependent objects."""
import os

from bison.common.log import Logger
from bison.common.util import count_lines, BisonNameOp
from bison.process.annotate import Annotator, annotate_occurrence_file
from tests.tools.test_setup import get_test_parameters

script_name = os.path.splitext(os.path.basename(__file__))[0]


# .............................................................................
class Test_annotate_gbif:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""

    # .....................................
    def test_ant_init(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        ant = Annotator(
            fn_args["geoinput_path"], logger,
            riis_with_gbif_filename=fn_args["riis_with_gbif_taxa_filename"])
        spatial_idxs = [ant._geo_county, ant._geo_aianhh, ant._geo_doi]
        spatial_idxs.extend(ant._geo_pads)
        for geo_spidx in spatial_idxs:
            assert(geo_spidx.spatial_index is not None)

        assert (
            count_lines(fn_args["_test_small_riis_filename"])
            == fn_args["_test_small_riis_record_count"] + 1)

    # .....................................
    def test_annotate_occs(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        ant = Annotator(
            fn_args["geoinput_path"], logger,
            riis_with_gbif_filename=fn_args["riis_with_gbif_taxa_filename"])
        for fn in fn_args["dwc_filenames"]:
            out_fname = BisonNameOp.get_out_filename(fn, outpath=fn_args["output_path"])
            ant.annotate_dwca_records(fn, out_fname)

    # .....................................
    def test_annotate_dwca_records(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        infile = fn_args["dwc_filenames"][0]
        annotate_occurrence_file(
            infile, fn_args["riis_with_gbif_taxa_filename"], fn_args["geoinput_path"],
            fn_args["output_path"])
