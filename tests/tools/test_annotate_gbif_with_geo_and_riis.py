"""Tests for the annotate_riis tool and dependent objects."""
import os
from rtree.index import Index as rtree_index

from bison.common.constants import APPEND_TO_DWC, LMBISON_PROCESS
from bison.common.log import Logger
from bison.common.util import BisonNameOp, get_fields_from_header
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
        spatial_idxs = ant._geo_fulls
        spatial_idxs.extend(ant._geo_partials.values())

        for geo_spidx in spatial_idxs:
            assert(isinstance(geo_spidx.spatial_index, rtree_index))

    # .....................................
    def test_annotate_occs(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        logger = Logger(script_name)
        ant = Annotator(
            fn_args["geoinput_path"], logger,
            riis_with_gbif_filename=fn_args["riis_with_gbif_taxa_filename"])
        infile = fn_args["dwc_filenames"][0]
        infields = get_fields_from_header(infile)
        new_fields = APPEND_TO_DWC.annotation_fields()

        for fn in fn_args["dwc_filenames"]:
            outfile = BisonNameOp.get_out_process_filename(
                fn, outpath=fn_args["output_path"],
                step_or_process=LMBISON_PROCESS.ANNOTATE)
            ant.annotate_dwca_records(fn, outfile)
            outfields = get_fields_from_header(outfile)
            assert(len(infields) + len(new_fields) == len(outfields))
            for fld in APPEND_TO_DWC.annotation_fields():
                assert(fld in outfields)

    # .....................................
    def test_annotate_dwca_records(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        infile = fn_args["dwc_filenames"][0]
        infields = get_fields_from_header(infile)
        new_fields = APPEND_TO_DWC.annotation_fields()

        report = annotate_occurrence_file(
            infile, fn_args["riis_with_gbif_taxa_filename"],
            fn_args["geoinput_path"], fn_args["output_path"])

        outfile = report["dwc_with_geo_and_riis_filename"]
        outfields = get_fields_from_header(outfile)
        assert (len(infields) + len(new_fields) == len(outfields))
        for fld in APPEND_TO_DWC.annotation_fields():
            assert (fld in outfields)
