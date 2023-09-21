"""Tests for the annotate_riis tool and dependent objects."""
import os
from rtree.index import Index as rtree_index

from bison.common.constants import APPEND_TO_DWC, REPORT
from bison.common.log import Logger
from bison.common.util import get_fields_from_header
from bison.process.annotate import Annotator
from tests.tools.test_setup import get_test_parameters

script_name = os.path.splitext(os.path.basename(__file__))[0]


# .............................................................................
class Test_annotate_gbif:
    """Test the CLI tool and dependencies that annotate RIIS data with GBIF taxa."""
    def __init__(self):
        """Constructor."""
        self._logger = Logger(script_name)

    # .....................................
    def test_ant_init(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        ant = Annotator(
            self._logger, fn_args["geoinput_path"],
            annotated_riis_filename=fn_args["riis_with_gbif_taxa_filename"])
        spatial_idxs = ant._geo_fulls
        spatial_idxs.extend(ant._pad_states.values())

        for geo_spidx in spatial_idxs:
            assert(isinstance(geo_spidx.spatial_index, rtree_index))

    # .....................................
    def test_annotate_occs(self):
        """Test reading an original RIIS file by checking counts."""
        fn_args = get_test_parameters(script_name)
        ant = Annotator(
            self._logger, fn_args["geoinput_path"],
            annotated_riis_filename=fn_args["riis_with_gbif_taxa_filename"])
        infile = fn_args["dwc_filenames"][0]
        infields = get_fields_from_header(infile)
        new_fields = APPEND_TO_DWC.annotation_fields()

        for fn in fn_args["dwc_filenames"]:
            rpt = ant.annotate_dwca_records(fn, fn_args["output_path"])
            outfields = get_fields_from_header(rpt[REPORT.OUTFILE])
            assert(len(infields) + len(new_fields) == len(outfields))
            for fld in APPEND_TO_DWC.annotation_fields():
                assert(fld in outfields)

    # # .....................................
    # def test_annotate_dwca_records(self):
    #     """Test annotating one occurrence file and checking for added fields."""
    #     fn_args = get_test_parameters(script_name)
    #     infile = fn_args["dwc_filenames"][0]
    #     infields = get_fields_from_header(infile)
    #     new_fields = APPEND_TO_DWC.annotation_fields()
    #
    #     report = annotate_occurrence_file(
    #         infile, fn_args["annotated_riis_filename"], fn_args["geoinput_path"],
    #         fn_args["output_path"], fn_args["log_path"])
    #
    #     outfields = get_fields_from_header(report[REPORT.OUTFILE])
    #     assert (len(infields) + len(new_fields) == len(outfields))
    #     for fld in APPEND_TO_DWC.annotation_fields():
    #         assert (fld in outfields)
