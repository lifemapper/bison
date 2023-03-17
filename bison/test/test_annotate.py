"""Module to test appending RIIS determinations to GBIF occurrences."""
import os

from bison.process.annotate import Annotator
from bison.common.constants import BIG_DATA_PATH, DATA_PATH, GBIF, LOG
from bison.provider.riis_data import RIIS
from bison.common.log import Logger
from bison.common.util import Chunker

class TestAnnotator(Annotator):
    """Class for testing downloaded simple GBIF CSV file."""

    # .............................................................................
    def __init__(self, gbif_occ_filename, nnsl=None, logger=None):
        """Constructor reads RIIS species file and opens a CSVReader to the GBIF data.

        Args:
            gbif_occ_filename (str): full path of CSV occurrence file to annotate
            nnsl (bison.common.riis.NNSL): object containing USGS RIIS data for annotating records
            logger (object): logger for saving relevant processing messages
        """
        Annotator.__init__(self, gbif_occ_filename, nnsl=nnsl, logger=logger)

    # .............................................................................
    def test_annotate_records(self):
        """Test annotating records and returning summarized errors in state field."""
        print(f"Appending records to: {self._csvfile}")
        self.annotate_dwca_records()
        # print(f"   Matched states: {self.matched_states}")
        # print(f"   Mis-matched states: {self.mismatched_states}")
        # print(f"   Missing states: {self.missing_states}")
        # print("   Good states: ")
        for st, counties in self.good_locations.items():
            print(f"  {st}: {counties}")
        # print("   Bad states: ")
        # for st, counties in self.bad_locations.items():
        #     print(f"  {st}: {counties}")


# .............................................................................
if __name__ == "__main__":
    # Test the taxonkey contents in GBIF simple CSV download file
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    # config, logger, report_filename = get_common_arguments(
    #     script_name, DESCRIPTION, PARAMETERS)
    #
    # big_gbif_fname = os.path.join(BIG_DATA_PATH, GBIF.TEST_DATA)
    # logger = Logger(script_name, os.path.join(BIG_DATA_PATH, LOG.DIR), logname=)
    # nnsl_data = RIIS(DATA_PATH, logger=logger)
    #
    # chunk_fnames = Chunker.chunk_files(big_gbif_fname)
    # for fname in chunk_fnames:
    #     tst = TestAnnotator(fname, do_resolve=False, logger=logger)
    #     tst.test_annotate_records()
