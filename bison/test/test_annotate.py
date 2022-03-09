"""Module to test appending RIIS determinations to GBIF occurrences."""
import os

from bison.common.annotate import Annotator
from bison.common.constants import GBIF, DATA_PATH
from bison.common.riis import NNSL
from bison.tools.util import get_logger, chunk_files


class TestAnnotator(Annotator):
    """Class for testing downloaded simple GBIF CSV file."""

    # .............................................................................
    def __init__(self, datapath, gbif_occ_fname, do_resolve=False, logger=None):
        """Constructor reads RIIS species file and opens a CSVReader to the GBIF data.

        Args:
            datapath (str): base directory for datafiles
            gbif_occ_fname (str): base filename for GBIF occurrence CSV file
            do_resolve (bool): flag indicating whether to query GBIF for updated accepted name/key
            logger (object): logger for saving relevant processing messages
        """
        Annotator.__init__(
            self, datapath, gbif_occ_fname, do_resolve=do_resolve, logger=logger)

    # .............................................................................
    def test_annotate_records(self):
        """Test annotating records and returning summarized errors in state field."""
        print(f"Appending records to: {self._csvfile}")
        self.append_dwca_records()
        print(f"   Matched states: {self.matched_states}")
        print(f"   Mis-matched states: {self.mismatched_states}")
        print(f"   Missing states: {self.missing_states}")
        print("   Good states: ")
        for st, counties in self.good_locations.items():
            print(f"  {st}: {counties}")
        print("   Bad states: ")
        for st, counties in self.bad_locations.items():
            print(f"  {st}: {counties}")


# .............................................................................
if __name__ == "__main__":
    # Test the taxonkey contents in GBIF simple CSV download file
    logger = get_logger(DATA_PATH, logname="test_annotate")
    nnsl_data = NNSL(DATA_PATH, logger=logger)
    big_gbif_fname = os.path.join(DATA_PATH, GBIF.TEST_DATA)

    chunk_fnames = chunk_files(big_gbif_fname)
    for fname in chunk_fnames:
        tst = TestAnnotator(fname, do_resolve=False, logger=logger)
        tst.test_annotate_records()

"""
from test.test_annotate import *

outpath = "/tmp"
logname = "test_annotate"
csvfile = GBIF.TEST_DATA
logger = get_logger(DATA_PATH, logname=logname)

"""
