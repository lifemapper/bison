"""Module to test appending RIIS determinations to GBIF occurrences."""
from bison.common.annotate import Annotator
from bison.common.constants import GBIF, DATA_PATH
from bison.common.riis import NNSL
from bison.tools.util import get_logger


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
        self.open()


    # .............................................................................
    def test_gbif_name_accepted(self, nnsl_data):

# .............................................................................
if __name__ == "__main__":
    # Test the taxonkey contents in GBIF simple CSV download file
    logger = get_logger(DATA_PATH, logname="test_annotate")
    nnsl_data = NNSL(DATA_PATH, logger=logger)

    tst = TestAnnotator(DATA_PATH, GBIF.TEST_DATA, do_resolve=False, logger=logger)
    tst.test_gbif_name_accepted()

"""
from test.test_annotate import *

outpath = "/tmp"
logname = "test_annotate"
csvfile = GBIF.TEST_DATA
logger = get_logger(DATA_PATH, logname=logname)

"""
