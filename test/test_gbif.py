"""Module to test the contents of the input GBIF csv occurrence data file."""
import os

from bison.common.constants import DATA_PATH, GBIF
from bison.common.occurrence import GBIFReader
from bison.tools.gbif_api import GbifSvc
from bison.tools.util import get_logger


class TestGBIFData(GBIFReader):
    """Class for testing downloaded simple GBIF CSV file."""

    # .............................................................................
    def __init__(self, datapath, csvfile, logger):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            datapath (str): base directory for datafiles
            csvfile (str): basename of file to read
            logger (object): logger for saving relevant processing messages
        """
        GBIFReader.__init__(self, datapath, csvfile, logger)
        self.open()

    # ...............................................
    def test_gbif_name_accepted(self):
        """Open a GBIF datafile with a csv.DictReader."""
        svc = GbifSvc()
        self.open()
        for rec in self._gbif_reader:
            if rec is None:
                break
            elif (self.recno % 100) == 0:
                self.logit('*** Record number {} ***'.format(self.recno))
            taxkey = rec[GBIF.TAXON_FLD]
            # API data
            taxdata = svc.query_for_name(taxkey=taxkey)
            taxstatus = taxdata[GBIF.STATUS_FLD]
            taxname = taxdata[GBIF.NAME_FLD]
            # Make sure simple CSV data taxonkey is accepted
            if taxstatus.lower() != "accepted":
                self.logit("Record {} taxon key {} is not an accepted name {}".format(
                    rec[GBIF.OCCID_FLD], taxkey, taxstatus))
            # Make sure simple CSV data sciname matches name for taxonkey
            if rec[GBIF.NAME_FLD] != taxname:
                self.logit("Record {} name {} does not match taxon key {} name {}".format(
                    rec[GBIF.OCCID_FLD], rec[GBIF.NAME_FLD], taxkey, taxname))


# .............................................................................
if __name__ == "__main__":
    logname = "test_gbif"
    csvfile = GBIF.TEST_DATA

    # Test the taxonkey contents in GBIF simple CSV download file
    logger = get_logger(DATA_PATH, logname=logname)

    Tst = TestGBIFData(DATA_PATH, csvfile, logger)
    Tst.test_gbif_name_accepted()

"""
from test.test_gbif import *

outpath = "/tmp"
logname = "test_gbif"
csvfile = GBIF.TEST_DATA
logger = get_logger(DATA_PATH, logname=logname)

Tst = TestGBIFData(DATA_PATH, csvfile, logger)
Tst.test_gbif_name_accepted()
"""
