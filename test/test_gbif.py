"""Module to test the contents of the input GBIF csv occurrence data file."""

from bison.common.constants import GBIF
from bison.common.occurrence import GBIFReader


class TestGBIFData(GBIFReader):
    """Class for testing downloaded simple GBIF CSV file."""

    # .............................................................................
    def __init__(self, base_path):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            base_path (str): base file path for project execution
        """
        GBIFReader.__init__(self, base_path)
        self.open()

    # ...............................................
    def test_gbif_read(self, csvfile):
        """Open a GBIF datafile with a csv.DictReader.

        Args:
            csvfile(str): basename of file to read
        """
        self.open(csvfile)
