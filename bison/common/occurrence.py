"""Common classes for Specimen Occurrence record processing."""

import os

from bison.common.constants import (
    ENCODING, GBIF, LOG, NEW_RIIS_ASSESSMENT_FLD, NEW_RIIS_KEY_FLD)
from bison.tools.util import get_csv_dict_reader, get_logger, logit


# .............................................................................
class DwcData(object):
    """Class to read or write a GBIF DWC format CSV datafile.

    Note:
        To chunk the input file into more easily managed small files (i.e. fewer
        GBIF API queries), split using sed command output to file like:
        sed -e '1,5000d;10000q' occurrence.txt > occurrence_lines_5000-10000.csv
        where 1-5000 are lines to delete, and 10000 is the line on which to stop.
    """
    # ...............................................
    def __init__(self, datapath, csvfile, logger=None):
        """Construct an object to read a GBIF datafile.

        Args:
            datapath(str): base directory for datafiles
            csvfile(str): basename of file to read
            logger (object): logger for saving relevant processing messages
        """
        # Remove any trailing /
        self._datapath = datapath.rstrip(os.sep)
        self.csvfile = os.path.join(datapath, csvfile)
        if logger is None:
            logger = get_logger(datapath)
        self._log = logger

        # Open file
        self._inf = None

        # CVS DictReader and current record
        self._csv_reader = None
        self.dwcrec = None

    # ...............................................
    def open(self):
        """Open a GBIF datafile with a csv.DictReader.

        Raises:
            Exception: on failure to open csvfile and get a csv.DictReader
        """
        try:
            self._csv_reader, self._inf = get_csv_dict_reader(
                self.csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING)
        except Exception:
            raise
        else:
            # Fill self.dwcrec
            self._dwcdata.get_record()

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        try:
            self._inf.close()
        except AttributeError:
            pass
        # Used only for reading from open gbif file to test bison transformreader
        self._csv_reader = None

    # ...............................................
    @property
    def is_open(self):
        """Return true if any files are open.

        Returns:
            :type bool, True if CSV file is open, False if CSV file is closed
        """
        if self._inf is not None and not self._inf.closed:
            return True
        return False

    # ...............................................
    @property
    def recno(self):
        """Return the line number most recently read by the CSV reader.

        Returns:
            most recently read line number of the CSV reader.  If the CSV reader does not exist, return None.
        """
        lineno = -1
        try:
            lineno = self._csv_reader.line_num
        except AttributeError:
            pass
        return lineno

    # ...............................................
    def get_record(self):
        """Return next record from the reader."""
        self.dwcrec = next(self._csv_reader)

    # ...............................................
    def find_gbif_record(self, gbifid):
        """Find a GBIF occurrence record identified by provided gbifID.

        Args:
            gbifid: local GBIF identifier for finding a record in a large file.

        Returns:
            self.dwcrec: a dictionary containing GBIF record
        """
        if self._csv_reader is None:
            self.open()
        found = False
        try:
            while (self.dwcrec is not None and found is False):
                # Get interpreted record
                self.get_record()
                if self.dwcrec[GBIF.ID_FLD] == gbifid:
                    found = True

                # Where are we
                if (self.recno % LOG.INTERVAL) == 0:
                    logit(self._log, '*** Record number {} ***'.format(self.recno))
            if (self.dwcrec is None and found is False):
                logit(self._log, 'Failed to find {}'.format(gbifid))
                self.close()
        except Exception as e:
            logit(self._log, 'Failed on line {}, exception {}'.format(self.recno, e))
        return self.dwcrec


# ...............................................
if __name__ == '__main__':
    pass
