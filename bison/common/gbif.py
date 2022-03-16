"""Common classes for Specimen Occurrence record processing."""

import os

from bison.common.constants import (DATA_PATH, ENCODING, GBIF, LOG)
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
    def __init__(self, occ_filename, logger=None):
        """Construct an object to read a GBIF datafile.

        Args:
            occ_filename(str): full path of CSV occurrence file to read
            logger (object): logger for saving relevant processing messages
        """
        datapath, _ = os.path.split(occ_filename)
        self._datapath = datapath
        self._csvfile = occ_filename
        if logger is None:
            logger = get_logger(datapath)
        self._log = logger

        # Open file
        self._inf = None

        # CVS DictReader and current record
        self._csv_reader = None
        self.dwcrec = None

    # ...............................................
    @property
    def input_file(self):
        """Public property for input file.

        Returns:
            self._csvfile: input file containing DwC records.
        """
        return self._csvfile

    # ...............................................
    def open(self):
        """Open a GBIF datafile with a csv.DictReader.

        Raises:
            Exception: on failure to open csvfile and get a csv.DictReader
        """
        try:
            self._csv_reader, self._inf = get_csv_dict_reader(self._csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING)
        except Exception:
            raise

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
    @property
    def fieldnames(self):
        """Return the fieldnames read by the CSV reader.

        Returns:
            fieldnames of the CSV reader.  If the CSV reader does not exist, return None.
        """
        try:
            fieldnames = self._csv_reader.fieldnames
        except AttributeError:
            fieldnames = None
        return fieldnames

    # ...............................................
    def get_record(self):
        """Get next record from the reader.

        Returns:
            self.dwcrec: next record from the CSV reader
        """
        try:
            self.dwcrec = next(self._csv_reader)
        except StopIteration:
            self.dwcrec = None
        return self.dwcrec

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


# # ...............................................
# def compare_rec(good_rec, bad_rec):
#     """Compare 2 records for differences.
#
#     Args:
#         good_rec (dict): a record containing expected keys
#         bad_rec (dict): a bad record
#
#     """
#     good_fns = set(good_rec.keys())
#     bad_fns = set(bad_rec.keys())
#     extra_fns = bad_fns.difference(good_fns)
#
#     for k, v in good_rec.items():
#         print("$$$$ {}:  {}".format(k, v))
#         print("       vs {}".format(bad_rec[k]))
#     for extra in extra_fns:
#         print("!!!! {}:  {}".format(extra, bad_rec[extra]))


# ...............................................
if __name__ == '__main__':
    # Bad record plus before and after
    before = "1698052989"
    bad = "1698055779"
    after = "1698058398"

    logname = "test_gbif"
    # csvfile = GBIF.TEST_DATA
    csvfile = os.path.join(DATA_PATH, "gbif_2022-02-15_testbad_10.csv")
    logger = get_logger(DATA_PATH, logname=logname)

    dwcdata = DwcData(csvfile, logger)
    dwcdata.open()

    finished = False
    rec = dwcdata.get_record()
    while rec is not None and not finished:
        gbif_id = rec["gbifID"]
        if gbif_id == before:
            before_rec = rec
        elif gbif_id == bad:
            bad_rec = rec
        elif gbif_id == after:
            after_rec = rec
            finished = True
        rec = dwcdata.get_record()

    print("fini")
"""

from bison.common.constants import DATA_PATH
from bison.common.occurrence import *

# Bad record plus before and after
before = "1698052989"
bad = "1698055779"
after = "1698058398"

logname = "test_gbif"
# csvfile = GBIF.TEST_DATA
csvfile = os.path.join(DATA_PATH, "gbif_2022-02-15_testbad_10.csv")
logger = get_logger(DATA_PATH, logname=logname)

dwcdata = DwcData(csvfile, logger)
dwcdata.open()

finished = False
rec = dwcdata.get_record()

while rec is not None and not finished:
    gbif_id = rec["gbifID"]
    if gbif_id == before:
        before_rec = rec
    elif gbif_id == bad:
        bad_rec = rec
    elif gbif_id == after:
        after_rec = rec
        finished = True
    rec = dwcdata.get_record()



Tst.test_gbif_name_accepted()
"""
