"""Common classes for Specimen Occurrence record processing."""

import os

from bison.common.constants import GBIF, ENCODING
from bison.tools.util import get_csv_dict_reader, get_logger


# .............................................................................
class GBIFReader(object):
    """Class to read a GBIF simple CSV datafile.

    Note:
        To chunk the file into more easily managed small files (i.e. fewer
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

        # CVS DictReader, and current line
        self._gbif_reader = None

    # ...............................................
    def open(self):
        """Open a GBIF datafile with a csv.DictReader.

        Raises:
            Exception: on failure to open csvfile and get a csv.DictReader
        """
        try:
            self._gbif_reader, self._inf = get_csv_dict_reader(self.csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING)
        except Exception:
            raise

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        try:
            self._inf.close()
        except AttributeError:
            pass
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None

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
            lineno = self._gbif_reader.line_num
        except AttributeError:
            pass
        return lineno

#     # ...............................................
#     def find_gbif_record(self, gbifid):
#         """
#         @summary: Find a GBIF occurrence record identified by provided gbifID.
#         """
#         if (not self._gbif_reader or
#             not self._gbif_line):
#             raise Exception('Use open_gbif_for_search before searching')
#
#         rec = None
#         try:
#             while (not rec and self._gbif_line is not None):
#                 # Get interpreted record
#                 self._gbif_line, self.recno = getLine(self._gbif_reader,
#                                                             self._gbif_recno)
#
#                 if self._gbif_line is None:
#                     break
#                 else:
#                     if self._gbif_line[0] == gbifid:
#                         # Create new record or empty list
#                         rec = self._create_rough_bisonrec(self._gbif_line,
#                                                           self._gbif_column_map)
#                     # Where are we
#                     if (self.recno % LOGINTERVAL) == 0:
#                         self._log.info('*** Record number {} ***'.format(self.recno))
#             if (not rec and self._gbif_line is None):
#                 self._log.error('Failed to find {} in remaining records'.format(gbifid))
#                 self.close()
#         except Exception as e:
#             self._log.error('Failed on line {}, exception {}'.format(self.recno, e))
#         return rec
#
#     # ...............................................
#     def open_gbif_for_search(self, gbif_interp_fname):
#         """
#         @summary: Open a CSV file containing GBIF occurrence records extracted
#                      from the interpreted occurrence file provided
#                      from an Occurrence Download, in Darwin Core format.
#         """
#         if self.is_open():
#             self.close()
#         # Open raw GBIF data
#         self._gbif_reader, inf = get_csv_reader(gbif_interp_fname, GBIF_DELIMITER, ENCODING)
#         self._files.append(inf)
#         # Pull the header row
#         self._gbif_line, self.recno = getLine(self._gbif_reader, 0)


# ...............................................
if __name__ == '__main__':
    pass
