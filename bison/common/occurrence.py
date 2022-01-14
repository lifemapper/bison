"""Common classes for Specimen Occurrence record processing."""

import os

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
    def __init__(self, workpath, logger):
        """Construct an object to read a GBIF datafile.

        Args:
            workpath(str): base directory for datafiles
            logger (object): logger for saving relevant processing messages
        """
        self._log = logger
        # Remove any trailing /
        workpath.rstrip(os.sep)
        self._files = []
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0
        self._gbif_line = None

    # ...............................................
    def is_open(self):
        """Return true if any files are open."""
        for f in self._files:
            if not f is None and not f.closed:
                return True
        return False

    # ...............................................
    def close(self):
        '''Close input datafiles and output file.'''
        for f in self._files:
            try:
                f.close()
            except Exception:
                pass
        # Used only for reading from open gbif file to test bison transform
        self._gbif_reader = None
        self._gbif_recno = 0
        self._gbif_line = None

    # ...............................................
    def _test_for_discard(self, brec):
        """Remove record without name fields or with absence status.

        Args:
            brec (dict): current record
        """
        if brec is not None:
            gid = brec[OCC_ID_FLD]
            # Required fields exist
            if (not brec['provided_scientific_name'] and not brec['taxonKey']):
                brec = None
                self._log.info('Discard brec {}: missing both sciname and taxkey'
                               .format(gid))
        if brec is not None:
            # remove records with occurrenceStatus = absence
            ostat = brec['occurrenceStatus']
            if ostat and ostat.lower() == 'absent':
                brec = None

               self._log.info('Discard brec {}: with occurrenceStatus absent'
                               .format(gid))
        return brec

    # ...............................................
    def _remove_internal_delimiters(self, rec):
        for fld, val in rec.items():
            if isinstance(val, str):
                if val.find(BISON_DELIMITER) >= 0:
                    print ('Delimiter in val {}'.format(val))
                    rec[fld] = val.replace(BISON_DELIMITER, '')

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
#                 self._gbif_line, self._gbif_recno = getLine(self._gbif_reader,
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
#                     if (self._gbif_self.recno % LOGINTERVAL) == 0:
#                         self._log.info('*** Record number {} ***'.format(self._gbif_recno))
#             if (not rec and self._gbif_line is None):
#                 self._log.error('Failed to find {} in remaining records'.format(gbifid))
#                 self.close()
#         except Exception as e:
#             self._log.error('Failed on line {}, exception {}'.format(self._gbif_recno, e))
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
#         self._gbif_line, self._gbif_recno = getLine(self._gbif_reader, 0)

# ...............................................
if __name__ == '__main__':
    pass
