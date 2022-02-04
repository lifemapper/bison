"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    ENCODING, GBIF, LOG, NEW_RIIS_ASSESSMENT_FLD, NEW_RIIS_KEY_FLD, RIIS_SPECIES)
from bison.common.riis import NNSL

from bison.tools.util import (
    get_csv_dict_reader, get_csv_dict_writer, get_logger, logit)


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(self, datapath, gbif_occ_fname, do_resolve=False, logger=None):
        """Constructor.

        Args:
            datapath (str): base directory for datafiles
            gbif_occ_fname (str): base filename for GBIF occurrence CSV file
            do_resolve (bool): flag indicating whether to query GBIF for updated accepted name/key
            logger (object): logger for saving relevant processing messages
        """
        self._datapath = datapath
        self._csvfile = os.path.join(datapath, gbif_occ_fname)

        if logger is None:
            logger = get_logger(datapath)
        self._log = logger

        self.nnsl = NNSL(datapath, logger=logger)
        if do_resolve is True:
            self.resolve_write_gbif_taxa()
        else:
            self.nnsl.read_species()
        # Input reader
        # self._dwc_occ = DwcOccurrence(datapath, occ_infname, logger=logger)
        self._csv_reader, self._inf = get_csv_dict_reader(
            self._csvfile, GBIF.DWCA_DELIMITER, encoding=ENCODING)
        # Output writer
        self._csv_writer = None

    # ...............................................
    @property
    def annotated_dwc_fname(self):
        """Construct a filename for the annotated GBIF DWC file from the original.

        Returns:
            outfname: output filename derived from the input GBIF DWC filename
        """
        basename, ext = os.path.splitext(self._csvfile)
        outfname = "{}_annotated{}".format(basename, ext)
        return outfname

    # ...............................................
    def _open_for_write(self, outfname):
        header = self._csv_reader.fieldnames
        header.append(NEW_RIIS_KEY_FLD)
        header.append(NEW_RIIS_ASSESSMENT_FLD)
        try:
            self._csv_writer, self._outf = get_csv_dict_writer(
                outfname, header, GBIF.DWCA_DELIMITER, fmode="w", encoding=ENCODING,
                overwrite=True)
        except Exception:
            raise

    # ...............................................
    def close(self):
        """Close input datafiles and output file."""
        try:
            self._inf.close()
            self._csv_reader = None
        except AttributeError:
            pass
        try:
            self._outf.close()
            self._csv_writer = None
        except AttributeError:
            pass

    # ...............................................
    @property
    def is_open(self):
        """Return true if any files are open.

        Returns:
            :type bool, True if CSV file is open, False if CSV file is closed
        """
        if ((self._inf is not None and not self._inf.closed)
                or (self._outf is not None and not self._outf.closed)):
            return True
        return False

    # ...............................................
    def _assess_occurrence(self, taxkey, dwcrec, iis_reclist):
        occ_state = dwcrec[GBIF.STATE_FLD]
        occ_acc_taxa = dwcrec[GBIF.ACC_TAXON_FLD]
        for iisrec in iis_reclist:
            # Double check NNSL dict key == RIIS resolved key == occurrence accepted key
            if taxkey != occ_acc_taxa or taxkey != iisrec[RIIS_SPECIES.NEW_GBIF_KEY]:
                logit(self._log, "WTF is happening?!?")
            # Look for AK or HI
            if occ_state in ("AK", "HI"):
                if occ_state == iisrec[RIIS_SPECIES.LOCALITY_FLD]:
                    dwcrec[NEW_RIIS_ASSESSMENT_FLD] = iisrec[RIIS_SPECIES.ASSESSMENT_FLD]
                    dwcrec[NEW_RIIS_KEY_FLD] = iisrec[RIIS_SPECIES.KEY]
            # Not AK or HI, must be L48
            elif iisrec[RIIS_SPECIES.LOCALITY_FLD] == "L48":
                dwcrec[NEW_RIIS_ASSESSMENT_FLD] = iisrec[RIIS_SPECIES.ASSESSMENT_FLD]
                dwcrec[NEW_RIIS_KEY_FLD] = iisrec[RIIS_SPECIES.KEY]
        return dwcrec

    # ...............................................
    def append_dwca_records(self):
        """Append 'introduced' or 'invasive' status to GBIF DWC occurrence records."""
        self._open_for_write(self.annotated_dwc_fname)
        for dwcrec in self._csv_reader:
            if dwcrec is None:
                break
            elif (self._csv_reader.recno % LOG.INTERVAL) == 0:
                logit(self._log, '*** Record number {} ***'.format(self._csv_reader.recno))

            taxkey = dwcrec[GBIF.ACC_TAXON_FLD]
            try:
                iis_reclist = self.nnsl.data[taxkey]
            except Exception:
                pass
            else:
                dwcrec = self._assess_occurrence(taxkey, dwcrec, iis_reclist)

            self._csv_writer.writerow(dwcrec)
