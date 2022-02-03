"""Common classes for adding USGS RIIS info to GBIF occurrences."""
from bison.common.constants import (GBIF, LOG)
from bison.common.occurrence import GBIFReader
from bison.common.riis import NNSL

from bison.tools.util import get_logger, logit


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

        if logger is None:
            logger = get_logger(datapath)
        self._log = logger

        self.nnsl = NNSL(datapath, logger=logger)
        if do_resolve is True:
            self.resolve_write_gbif_taxa()
        else:
            self.nnsl.read_species()

        self.gbif_rdr = GBIFReader(datapath, gbif_occ_fname, logger=logger)
        self.gbif_rdr.open()

    # ...............................................
    def append_dwca_records(self):
        """Append 'introduced' or 'invasive' status to GBIF DWC occurrence records."""
        for rec in self._gbif_rdr:
            if rec is None:
                break
            elif (self._gbif_rdr.recno % LOG.INTERVAL) == 0:
                logit(self._log, '*** Record number {} ***'.format(self._gbif_rdr.recno))

            taxkey = rec[GBIF.ACC_TAXON_FLD]
            sciname = rec[GBIF.ACC_NAME_FLD]

            if taxkey is not None and sciname is not None:
                logit(self._log, "They're here")
