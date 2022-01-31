"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import csv
import os

from bison.common.constants import (DATA_PATH, ERR_SEPARATOR, GBIF, LINENO_FLD, LOG, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)
from bison.common.occurrence import GBIFReader
from bison.common.riis import NNSL

from bison.tools.gbif_api import GbifSvc
from bison.tools.util import get_csv_dict_writer, get_logger


# .............................................................................
class Annotator():
    """Class for adding USGS RIIS info to GBIF occurrences."""
    def __init__(self, datapath, gbif_occ_fname, do_resolve=False, logger=None):
        """Constructor.

        Args:
            datapath (str): base directory for datafiles
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
        for rec in self._gbif_rdr:
            if rec is None:
                break
            elif (self._gbif_rdr.recno % LOG.INTERVAL) == 0:
                self.logit('*** Record number {} ***'.format(self._gbif_rdr.recno))

            taxkey = rec[GBIF.TAXON_FLD]

            if taxkey
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
