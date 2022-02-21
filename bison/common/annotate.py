"""Common classes for adding USGS RIIS info to GBIF occurrences."""
import os

from bison.common.constants import (
    ENCODING, GBIF, LOG, NEW_RIIS_ASSESSMENT_FLD, NEW_RIIS_KEY_FLD, RIIS_SPECIES, STATES)
from bison.common.occurrence import DwcData
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
            do_resolve (bool): flag indicating whether to (re-)query GBIF for updated
                accepted name/key
            logger (object): logger for saving relevant processing messages
        """
        self._datapath = datapath
        self._csvfile = os.path.join(datapath, gbif_occ_fname)

        if logger is None:
            logger = get_logger(datapath)
        self._log = logger

        self.nnsl = NNSL(datapath, logger=logger)
        if do_resolve is True:
            self.nnsl.read_riis(read_resolved=False)
            self.nnsl.resolve_riis_to_gbif_taxa()
        else:
            self.nnsl.read_riis(read_resolved=True)
        # Input reader
        self._dwcdata = DwcData(datapath, gbif_occ_fname, logger=logger)
        # Output writer
        self._csv_writer = None

        self._valid_states = [k.lower() for k in STATES.keys()]
        self._valid_states.extend([v.lower() for v in STATES.values()])

        self.good_locations = {}
        self.bad_locations = {}


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
    def open(self, outfname=None):
        """Open the DwcData for reading and the csv_writer for writing.

        Also reads the first record and writes the header.

        Args:
            outfname: full filename for output file.

        Raises:
            Exception: on failure to open the csv_writer.
        """
        if outfname is None:
            outfname = self.annotated_dwc_fname
        self._dwcdata.open()
        header = self._dwcdata.fieldnames
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
    def assess_occurrence(self, dwcrec, iis_reclist):
        """Find RIIS assessment matching the acceptedTaxonKey and state in this record.

        Args:
            dwcrec: dictionary of original DwC specimen occurrence record
            iis_reclist: list of RIIS records with acceptedTaxonKey matching the
                acceptedTaxonKey for this occurrence

        Returns:
            riis_assessment: Determination of "introduced" or "invasive" for this
                record with species in this locaation.
            riis_id: locally unique RIIS occurrenceID identifying this determination
                for this species in this location.
        """
        riis_assessment = None
        riis_key = None
        occ_state = dwcrec[GBIF.STATE_FLD].lower()
        for iisrec in iis_reclist:

            # Double check NNSL dict key == RIIS resolved key == occurrence accepted key
            if dwcrec[GBIF.ACC_TAXON_FLD] != iisrec[RIIS_SPECIES.NEW_GBIF_KEY]:
                logit(self._log, "WTF is happening?!?")

            # Look for AK or HI
            if ((occ_state in ("ak", "alaska") and iisrec[RIIS_SPECIES.LOCALITY_FLD] == "AK")
                or
                (occ_state in ("hi", "hawaii") and iisrec[RIIS_SPECIES.LOCALITY_FLD] == "HI")):
                riis_assessment = iisrec[RIIS_SPECIES.ASSESSMENT_FLD]
                riis_key = iisrec[RIIS_SPECIES.KEY]

            # Not AK or HI, must be L48
            elif iisrec[RIIS_SPECIES.LOCALITY_FLD] == "L48":
                riis_assessment = iisrec[RIIS_SPECIES.ASSESSMENT_FLD]
                riis_key = iisrec[RIIS_SPECIES.KEY]

        return riis_assessment, riis_key

    # ...............................................
    def annotate_record(self, dwcrec, iis_reclist):
        """Add RIIS data to a GBIF record.

        Args:
            riis_assessment: Determination of "introduced" or "invasive" for this species in this locaation.
            riis_id: locally unique RIIS occurrenceID identifying this determination for this species in this location.
            dwcrec: dictionary of original DwC specimen occurrence record
            iis_reclist: list of RIIS records with acceptedTaxonKey matching the
                acceptedTaxonKey for this occurrence
        """
        pass

    # ...............................................
    def _aggregate_locations(self, dwcrec):
        # Save state/county from DwC record
        state = dwcrec[GBIF.STATE_FLD].lower()
        county = dwcrec[GBIF.COUNTY_FLD].lower()
        if state in self._valid_states and county not in ("", None):
            try:
                self.good_locations[state].add(county)
            except:
                self.good_locations[dwcrec[GBIF.STATE_FLD]] = set(dwcrec[GBIF.COUNTY_FLD])
        else:
            try:
                self.bad_locations[state].add(county)
            except:
                self.bad_locations[dwcrec[GBIF.STATE_FLD]] = set(dwcrec[GBIF.COUNTY_FLD])

    # ...............................................
    def append_dwca_records(self):
        """Append 'introduced' or 'invasive' status to GBIF DWC occurrence records."""
        self.open(self.annotated_dwc_fname)
        # iterate over DwC records
        dwcrec = self._dwcdata.get_record()
        while dwcrec is not None:
            if (self._dwcdata.recno % LOG.INTERVAL) == 0:
                logit(self._log, '*** Record number {} ***'.format(self._dwcdata.recno))
            self._aggregate_locations(dwcrec)
            # Find acceptedTaxonKey in DwC record
            taxkey = dwcrec[GBIF.ACC_TAXON_FLD]
            # Find RIIS records for this acceptedTaxonKey
            try:
                iis_reclist = self.nnsl.data[taxkey]
            except Exception:
                iis_reclist = None
            else:
                if iis_reclist is not None:
                    riis_assessment, riis_key = self.assess_occurrence(dwcrec, iis_reclist)
                    dwcrec[NEW_RIIS_ASSESSMENT_FLD] = riis_assessment
                    dwcrec[NEW_RIIS_KEY_FLD] = riis_key

            try:
                self._csv_writer.writerow(dwcrec)
            except ValueError as e:
                print("Error {} on line {}".format(e, self._dwcdata.recno))

            dwcrec = self._dwcdata.get_record()

