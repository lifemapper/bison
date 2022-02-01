"""Common classes for BISON RIIS data processing."""
import csv
import os

from bison.common.constants import (ERR_SEPARATOR, GBIF, LINENO_FLD, LOG, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)
from bison.tools.gbif_api import GbifSvc
from bison.tools.util import get_csv_dict_writer, get_logger


# .............................................................................
def standardize_name(sci_name, sci_author):
    """Construct a small record to hold relevant data for a RIIS species/locality record.

    Args:
        sci_name (str): canonical scientific name
        sci_author (str): authorship for scientific name

    Returns:
        String that is a concatenation of the 2 inputs, with a space between.
    """
    return "{} {}".format(sci_name, sci_author)


# .............................................................................
class RIISRec():
    """Class for comparing relevant fields in species data records."""
    def __init__(self, record, line_num, new_gbif_key=None, new_gbif_name=None):
        """Construct a small record to hold relevant data for a RIIS species/locality record.

        Args:
            record (dict): original US RIIS record
            line_num (int): line number for this record in the original US RIIS file
            new_gbif_key (int): Newly resolved GBIF TaxonKey, unique identifier for the accepted taxon.
            new_gbif_name (str): Newly resolved GBIF scientific name to match new_gbif_key.

        Raises:
            ValueError: on non-integer GBIF taxonKey or non-integer ITIS TSN
        """
        self.data = record
        self.data[RIIS_SPECIES.NEW_GBIF_KEY] = new_gbif_key
        self.data[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD] = new_gbif_name
        self.data[LINENO_FLD] = line_num
        self.name = standardize_name(record[RIIS_SPECIES.SCINAME_FLD], record[RIIS_SPECIES.SCIAUTHOR_FLD])

        # Set missing GBIF or ITIS key to -1
        for fld in (RIIS_SPECIES.GBIF_KEY, RIIS_SPECIES.ITIS_KEY):
            taxon_key = record[fld]
            if not taxon_key:
                taxon_key = -1
            else:
                try:
                    taxon_key = int(taxon_key)
                except ValueError:
                    raise
            self.data[fld] = taxon_key

        # Edit taxonomy authority to trim "Accepted "
        prefix = 'Accepted '
        taxon_authority = record[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
        try:
            is_accepted = taxon_authority.startswith(prefix)
        except AttributeError:
            is_accepted = False
        if is_accepted:
            taxon_authority = taxon_authority[len(prefix):]
        else:
            print('taxon_authority value {} in line {}'.format(taxon_authority, line_num))
        self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] = taxon_authority

    # ...............................................
    def logit(self, msg):
        """Log a message to the console or file.

        Args:
            msg (str): Message to be printed or written to file.
        """
        if self._log:
            self._log.info(msg)
        else:
            print(msg)

    # ...............................................
    def update_gbif_resolution(self, gbif_key, gbif_sciname):
        """Update the new gbif resolution fields in the data dictionary.

        Args:
            gbif_key (int): current GBIF accepted taxonKey, in the GBIF Backbone
                Taxonomy, for a scientific name
            gbif_sciname (str):  current GBIF accepted scientific name, in the GBIF Backbone
                Taxonomy, for a scientific name
        """
        self.data[RIIS_SPECIES.NEW_GBIF_KEY] = gbif_key
        self.data[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD] = gbif_sciname

    # ...............................................
    def is_name_match(self, rrec):
        """Test equality of scientific name, author and kingdom fields.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec scientific name, author, and kingdom match.
        """
        return (self.data[RIIS_SPECIES.SCINAME_FLD] == rrec.data[RIIS_SPECIES.SCINAME_FLD]
                and self.data[RIIS_SPECIES.KINGDOM_FLD] == rrec.data[RIIS_SPECIES.KINGDOM_FLD]
                and self.data[RIIS_SPECIES.SCIAUTHOR_FLD] == rrec.data[RIIS_SPECIES.SCIAUTHOR_FLD])

    # ...............................................
    def is_duplicate(self, rrec):
        """Test equality of all fields except the occurrenceID and linenum, effectively a duplicate.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec scientific name, author, kingdom, GBIF key, ITIS TSN, assessment, and location match.
        """
        return (self.is_name_match(rrec)
                and self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY]
                and self.data[RIIS_SPECIES.ITIS_KEY] == rrec.data[RIIS_SPECIES.ITIS_KEY]
                and self.data[RIIS_SPECIES.ASSESSMENT_FLD] == rrec.data[RIIS_SPECIES.ASSESSMENT_FLD]
                and self.data[RIIS_SPECIES.LOCALITY_FLD] == rrec.data[RIIS_SPECIES.LOCALITY_FLD])

    # ...............................................
    def is_duplicate_locality(self, rrec):
        """Test equality of scientific name, author and kingdom fields and locality.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec scientific name, author, kingdom, and location match.
        """
        return (self.is_name_match(rrec)
                and self.data[RIIS_SPECIES.LOCALITY_FLD] == rrec.data[RIIS_SPECIES.LOCALITY_FLD])

    # ...............................................
    def is_assessment_locality_match(self, rrec):
        """Test equality of assessment and locality match.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec assessment and location match.
        """
        return (self.data[RIIS_SPECIES.ASSESSMENT_FLD] == rrec.data[RIIS_SPECIES.ASSESSMENT_FLD]
                and self.data[RIIS_SPECIES.LOCALITY_FLD] == rrec.data[RIIS_SPECIES.LOCALITY_FLD])

    # ...............................................
    def is_gbif_match(self, rrec):
        """Test equality of GBIF taxonKey.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec GBIF key match.
        """
        return (self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY])

    # ...............................................
    def is_itis_match(self, rrec):
        """Test equality of ITIS TSN.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec ITIS TSN match.
        """
        return (self.data[RIIS_SPECIES.ITIS_KEY] == rrec.data[RIIS_SPECIES.ITIS_KEY])

    # ...............................................
    def consistent_gbif_resolution(self):
        """Test equality of existing and new gbif taxon key resolution.

        Returns:
            True if self and rrec GBIF key match.
        """
        return (self.data[RIIS_SPECIES.GBIF_KEY] == self.data[RIIS_SPECIES.NEW_GBIF_KEY])

    # ...............................................
    def is_taxauthority_match(self, rrec):
        """Test equality of taxon authority and authority key.

        Args:
            rrec (bison.riis.RIISRec): object containing a BISON Introduced or Invasive species record

        Returns:
            True if self and rrec name authority are both GBIF and GBIF keys match
            or authority are both ITIS and the ITIS TSNs match.
        """
        # Test GBIF match
        if (
                self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "GBIF"
                and rrec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "GBIF"
                and self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY]
        ):
            return True
        # Test ITIS match
        else:
            return (
                self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "ITIS"
                and rrec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "ITIS"
                and self.data[RIIS_SPECIES.ITIS_KEY] == rrec.data[RIIS_SPECIES.ITIS_KEY]
            )


# .............................................................................
class NNSL:
    """Class for reading, writing, comparing RIIS species data records."""

    # ...............................................
    def __init__(self, datapath, logger=None):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            datapath (str): Path to the base of the input data, used to construct full
                filenames from basepath and relative path constants.
            logger (object): logger for writing messages to file and console
        """
        self._datapath = datapath.rstrip(os.sep)
        if logger is None:
            logger = get_logger(datapath)
        self._log = logger
        self.auth_fname = "{}.{}".format(
            os.path.join(self._datapath, RIIS_AUTHORITY.FNAME), RIIS.DATA_EXT
        )
        self.riis_fname = "{}.{}".format(
            os.path.join(self._datapath, RIIS_SPECIES.FNAME), RIIS.DATA_EXT
        )
        self.test_riis_fname = "{}.{}".format(
            os.path.join(self._datapath, RIIS_SPECIES.DEV_FNAME), RIIS.DATA_EXT
        )

        # Test and clean headers of non-ascii characters
        self.auth_header = self._clean_header(self.auth_fname, RIIS_AUTHORITY.HEADER)
        self.riis_header = self._clean_header(self.riis_fname, RIIS_SPECIES.HEADER)
        # Trimmed and updated Non-native Species List, built from RIIS
        self.nnsl = None
        self.nnsl_header = None

    # ...............................................
    def _read_authorities(self) -> set:
        """Assemble a set of unique authority identifiers for use as foreign keys in the MasterList.

        Returns:
            Set of authority identifiers valid for use as foreign keys in related datasets.
        """
        authorities = set()
        with open(self.auth_fname, "r", newline="") as csvfile:
            rdr = csv.DictReader(
                csvfile,
                fieldnames=self.auth_header,
                delimiter=RIIS.DELIMITER,
                quotechar=RIIS.QUOTECHAR,
            )
            for row in rdr:
                authorities.add(row[RIIS_AUTHORITY.KEY])
        return authorities

    # ...............................................
    @property
    def gbif_resolved_riis_fname(self):
        """Construct a filename for the resolved RIIS from the original.

        Returns:
            updated_riis_fname: output filename derived from the input RIIS filename
        """
        basename, ext = os.path.splitext(self.riis_fname)
        updated_riis_fname = "{}_updated_gbif{}".format(basename, ext)
        return updated_riis_fname

    # ...............................................
    def read_species(self):
        """Assemble 2 dictionaries of records with valid and invalid data."""
        self.bad_species = {}
        self.nnsl = {}
        # Use species data with updated GBIF taxonomic resolution if exists
        if os.path.exists(self.gbif_resolved_riis_fname):
            infname = self.gbif_resolved_riis_fname
        else:
            infname = self.riis_fname

        with open(infname, "r", newline="") as csvfile:
            rdr = csv.DictReader(
                csvfile,
                fieldnames=self.riis_header,
                delimiter=RIIS.DELIMITER,
                quotechar=RIIS.QUOTECHAR,
            )
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    # Read new gbif resolutions if they exist
                    try:
                        new_gbif_key = row[RIIS_SPECIES.NEW_GBIF_KEY]
                        new_gbif_name = row[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD]
                    except KeyError:
                        new_gbif_key = new_gbif_name = None
                    # Create record of original data and optional new data
                    try:
                        rec = RIISRec(row, lineno, new_gbif_key=new_gbif_key, new_gbif_name=new_gbif_name)
                    except ValueError:
                        row[LINENO_FLD] = lineno
                        self.bad_species[lineno] = row
                    # Save to list of records for this species
                    try:
                        self.nnsl[rec.name].append(rec)
                    except KeyError:
                        self.nnsl[rec.name] = [rec]

    # ...............................................
    def _get_accepted_name_key(self, gbifrec):
        name = None
        key = None
        msg = None
        status = None

        if gbifrec:
            try:
                match_type = gbifrec[GBIF.MATCH_FLD]
            except KeyError:
                match_type = 'unknown'
                # print('No match type in record')

            if match_type != 'NONE':
                try:
                    # Results from species/match?name=<name>
                    status = gbifrec["status"].lower()
                    key_fld = "usageKey"
                    accepted_key_fld = "acceptedUsageKey"
                except KeyError:
                    try:
                        # Results from species/<key>
                        status = gbifrec[GBIF.STATUS_FLD].lower()
                        key_fld = "key"
                        accepted_key_fld = "nubKey"
                    except KeyError:
                        print('Failed to get status from {}'.format(gbifrec))

                if status is not None:
                    if status == "accepted":
                        key = gbifrec[key_fld]
                        name = gbifrec[GBIF.NAME_FLD]
                    else:
                        # Return accepted key to use for another query
                        try:
                            key = gbifrec[accepted_key_fld]
                        except KeyError:
                            msg = "No accepted key found in results {}".format(gbifrec)

        return key, name, msg

    # ...............................................
    def _add_msg(self, msgdict, key, msg):
        if msg:
            try:
                msgdict[key].append(msg)
            except KeyError:
                msgdict[key] = [msg]

    # ...............................................
    def resolve_gbif_species(self):
        """Resolve accepted name and key from the GBIF taxonomic backbone, add to self.nnsl."""
        msgdict = {}
        # New fields for GBIF resolution in species records

        if not self.nnsl:
            self.read_species()
        gbif_svc = GbifSvc()
        for spname, reclist in self.nnsl.items():
            # Just get name/kingdom from first record
            rec1 = reclist[0]
            taxkey = rec1.data[RIIS_SPECIES.GBIF_KEY]
            kingdom = reclist[0].data[RIIS_SPECIES.KINGDOM_FLD]

            # Try to match, if match is not 'accepted', repeat with returned accepted keys
            new_key, new_name, msg = self._find_current_accepted_taxon(gbif_svc, spname, kingdom, taxkey)
            self._add_msg(msgdict, spname, msg)

            # Supplement all records for this species with GBIF accepted key and name
            for sprec in reclist:
                sprec.update_gbif_resolution(new_key, new_name)

    # ...............................................
    def _find_current_accepted_taxon(self, gbif_svc, sciname, kingdom, taxkey):
        gbifrec = gbif_svc.query_for_name(sciname=sciname, kingdom=kingdom)

        # Get match results
        new_key, new_name, msg = self._get_accepted_name_key(gbifrec)

        # Match
        if new_key != taxkey or new_name != sciname:
            msg = "File GBIF taxonKey {} / {} conflicts with API GBIF taxonKey {} / {}".format(
                taxkey, sciname, new_key, new_name)

        # If match results are not 'accepted', query for the returned acceptedUsageKey
        if new_key and new_name is None:
            # self._add_msg(msgdict, spname, msg)
            gbifrec2 = gbif_svc.query_for_name(taxkey=new_key)
            # Replace new key and name with results of 2nd query
            new_key, new_name, msg = self._get_accepted_name_key(gbifrec2)

        return new_key, new_name, msg

    # ...............................................
    def resolve_write_gbif_taxa(self, outfname=None):
        """Resolve accepted name and key from the GBIF taxonomic backbone, write to file.

        Args:
            outfname (str): Full path and filename for updated RIIS records.
        """
        msgdict = {}
        if not outfname:
            outfname = self.gbif_resolved_riis_fname
        # Read and get a record for its keys
        rec1 = self._get_random_rec()
        newheader = rec1.data.keys()
        writer, outf = get_csv_dict_writer(outfname, newheader, RIIS.DELIMITER, fmode="w")

        name_count = 0
        gbif_svc = GbifSvc()
        try:
            for spname, reclist in self.nnsl.items():
                name_count += 1
                # Resolve each name, update each record (1-3) for that name
                try:
                    # Get name/kingdom from first record
                    taxkey = reclist[0].data[RIIS_SPECIES.GBIF_KEY]
                    kingdom = reclist[0].data[RIIS_SPECIES.KINGDOM_FLD]

                    # Try to match, if match is not 'accepted', repeat with returned accepted keys
                    new_key, new_name, msg = self._find_current_accepted_taxon(gbif_svc, spname, kingdom, taxkey)
                    self._add_msg(msgdict, spname, msg)

                    # Supplement all records for this name with GBIF accepted key and sciname, then write
                    for rec in reclist:
                        rec.update_gbif_resolution(new_key, new_name)
                        # then write records
                        try:
                            writer.writerow(rec.data)
                        except Exception as e:
                            print("Failed to write {}, {}".format(rec.data, e))
                            self._add_msg(msgdict, spname, 'Failed to write record {} ({})'.format(rec.data, e))

                    if (name_count % LOG.INTERVAL) == 0:
                        self.logit('*** NNSL Name {} ***'.format(name_count))

                except Exception:
                    self._add_msg(msgdict, spname, 'Failed to read records in dict')
        except Exception as e:
            self._add_msg(msgdict, 'unknown_error', '{}'.format(e))
        finally:
            outf.close()

    # ...............................................
    def _get_random_rec(self):
        if not self.nnsl:
            self.read_species()
        # First name in keys, first record in value-list, keys from data attribute
        name = list(self.nnsl.keys())[0]
        rec1 = self.nnsl[name][0]
        return rec1

    # ...............................................
    def write_species(self, outfname):
        """Write species data to a CSV file.

        Args:
            outfname (str): full path and filename for output file.
        """
        rec1 = self._get_random_rec()
        header = rec1.data.keys()

        writer, outf = get_csv_dict_writer(outfname, header, RIIS.DELIMITER, fmode="w")
        try:
            for reclist in self.nnsl.values():
                for rec in reclist:
                    try:

                        writer.writerow(rec.data)
                    except Exception as e:
                        print("Failed to write {}, {}".format(rec.data, e))
        finally:
            outf.close()

    # ...............................................
    def _only_ascii(self, name):
        """Return a string without non-ascii characters.

        Args:
            name (str): name to strip of all non-ascii characters

        Returns:
            cleaned name string
        """
        good = []
        for ch in name:
            if ch.isascii():
                good.append(ch)
        better_name = "".join(good)
        return better_name

    # ...............................................
    def _clean_header(self, fname, expected_header):
        """Compare the actual header in fname with an expected_header.

        Print warnings if actual fieldnames contain non-ascii characters.  Print errors
        if the actual header does not contain the same fieldnames, in the same order, as
        the expected_header.

        Args:
            fname (str): CSV data file containing header to check
            expected_header (list): list of fieldnames expected for the data file

        Returns:
             list of fieldnames from the actual header, stripped of non-ascii characters
        """
        clean_header = []
        with open(fname, "r", newline="") as csvfile:
            rdr = csv.reader(
                csvfile, delimiter=RIIS.DELIMITER, quotechar=RIIS.QUOTECHAR
            )
            header = next(rdr)
        # Test header length
        fld_count = len(header)
        if fld_count != len(expected_header):
            print(ERR_SEPARATOR)
            print("[Error] Header has {} fields, != {} expected count".format(
                fld_count, len(expected_header))
            )
        # Test header fieldnames
        for i in range(len(header)):
            good_fieldname = self._only_ascii(header[i])
            if header[i] != good_fieldname:
                print(ERR_SEPARATOR)
                print('[Warning] File {}, header field {} "{}" has non-ascii characters'.format(
                    fname, i, header[i])
                )

            clean_header.append(good_fieldname)
            if good_fieldname != expected_header[i]:
                print(ERR_SEPARATOR)
                print("[Error] Header fieldname {} != {} expected fieldname".format(
                    header[i], expected_header[i])
                )
        return clean_header


# .............................................................................
if __name__ == "__main__":
    # Test number of rows and columns in authority and species files
    pass
