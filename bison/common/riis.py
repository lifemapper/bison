"""Common classes for BISON RIIS data processing."""
import csv
import os

from bison.common.constants import (
    ERR_SEPARATOR, GBIF, LINENO_FLD, LOG, NEW_GBIF_KEY_FLD, NEW_GBIF_SCINAME_FLD, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)
from bison.tools.gbif_api import GbifSvc
from bison.tools.util import get_csv_dict_reader, get_csv_dict_writer, get_logger


# .............................................................................
def standardize_name(sci_name, sci_author):
    """Construct a small record to hold relevant data for a RIIS species/locality record.

    Args:
        sci_name (str): canonical scientific name
        sci_author (str): authorship for scientific name

    Returns:
        String that is a concatenation of the 2 inputs, with a space between.
    """
    return f"{sci_name} {sci_author}"


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
        self.data[NEW_GBIF_KEY_FLD] = new_gbif_key
        self.data[NEW_GBIF_SCINAME_FLD] = new_gbif_name
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

        # # Edit taxonomy authority to trim "Accepted "
        # prefix = 'Accepted '
        # start_idx = len(prefix)
        # taxon_authority = record[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
        # try:
        #     is_accepted = taxon_authority.startswith(prefix)
        # except AttributeError:
        #     pass
        # else:
        #     if is_accepted:
        #         self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] = taxon_authority[start_idx:]

    # ...............................................
    @property
    def locality(self):
        """Public property for locality value.

        Returns:
            locality value
        """
        return self.data[RIIS_SPECIES.LOCALITY_FLD]

    # ...............................................
    @property
    def assessment(self):
        """Public property for locality value.

        Returns:
            RIIS assessment value
        """
        return self.data[RIIS_SPECIES.ASSESSMENT_FLD].lower()

    # ...............................................
    @property
    def occurrence_id(self):
        """Public property for RIIS occurrenceID value.

        Returns:
            RIIS occurrence_id value
        """
        return self.data[RIIS_SPECIES.KEY]

    # ...............................................
    @property
    def gbif_taxon_key(self):
        """Public property for GBIF accepted taxon key value.

        Returns:
            latest resolved GBIF accepted taxon key value
        """
        return self.data[NEW_GBIF_KEY_FLD]

    # ...............................................
    def update_data(self, gbif_key, gbif_sciname):
        """Update the new gbif resolution fields in the data dictionary.

        Args:
            gbif_key (int): current GBIF accepted taxonKey, in the GBIF Backbone
                Taxonomy, for a scientific name
            gbif_sciname (str):  current GBIF accepted scientific name, in the GBIF Backbone
                Taxonomy, for a scientific name
        """
        self.data[NEW_GBIF_KEY_FLD] = gbif_key
        self.data[NEW_GBIF_SCINAME_FLD] = gbif_sciname

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
        return (self.data[RIIS_SPECIES.GBIF_KEY] == self.data[NEW_GBIF_KEY_FLD])

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
                self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "Accepted GBIF"
                and rrec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "Accepted GBIF"
                and self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY]
        ):
            return True
        # Test ITIS match
        else:
            return (
                self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "Accepted ITIS"
                and rrec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "Accepted ITIS"
                and self.data[RIIS_SPECIES.ITIS_KEY] == rrec.data[RIIS_SPECIES.ITIS_KEY]
            )


# .............................................................................
class NNSL:
    """Class for reading, writing, comparing RIIS species data records."""

    # ...............................................
    def __init__(self, riis_filename, logger=None):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            riis_filename (str): Path to the base of the input data, used to construct full
                filenames from basepath and relative path constants.
            logger (object): logger for writing messages to file and console
        """
        datapath, fname_w_ext = os.path.split(riis_filename)
        basename, ext = os.path.splitext(fname_w_ext)

        self._datapath = datapath
        self._csvfile = riis_filename

        if logger is None:
            logger = get_logger(os.path.join(datapath, LOG.DIR))
        self._log = logger

        self.auth_fname = f"{os.path.join(self._datapath, RIIS_AUTHORITY.FNAME)}.{RIIS.DATA_EXT}"

        # Trimmed and updated Non-native Species List, built from RIIS
        self.by_gbif_taxkey = None
        self.by_riis_id = None
        self.nnsl_header = None

    # ...............................................
    def _read_authorities(self) -> set:
        """Assemble a set of unique authority identifiers for use as foreign keys in the MasterList.

        Returns:
            Set of authority identifiers valid for use as foreign keys in related datasets.
        """
        authorities = set()
        rdr, f = get_csv_dict_reader(self.auth_fname, RIIS.DELIMITER)
        # with open(self.auth_fname, "r", newline="") as csvfile:
        #     rdr = csv.DictReader(
        #         csvfile,
        #         fieldnames=RIIS_AUTHORITY.HEADER,
        #         delimiter=RIIS.DELIMITER,
        #         quotechar=RIIS.QUOTECHAR,
        #     )
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
        basename, ext = os.path.splitext(self._csvfile)
        updated_riis_fname = f"{basename}_updated_gbif{ext}"
        return updated_riis_fname

    # ...............................................
    @classmethod
    def get_gbif_resolved_riis_fname(cls, orig_riis_filename):
        """Construct a filename for the resolved RIIS from the original.

        Args:
            orig_riis_filename (str): full original (non-resolved) RIIS filename

        Returns:
            updated_riis_fname: output filename derived from the input RIIS filename
        """
        basename, ext = os.path.splitext(orig_riis_filename)
        updated_riis_fname = f"{basename}_updated_gbif{ext}"
        return updated_riis_fname

    # ...............................................
    @property
    def gbif_resolved_riis_header(self):
        """Construct the expected header for the resolved RIIS.

        Returns:
            updated_riis_header: fieldnames for the updated file
        """
        header = RIIS_SPECIES.HEADER.copy()
        header.append(NEW_GBIF_KEY_FLD)
        header.append(NEW_GBIF_SCINAME_FLD)
        header.append(LINENO_FLD)
        return header

    # ...............................................
    def get_riis_by_gbif_taxonkey(self, gbif_taxon_key):
        """Get all RIIS records for this GBIF taxonKey.

        Args:
            gbif_taxon_key (str): unique identifier for GBIF taxon record

        Returns:
            list of RIISRecs for the species with this GBIF taxonKey
        """
        riis_recs = []
        try:
            riis_recs = self.by_gbif_taxkey[gbif_taxon_key]
        except KeyError:
            # Taxon is not present
            pass
        return riis_recs

    # ...............................................
    def get_assessments_for_gbif_taxonkey(self, gbif_taxon_key):
        """Get all RIIS assessments for this GBIF taxonKey.

        Args:
            gbif_taxon_key (str): unique identifier for GBIF taxon record

        Returns:
            dict of 0 or more, like {"AK": "introduced", "HI": "invasive", "L48": "introduced"}
                for the species with this GBIF taxonKey
        """
        assessments = {}
        riis_recs = self.get_riis_by_gbif_taxonkey(gbif_taxon_key)
        for riis in riis_recs:
            assessments[riis.data[RIIS_SPECIES.LOCALITY_FLD]] = riis.data[RIIS_SPECIES.ASSESSMENT_FLD]
        return assessments

    # ...............................................
    def get_assessment_for_gbif_taxonkey_region(self, gbif_taxon_key, region):
        """Get all RIIS assessments for this GBIF taxonKey.

        Args:
            gbif_taxon_key (str): unique identifier for GBIF taxon record
            region (str): RIIS-defined US region for assessment, choices are AK, HI, L48

        Returns:
            dict of 0 or more, like {"AK": "introduced", "HI": "invasive", "L48": "introduced"}
                for the species with this GBIF taxonKey
        """
        assess = "presumed_native"
        recid = None
        riis_recs = self.get_riis_by_gbif_taxonkey(gbif_taxon_key)
        for riis in riis_recs:
            if region == riis.locality:
                assess = riis.assessment
                recid = riis.occurrence_id
        return assess, recid

    # ...............................................
    def read_riis(self, read_resolved=False):
        """Assemble 2 dictionaries of records with valid and invalid data.

        Args:
            read_resolved (bool): True if reading amended RIIS data, with scientific
                names resolved to the currently accepted taxon.

        Raises:
            FileNotFoundError: if read_resolved is True but resolved data file does not exist
            Exception: on read error
        """
        self.bad_species = {}
        self.by_gbif_taxkey = {}
        self.by_riis_id = {}
        if read_resolved is True:
            # Use species data with updated GBIF taxonomic resolution if exists
            if not os.path.exists(self.gbif_resolved_riis_fname):
                raise FileNotFoundError(f"File {self.gbif_resolved_riis_fname} does not exist")
            else:
                infname = self.gbif_resolved_riis_fname
        else:
            infname = self._csvfile

        # Test and clean headers of non-ascii characters
        good_header = self._fix_header(infname, RIIS_SPECIES.HEADER)
        if good_header is False:
            raise Exception(f"Unexpected file header found in {self._csvfile}")
        rdr, inf = get_csv_dict_reader(infname, RIIS.DELIMITER, fieldnames=good_header, quote_none=False)

        self._log.debug(f"Reading RIIS from {infname}")
        try:
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    # Read new gbif resolutions if they exist
                    if read_resolved is True:
                        new_gbif_key = row[NEW_GBIF_KEY_FLD]
                        new_gbif_name = row[NEW_GBIF_SCINAME_FLD]
                    else:
                        new_gbif_key = new_gbif_name = None
                    # Create record of original data and optional new data
                    try:
                        rec = RIISRec(row, lineno, new_gbif_key=new_gbif_key, new_gbif_name=new_gbif_name)
                    except ValueError:
                        row[LINENO_FLD] = lineno
                        self.bad_species[lineno] = row

                    # Organize the records by scientificName before query into GBIF, or failed query
                    #                   or by GBIF taxonKey after successful query
                    index = rec.name
                    if read_resolved is True and new_gbif_key is not None:
                        index = new_gbif_key
                    # Group records by matching name/taxon
                    try:
                        self.by_gbif_taxkey[index].append(rec)
                    except KeyError:
                        self.by_gbif_taxkey[index] = [rec]

                    # Use RIIS occurrenceID for dictionary key
                    self.by_riis_id[row[RIIS_SPECIES.KEY]] = rec

        except Exception as e:
            raise(e)
        finally:
            inf.close()

    # ...............................................
    def _get_alternatives(self, gbifrec):
        name = None
        key = None
        try:
            alternatives = gbifrec["alternatives"]
        except KeyError:
            pass
        else:
            # Take the first accepted alternative, assuming ranked high-low confidence
            for alt in alternatives:
                try:
                    status = alt["status"].lower()
                except KeyError:
                    print(f"Failed to get status from alternative {alt}")
                else:
                    if status == "accepted":
                        key = alt["usageKey"]
                        name = alt["scientificName"]
                    else:
                        try:
                            key = alt["acceptedUsageKey"]
                        except KeyError:
                            pass
        return key, name

    # ...............................................
    def _get_accepted_name_key_from_match(self, gbifrec):
        name = None
        key = None
        msg = None
        try:
            # Results from fuzzy match search (species/match?name=<name>)
            match_type = gbifrec[GBIF.MATCH_FLD]
        except KeyError:
            print(f"Failed to get matchType from {gbifrec}")
        else:
            if match_type != 'NONE':
                try:
                    # Results from species/match?name=<name>
                    status = gbifrec["status"].lower()
                except KeyError:
                    print(f"Failed to get status from {gbifrec}")
                else:
                    if status == "accepted":
                        key = gbifrec["usageKey"]
                        name = gbifrec["scientificName"]
                    else:
                        try:
                            key = gbifrec["acceptedUsageKey"]
                        except KeyError:
                            key, name = self._get_alternatives(gbifrec)
        return key, name, msg

    # ...............................................
    def _get_accepted_name_key_from_get(self, gbifrec):
        name = None
        key = None
        msg = None
        try:
            # Results from species/<key>
            status = gbifrec[GBIF.STATUS_FLD].lower()
        except KeyError:
            msg = f"Failed to get status from {gbifrec}"
        else:
            if status == "accepted":
                key = gbifrec["key"]
                name = gbifrec["scientificName"]
            else:
                key = gbifrec["acceptedKey"]
                name = gbifrec["accepted"]
        return key, name, msg

    # ...............................................
    def _add_msg(self, msgdict, key, msg):
        if msg:
            try:
                msgdict[key].append(msg)
            except KeyError:
                msgdict[key] = [msg]

    # ...............................................
    def _find_current_accepted_taxon(self, gbif_svc, sciname, kingdom, taxkey):
        # Query GBIF with the name/kingdom
        gbifrec = gbif_svc.query_by_name(sciname, kingdom=kingdom)

        # Interpret match results
        new_key, new_name, msg = self._get_accepted_name_key_from_match(gbifrec)

        if new_name is None:
            # Try again, query GBIF with the returned acceptedUsageKey (new_key)
            if new_key is not None and new_name is None:
                gbifrec2 = gbif_svc.query_by_namekey(taxkey=new_key)
                # Replace new key and name with results of 2nd query
                new_key, new_name, msg = self._get_accepted_name_key_from_get(gbifrec2)

        return new_key, new_name, msg

    # ...............................................
    def resolve_riis_to_gbif_taxa(self):
        """Resolve accepted name and key from the GBIF taxonomic backbone, write to file.

        Returns:
            name_count: count of updated records
            rec_count: count of resolved species
        """
        msgdict = {}

        if not self.by_gbif_taxkey:
            self.read_riis(read_resolved=False)

        name_count = 0
        rec_count = 0
        gbif_svc = GbifSvc()
        try:
            for name, reclist in self.by_gbif_taxkey.items():
                # Resolve each name, update each record (1-3) for that name
                try:
                    # Try to match, if match is not 'accepted', repeat with returned accepted keys
                    data = reclist[0].data
                    new_key, new_name, msg = self._find_current_accepted_taxon(
                        gbif_svc, data[RIIS_SPECIES.SCINAME_FLD],
                        data[RIIS_SPECIES.KINGDOM_FLD],
                        data[RIIS_SPECIES.GBIF_KEY])
                    self._add_msg(msgdict, name, msg)
                    name_count += 1

                    # Supplement all records for this name with GBIF accepted key and sciname, then write
                    for rec in reclist:
                        # Update record in dictionary nnsl_by_species with name keys
                        rec.update_data(new_key, new_name)
                        # Update dictionary nnsl_by_id with Occid keys
                        self.by_riis_id[rec.data[RIIS_SPECIES.KEY]] = rec
                        rec_count += 1

                    if (name_count % LOG.INTERVAL) == 0:
                        self._log.debug(f'*** NNSL Name {name_count} ***')

                except Exception as e:
                    self._add_msg(msgdict, name, f"Failed to read records in dict, {e}")
        except Exception as e:
            self._add_msg(msgdict, "unknown_error", f"{e}")

        self.write_resolved_riis(overwrite=True)

        return name_count, rec_count

    # ...............................................
    def write_resolved_riis(self, outfname=None, overwrite=True):
        """Write RIIS records to file.

        Args:
            outfname (str): Full path and filename for updated RIIS records.
            overwrite (bool): True to delete an existing updated RIIS file.

        Raises:
            Exception: on failure to get csv writer.

        Returns:
            count of successfully written records
        """
        msgdict = {}
        # Make sure data is present
        if not self.by_riis_id:
            raise Exception("RIIS dictionary is not present")
        else:
            keys = list(self.by_riis_id.keys())
            tstrec = self.by_riis_id[keys[0]]
            try:
                tstrec.data[NEW_GBIF_KEY_FLD]
            except KeyError:
                raise Exception("RIIS records have not been resolved to GBIF accepted taxa")

        if not outfname:
            outfname = self.gbif_resolved_riis_fname
        new_header = self.gbif_resolved_riis_header

        try:
            writer, outf = get_csv_dict_writer(
                outfname, new_header, RIIS.DELIMITER, fmode="w", overwrite=overwrite)
        except Exception:
            raise

        self._log.debug(f"Writing resolved RIIS to {outfname}")
        rec_count = 0
        try:
            for name, rec in self.by_riis_id.items():
                # write each record
                try:
                    writer.writerow(rec.data)
                    rec_count += 1
                except Exception as e:
                    print(f"Failed to write {rec.data}, {e}")
                    self._add_msg(msgdict, name, f"Failed to write {rec.data} ({e})")

        except Exception as e:
            self._add_msg(msgdict, "unknown_error", f"{e}")
        finally:
            outf.close()

        return rec_count

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
    def _fix_header(self, fname, expected_header):
        """Return a cleaned version of the header, correcting any fieldnames with illegal characters.

        Print warnings if actual fieldnames contain non-ascii characters.  Print errors
        if the actual header does not contain the same fieldnames, in the same order, as
        the expected_header.

        Args:
            fname (str): CSV data file containing header to check
            expected_header (list): list of fieldnames expected for the data file

        Returns:
             False if header has unexpected fields, otherwise a list of fieldnames from the actual header,
                stripped of non-ascii characters
        """
        with open(fname, "r", newline="") as csvfile:
            rdr = csv.reader(csvfile, delimiter=RIIS.DELIMITER)
            header = next(rdr)
        # Test header length
        fld_count = len(header)
        if fld_count != len(expected_header):
            self._log.error(ERR_SEPARATOR)
            self._log.error(
                f"[Error] Header has {fld_count} fields, != {len(expected_header)} expected")

        good_header = []
        for i in range(len(header)):
            # Test header fieldnames, correct if necessary
            good_fieldname = self._only_ascii(header[i])
            good_header.append(good_fieldname)

            if good_fieldname != expected_header[i]:
                self._log.error(ERR_SEPARATOR)
                self._log.error(
                    f"[Error] Header {header[i]} != {expected_header[i]} expected")
                return False
        return good_header


# .............................................................................
if __name__ == "__main__":
    # Test number of rows and columns in authority and species files
    pass
