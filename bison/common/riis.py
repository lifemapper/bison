"""Common classes for BISON RIIS data processing."""
import csv
import os

from bison.common.constants import (
    ERR_SEPARATOR, GBIF, LINENO_FLD, LOG, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)
from bison.tools.gbif_api import GbifSvc
from bison.tools.util import get_csv_dict_reader, get_csv_dict_writer, get_logger, logit


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
    def update_data(self, gbif_key, gbif_sciname):
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
    def __init__(self, datapath, logger=None):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            datapath (str): Path to the base of the input data, used to construct full
                filenames from basepath and relative path constants.
            logger (object): logger for writing messages to file and console

        Raises:
            Exception on unexpected file header
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

        # Test and clean headers of non-ascii characters
        self._test_header(self.auth_fname, RIIS_AUTHORITY.HEADER)
        good_header = self._test_header(self.riis_fname, RIIS_SPECIES.HEADER)
        if good_header is False:
            raise Exception("Unexpected file header found in {}".format(self.riis_fname))

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
                fieldnames=RIIS_AUTHORITY.HEADER,
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
    @property
    def gbif_resolved_riis_header(self):
        """Construct the expected header for the resolved RIIS.

        Returns:
            updated_riis_header: fieldnames for the updated file
        """
        header = RIIS_SPECIES.HEADER.copy()
        header.append(RIIS_SPECIES.NEW_GBIF_KEY)
        header.append(RIIS_SPECIES.NEW_GBIF_SCINAME_FLD)
        return header

    # ...............................................
    def read_riis_by_species(self, read_resolved=False):
        """Assemble 2 dictionaries of records with valid and invalid data.

        Args:
            read_resolved (bool): True if reading ammended RIIS data, with scientific
                names resolved to the currently accepted taxon.

        Raises:
            FileNotFoundError if read_resolved is True but resolved data file does not
                exist
            Exception on read error
        """
        self.bad_species = {}
        self.nnsl = {}
        if read_resolved is True:
            # Use species data with updated GBIF taxonomic resolution if exists
            if not os.path.exists(self.gbif_resolved_riis_fname):
                raise FileNotFoundError("File {} does not exist".format(self.gbif_resolved_riis_fname))
            else:
                infname = self.gbif_resolved_riis_fname
                header = self.gbif_resolved_riis_header
        else:
            infname = self.riis_fname
            header = RIIS_SPECIES.HEADER

        rdr, inf = get_csv_dict_reader(infname, RIIS.DELIMITER, fieldnames=header)
        try:
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    # Use GBIF taxon key for dictionary key
                    gbif_key = row[RIIS_SPECIES.GBIF_KEY]
                    # Read new gbif resolutions if they exist
                    if read_resolved is True:
                        new_gbif_key = row[RIIS_SPECIES.NEW_GBIF_KEY]
                        new_gbif_name = row[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD]
                    else:
                        new_gbif_key = new_gbif_name = None
                    # Create record of original data and optional new data
                    try:
                        rec = RIISRec(row, lineno, new_gbif_key=new_gbif_key, new_gbif_name=new_gbif_name)
                    except ValueError:
                        row[LINENO_FLD] = lineno
                        self.bad_species[lineno] = row

                    # Organize the records by scientificName for later query into GBIF
                    try:
                        self.nnsl[rec.name].append(rec)
                    except KeyError:
                        self.nnsl[rec.name] = [rec]
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
                    print("Failed to get status from alternative {}".format(alt))
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
            print('Failed to get matchType from {}'.format(gbifrec))
        else:
            if match_type != 'NONE':
                try:
                    # Results from species/match?name=<name>
                    status = gbifrec["status"].lower()
                except KeyError:
                    print('Failed to get status from {}'.format(gbifrec))
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
            msg = 'Failed to get status from {}'.format(gbifrec)
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
    def resolve_gbif_species(self, overwrite=False):
        """Resolve accepted name and key from the GBIF taxonomic backbone, add to self.nnsl.

        Args:
            overwrite (bool): True if overwrite existing file.
        """
        msgdict = {}
        self.read_riis_by_id(read_resolved=False)
        gbif_svc = GbifSvc()
        for name, reclist in self.nnsl.items():
            # Use name, key values from first record
            data = reclist[0].data
            # Try to match, if match is not 'accepted', repeat with returned accepted keys
            new_key, new_name, msg = self._find_current_accepted_taxon(
                gbif_svc, data[RIIS_SPECIES.SCINAME_FLD],
                data[RIIS_SPECIES.KINGDOM_FLD],
                data[RIIS_SPECIES.GBIF_KEY])
            self._add_msg(msgdict, name, msg)

            # Supplement all records for this species with GBIF accepted key and name
            for sprec in reclist:
                sprec.update_gbif_resolution(new_key, new_name)

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

        # if new_key != taxkey:
        #     print("File taxonKey/sciname {}/{} resolved to GBIF taxonKey {}/{}".format(
        #         taxkey, sciname, new_key, new_name))

        return new_key, new_name, msg

    # ...............................................
    def read_riis_by_id(self, read_resolved=False):
        """Assemble 2 dictionaries of records with valid and invalid data.

        Args:
            read_resolved (bool): True if reading ammended RIIS data, with scientific
                names resolved to the currently accepted taxon.

        Raises:
            FileNotFoundError if read_resolved is True but resolved data file does not
                exist
            Exception on read error
        """
        self.bad_species = {}
        self.nnsl_by_species = {}
        self.nnsl_by_id = {}
        if read_resolved is True:
            # Use species data with updated GBIF taxonomic resolution if exists
            if not os.path.exists(self.gbif_resolved_riis_fname):
                raise FileNotFoundError("File {} does not exist".format(self.gbif_resolved_riis_fname))
            else:
                infname = self.gbif_resolved_riis_fname
                header = self.gbif_resolved_riis_header
        else:
            infname = self.riis_fname
            header = RIIS_SPECIES.HEADER

        rdr, inf = get_csv_dict_reader(infname, RIIS.DELIMITER, fieldnames=header)
        try:
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    # Read new gbif resolutions if they exist
                    if read_resolved is True:
                        new_gbif_key = row[RIIS_SPECIES.NEW_GBIF_KEY]
                        new_gbif_name = row[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD]
                    else:
                        new_gbif_key = new_gbif_name = None
                    # Create record of original data and optional new data
                    try:
                        rec = RIISRec(row, lineno, new_gbif_key=new_gbif_key, new_gbif_name=new_gbif_name)
                    except ValueError:
                        row[LINENO_FLD] = lineno
                        self.bad_species[lineno] = row

                    # Use RIIS occurrenceID for dictionary key
                    self.nnsl_by_species[row[RIIS_SPECIES.KEY]] = rec
        except Exception as e:
            raise(e)
        finally:
            inf.close()

    # ...............................................
    def resolve_write_gbif_taxa(self, outfname=None, overwrite=True):
        """Resolve accepted name and key from the GBIF taxonomic backbone, write to file.

        Args:
            outfname (str): Full path and filename for updated RIIS records.
            overwrite (bool): True to delete an existing updated RIIS file.

        Raises:
            Exception: on failure to get csv writer.
        """
        msgdict = {}
        if not outfname:
            outfname = self.gbif_resolved_riis_fname
        try:
            writer, outf = get_csv_dict_writer(
                outfname, self.gbif_resolved_riis_header, RIIS.DELIMITER, fmode="w", overwrite=overwrite)
        except Exception:
            raise

        name_count = 0
        gbif_svc = GbifSvc()
        try:
            for key_or_name, reclist in self.nnsl.items():
                name_count += 1
                # Resolve each name, update each record (1-3) for that name
                try:
                    # Try to match, if match is not 'accepted', repeat with returned accepted keys
                    data = reclist[0].data
                    new_key, new_name, msg = self._find_current_accepted_taxon(
                        gbif_svc, data[RIIS_SPECIES.SCINAME_FLD],
                        data[RIIS_SPECIES.KINGDOM_FLD],
                        data[RIIS_SPECIES.GBIF_KEY])
                    self._add_msg(msgdict, key_or_name, msg)

                    # Supplement all records for this name with GBIF accepted key and sciname, then write
                    for rec in reclist:
                        rec.update_data(new_key, new_name)
                        # then write records
                        try:
                            writer.writerow(rec.data)
                        except Exception as e:
                            print("Failed to write {}, {}".format(rec.data, e))
                            self._add_msg(msgdict, key_or_name, 'Failed to write record {} ({})'.format(rec.data, e))

                    if (name_count % LOG.INTERVAL) == 0:
                        logit(self._log, '*** NNSL Name {} ***'.format(name_count))

                except Exception as e:
                    self._add_msg(
                        msgdict, key_or_name, 'Failed to read records in dict, {}'.format(e))
        except Exception as e:
            self._add_msg(msgdict, 'unknown_error', '{}'.format(e))
        finally:
            outf.close()

    # ...............................................
    def write_species(self, outfname):
        """Write species data to a CSV file.

        Args:
            outfname (str): full path and filename for output file.
        """
        # First name in keys, first record in value-list, keys from data attribute
        name = list(self.nnsl.keys())[0]
        rec1 = self.nnsl[name][0]
        header = rec1.data.keys()

        writer, outf = get_csv_dict_writer(outfname, header, RIIS.DELIMITER, fmode="w")
        try:
            for reclist in self.nnsl.values():
                for rec in reclist:
                    try:

                        writer.writerow(rec.data)
                    except Exception as e:
                        logit(self._log, "Failed to write {}, {}".format(rec.data, e))
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
    def _test_header(self, fname, expected_header):
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
        success = True
        with open(fname, "r", newline="") as csvfile:
            rdr = csv.reader(csvfile, delimiter=RIIS.DELIMITER)
            header = next(rdr)
        # Test header length
        fld_count = len(header)
        if fld_count != len(expected_header):
            logit(self._log, ERR_SEPARATOR)
            logit(self._log, "[Error] Header has {} fields, != {} expected count".format(
                fld_count, len(expected_header))
            )
        for i in range(len(header)):
            # Test header fieldnames
            good_fieldname = self._only_ascii(header[i])
            if good_fieldname != expected_header[i]:
                success = False
                logit(self._log, ERR_SEPARATOR)
                logit(self._log, "[Error] Header fieldname {} != {} expected fieldname".format(
                    header[i], expected_header[i])
                )
        return success


# .............................................................................
if __name__ == "__main__":
    # Test number of rows and columns in authority and species files
    pass
