"""Common classes for BISON RIIS data processing."""
import csv
import logging
import os

from bison.common.constants import (
    ERR_SEPARATOR, GBIF, LINENO_FLD, LOG, NEW_GBIF_KEY_FLD, NEW_GBIF_SCINAME_FLD, RIIS)
from bison.providers.gbif_api import GbifSvc
from bison.common.util import get_csv_dict_reader, get_csv_dict_writer


# .............................................................................
def standardize_name(sci_name, sci_author):
    """Construct a small record to hold data for a RIIS species/locality record.

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
        """Construct a record to hold relevant data for a RIIS species/locality record.

        Args:
            record (dict): original US RIIS record
            line_num (int): line number for this record in the original US RIIS file
            new_gbif_key (int): Newly resolved GBIF TaxonKey, unique identifier for the
                accepted taxon.
            new_gbif_name (str): Newly resolved GBIF scientific name to
                match new_gbif_key.

        Raises:
            ValueError: on non-integer GBIF taxonKey or non-integer ITIS TSN
        """
        self.data = record
        self.data[NEW_GBIF_KEY_FLD] = new_gbif_key
        self.data[NEW_GBIF_SCINAME_FLD] = new_gbif_name
        self.data[LINENO_FLD] = line_num
        self.name = standardize_name(
            record[RIIS.SCINAME_FLD], record[RIIS.SCIAUTHOR_FLD])

        # Set missing GBIF or ITIS key to -1
        for fld in (RIIS.GBIF_KEY, RIIS.ITIS_KEY):
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
        # taxon_authority = record[RIIS.TAXON_AUTHORITY_FLD]
        # try:
        #     is_accepted = taxon_authority.startswith(prefix)
        # except AttributeError:
        #     pass
        # else:
        #     if is_accepted:
        #         self.data[RIIS.TAXON_AUTHORITY_FLD] = taxon_authority[start_idx:]

    # ...............................................
    @property
    def locality(self):
        """Public property for locality value.

        Returns:
            locality value
        """
        return self.data[RIIS.LOCALITY_FLD]

    # ...............................................
    @property
    def assessment(self):
        """Public property for locality value.

        Returns:
            RIIS assessment value
        """
        return self.data[RIIS.ASSESSMENT_FLD].lower()

    # ...............................................
    @property
    def occurrence_id(self):
        """Public property for RIIS occurrenceID value.

        Returns:
            RIIS occurrence_id value
        """
        return self.data[RIIS.SPECIES_GEO_KEY]

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
            gbif_sciname (str):  current GBIF accepted scientific name, in the GBIF
                Backbone Taxonomy, for a scientific name
        """
        self.data[NEW_GBIF_KEY_FLD] = gbif_key
        self.data[NEW_GBIF_SCINAME_FLD] = gbif_sciname

    # ...............................................
    def is_name_match(self, rrec):
        """Test equality of scientific name, author and kingdom fields.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS species record

        Returns:
            True if self and rrec scientific name, author, and kingdom match.
        """
        return (self.data[RIIS.SCINAME_FLD] == rrec.data[RIIS.SCINAME_FLD]
                and self.data[RIIS.KINGDOM_FLD] == rrec.data[RIIS.KINGDOM_FLD]
                and self.data[RIIS.SCIAUTHOR_FLD] == rrec.data[RIIS.SCIAUTHOR_FLD])

    # ...............................................
    def is_duplicate(self, rrec):
        """Test equality of all fields except the occurrenceID and linenum.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec scientific name, author, kingdom, GBIF key,
                ITIS TSN, assessment, and location match.
        """
        return (self.is_name_match(rrec)
                and self.data[RIIS.GBIF_KEY] == rrec.data[RIIS.GBIF_KEY]
                and self.data[RIIS.ITIS_KEY] == rrec.data[RIIS.ITIS_KEY]
                and self.data[RIIS.ASSESSMENT_FLD] == rrec.data[RIIS.ASSESSMENT_FLD]
                and self.data[RIIS.LOCALITY_FLD] == rrec.data[RIIS.LOCALITY_FLD])

    # ...............................................
    def is_duplicate_locality(self, rrec):
        """Test equality of scientific name, author and kingdom fields and locality.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec scientific name, author, kingdom, and location match.
        """
        return (self.is_name_match(rrec)
                and self.data[RIIS.LOCALITY_FLD] == rrec.data[RIIS.LOCALITY_FLD])

    # ...............................................
    def is_assessment_locality_match(self, rrec):
        """Test equality of assessment and locality match.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec assessment and location match.
        """
        return (self.data[RIIS.ASSESSMENT_FLD] == rrec.data[RIIS.ASSESSMENT_FLD]
                and self.data[RIIS.LOCALITY_FLD] == rrec.data[RIIS.LOCALITY_FLD])

    # ...............................................
    def is_gbif_match(self, rrec):
        """Test equality of GBIF taxonKey.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec GBIF key match.
        """
        return (self.data[RIIS.GBIF_KEY] == rrec.data[RIIS.GBIF_KEY])

    # ...............................................
    def is_itis_match(self, rrec):
        """Test equality of ITIS TSN.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec ITIS TSN match.
        """
        return (self.data[RIIS.ITIS_KEY] == rrec.data[RIIS.ITIS_KEY])

    # ...............................................
    def consistent_gbif_resolution(self):
        """Test equality of existing and new gbif taxon key resolution.

        Returns:
            True if self and rrec GBIF key match.
        """
        return (self.data[RIIS.GBIF_KEY] == self.data[NEW_GBIF_KEY_FLD])

    # ...............................................
    def is_taxauthority_match(self, rrec):
        """Test equality of taxon authority and authority key.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec name authority are both GBIF and GBIF keys match
            or authority are both ITIS and the ITIS TSNs match.
        """
        # Test GBIF match
        if (
                self.data[RIIS.TAXON_AUTHORITY_FLD] == "Accepted GBIF"
                and rrec.data[RIIS.TAXON_AUTHORITY_FLD] == "Accepted GBIF"
                and self.data[RIIS.GBIF_KEY] == rrec.data[RIIS.GBIF_KEY]
        ):
            return True
        # Test ITIS match
        else:
            return (
                self.data[RIIS.TAXON_AUTHORITY_FLD] == "Accepted ITIS"
                and rrec.data[RIIS.TAXON_AUTHORITY_FLD] == "Accepted ITIS"
                and self.data[RIIS.ITIS_KEY] == rrec.data[RIIS.ITIS_KEY]
            )


# .............................................................................
class NNSL:
    """Class for reading, writing, comparing RIIS species data records."""

    # ...............................................
    def __init__(self, riis_filename, is_annotated=False, logger=None):
        """Set the authority and species files and headers expected for processing.

        Args:
            riis_filename (str): Path to the RIIS datafile, with or without annotations.
            is_annotated (bool): Flag indicating whether RIIS file has been
                annotated with GBIF accepted taxon names.
            logger (object): logger for writing messages to file and console
        """
        self._riis_filename = riis_filename
        self._is_annotated = is_annotated
        self._log = logger

        datapath, _ = os.path.split(riis_filename)
        self.auth_fname = f"{os.path.join(datapath, RIIS.AUTHORITY_FNAME)}.{RIIS.DATA_EXT}"

        # Trimmed and updated Non-native Species List, built from RIIS
        self.by_taxon = None
        self.by_riis_id = None
        self.bad_species = None

    # ...............................................
    def _read_authorities(self) -> set:
        """Assemble a set of unique authority identifiers for joining the MasterList.

        Returns:
            Set of authority identifiers valid for use as foreign keys in related datasets.
        """
        authorities = set()
        rdr, f = get_csv_dict_reader(self.auth_fname, RIIS.DELIMITER)
        # with open(self.auth_fname, "r", newline="") as csvfile:
        #     rdr = csv.DictReader(
        #         csvfile,
        #         fieldnames=RIIS.AUTHORITY_HEADER,
        #         delimiter=RIIS.DELIMITER,
        #         quotechar=RIIS.QUOTECHAR,
        #     )
        for row in rdr:
            authorities.add(row[RIIS.AUTHORITY_KEY])
        return authorities

    # ...............................................
    @property
    def annotated_riis_header(self):
        """Construct the expected header for the resolved RIIS.

        Returns:
            updated_riis_header: fieldnames for the updated file
        """
        header = RIIS.SPECIES_GEO_HEADER.copy()
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
            riis_recs = self.by_taxon[gbif_taxon_key]
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
            dict of 0 or more, like
                {"AK": "introduced", "HI": "invasive", "L48": "introduced"}
                for the species with this GBIF taxonKey
        """
        assessments = {}
        riis_recs = self.get_riis_by_gbif_taxonkey(gbif_taxon_key)
        for riis in riis_recs:
            assessments[riis.data[RIIS.LOCALITY_FLD]] = riis.data[RIIS.ASSESSMENT_FLD]
        return assessments

    # ...............................................
    def get_assessment_for_gbif_taxonkeys_region(self, gbif_taxon_keys, region):
        """Get all RIIS assessments for this GBIF taxonKey.

        Args:
            gbif_taxon_keys (list of str): unique identifier for GBIF taxon record. For
                records identified to a rank lower than species, check assessment for
                species key also.
            region (str): RIIS-defined US region for assessment, choices are AK, HI, L48

        Returns:
            dict of 0 or more, like
                {"AK": "introduced", "HI": "invasive", "L48": "introduced"}
                for the species with this GBIF taxonKey
        """
        assess = "presumed_native"
        recid = None
        riis_recs = []
        for gbif_taxon_key in gbif_taxon_keys:
            riis_recs.extend(self.get_riis_by_gbif_taxonkey(gbif_taxon_key))
        for riis in riis_recs:
            if region == riis.locality:
                assess = riis.assessment
                recid = riis.occurrence_id
        return assess, recid

    # ...............................................
    def read_riis(self):
        """Assemble 2 dictionaries of records with valid and invalid data.

        Notes:
            Fills dictionary self.by_taxon with key of either the RIIS scientificName
            or the GBIF acceptedTaxonId.
            Fills dictionary self.by_riis_id with key of the unique key in RIIS data,
            occurrenceId.

        Raises:
            FileNotFoundError: on missing input filename
            Exception: on unexpected, missing, or out-of-order header elements
            Exception: on unknown read error
        """
        # Reset data
        self.bad_species = {}
        self.by_taxon = {}
        self.by_riis_id = {}

        # Test file and header
        if not os.path.exists(self._riis_filename):
            raise FileNotFoundError(f"File {self._riis_filename} does not exist")
        if self._is_annotated is True:
            expected_header = self.annotated_riis_header
        else:
            expected_header = RIIS.SPECIES_GEO_HEADER
        # Clean header of non-ascii characters
        good_header = self._clean_header(self._riis_filename, expected_header)
        if good_header is None:
            raise Exception(f"Unexpected file header found in {self._riis_filename}")

        rdr, inf = get_csv_dict_reader(
            self._riis_filename, RIIS.DELIMITER, fieldnames=good_header,
            quote_none=False)
        self._log.log(
            f"Reading RIIS from {self._riis_filename}", refname=self.__class__.__name__,
            log_level=logging.INFO)
        try:
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    # Read new gbif resolutions if they exist
                    if self._is_annotated is True:
                        new_gbif_key = row[NEW_GBIF_KEY_FLD]
                        new_gbif_name = row[NEW_GBIF_SCINAME_FLD]
                    else:
                        new_gbif_key = new_gbif_name = None
                    # Create record of original data and optional new data
                    try:
                        rec = RIISRec(
                            row, lineno, new_gbif_key=new_gbif_key,
                            new_gbif_name=new_gbif_name)
                    except ValueError:
                        row[LINENO_FLD] = lineno
                        self.bad_species[lineno] = row

                    # Index records on scientificName if not annotated;
                    # Index records on GBIF taxonkey if annotation exists;
                    # i.e. group by species for easy lookup
                    index = rec.name
                    if self._is_annotated is True and new_gbif_key is not None:
                        index = new_gbif_key
                    # Group records by matching name/taxon
                    try:
                        self.by_taxon[index].append(rec)
                    except KeyError:
                        self.by_taxon[index] = [rec]

                    # Also index on RIIS occurrenceID
                    self.by_riis_id[row[RIIS.SPECIES_GEO_KEY]] = rec

        except Exception:
            raise
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
    def resolve_riis_to_gbif_taxa(self, outfname, overwrite=True):
        """Annotate RIIS records with GBIF accepted taxon name/key, write to file.

        Args:
            outfname: full filename of an output file for the RIIS data annotated with
                GBIF accepted taxon name and key
            overwrite (bool): True to delete an existing updated RIIS file.

        Returns:
            name_count (int): count of updated records
            rec_count (int): count of resolved species
        """
        msgdict = {}

        if not self.by_taxon:
            self.read_riis()

        name_count = 0
        rec_count = 0
        gbif_svc = GbifSvc()
        try:
            for name, reclist in self.by_taxon.items():
                # Resolve each name, update each record (1-3) for that name
                try:
                    # Try to match, if match is not 'accepted', repeat with returned
                    # accepted keys
                    data = reclist[0].data
                    new_key, new_name, msg = self._find_current_accepted_taxon(
                        gbif_svc, data[RIIS.SCINAME_FLD],
                        data[RIIS.KINGDOM_FLD],
                        data[RIIS.GBIF_KEY])
                    self._add_msg(msgdict, name, msg)
                    name_count += 1

                    # Supplement all records for this name with GBIF accepted key
                    # and sciname, then write
                    for rec in reclist:
                        # Update record in dictionary nnsl_by_species with name keys
                        rec.update_data(new_key, new_name)
                        # Update dictionary nnsl_by_id with Occid keys
                        self.by_riis_id[rec.data[RIIS.SPECIES_GEO_KEY]] = rec
                        rec_count += 1

                    if (name_count % LOG.INTERVAL) == 0:
                        self._log.log(
                            f"*** NNSL Name {name_count} ***",
                            refname=self.__class__.__name__)

                except Exception as e:
                    self._add_msg(msgdict, name, f"Failed to read records in dict, {e}")
        except Exception as e:
            self._add_msg(msgdict, "unknown_error", f"{e}")

        self.write_resolved_riis(outfname, overwrite=overwrite)

        return name_count, rec_count

    # ...............................................
    def write_resolved_riis(self, outfname, overwrite=True):
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
                raise Exception(
                    "RIIS records have not been resolved to GBIF accepted taxa")

        new_header = self.annotated_riis_header
        try:
            writer, outf = get_csv_dict_writer(
                outfname, new_header, RIIS.DELIMITER, fmode="w", overwrite=overwrite)
        except Exception:
            raise

        self._log.log(
            f"Writing resolved RIIS to {outfname}", refname=self.__class__.__name__)
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
    def _clean_header(self, fname, expected_header):
        """Return a cleaned version of the header, or False if errors are not fixable.

        Correct any fieldnames with non-ascii characters, and print warnings.  Print
        errors if the actual header does not contain the same fieldnames, in the same
        order, as the expected_header.

        Args:
            fname (str): CSV data file containing header to check
            expected_header (list): list of fieldnames expected for the data file

        Returns:
             False if header has unexpected fields, otherwise a list of fieldnames from
                the actual header, stripped of non-ascii characters
        """
        with open(fname, "r", newline="") as csvfile:
            rdr = csv.reader(csvfile, delimiter=RIIS.DELIMITER)
            header = next(rdr)
        # Test header length
        fld_count = len(header)
        if fld_count != len(expected_header):
            self._log.log(
                ERR_SEPARATOR, refname=self.__class__.__name__, log_level=logging.ERROR)
            self._log.error(
                f"[Error] Header has {fld_count} fields, != {len(expected_header)} " +
                "expected", refname=self.__class__.__name__, log_level=logging.ERROR)

        good_header = []
        for i in range(len(header)):
            # Test header fieldnames, correct if necessary
            good_fieldname = self._only_ascii(header[i])
            good_header.append(good_fieldname)

            if good_fieldname != expected_header[i]:
                self._log.log(
                    ERR_SEPARATOR, refname=self.__class__.__name__,
                    log_level=logging.ERROR)
                self._log.error(
                    f"[Error] Header {header[i]} != {expected_header[i]} expected",
                    refname=self.__class__.__name__, log_level=logging.ERROR)
                good_header = None
        return good_header


# .............................................................................
def resolve_riis_taxa(riis_filename, annotated_riis_filename, logger):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for original RIIS data records.
        annotated_riis_filename (str): full filename for output RIIS data records.
        logger (object): logger for saving relevant processing messages

    Returns:
        dictionary report of metadata/
    """
    refname = "resolve_riis_taxa"
    report = {
        refname: {
            "riis_filename": riis_filename,
            "annotated_riis_filename": annotated_riis_filename
        }
    }
    nnsl = NNSL(riis_filename, annotated_riis_filename, logger=logger)
    # Update species data
    try:
        nnsl.resolve_riis_to_gbif_taxa(annotated_riis_filename, overwrite=True)
        report[refname]["riis_count"] = len(nnsl.by_riis_id)
        logger.log(
            f"Resolved {len(nnsl.by_riis_id)} taxa in {riis_filename}, next, write.",
            refname=refname)
    except Exception as e:
        report[refname]["error"] = f"Failed to resolve {refname}: {e}"
        logger.log(
            f"Unexpected failure {e} in resolve_riis_taxa", refname=refname,
            log_level=logging.ERROR)

    else:
        try:
            count = nnsl.write_resolved_riis(annotated_riis_filename, overwrite=True)
            report[refname]["annotated_count"] = count
            logger.log(f"Wrote {count} records to {annotated_riis_filename}.")
        except Exception as e:
            report[refname]["error"] = f"Failed to write {refname}: {e}"
            logger.log(
                f"Unexpected failure {e} in resolve_riis_taxa", refname=refname,
                log_level=logging.ERROR)
        else:
            if count != RIIS.SPECIES_GEO_DATA_COUNT:
                report[refname]["error"] = f"Expected {count}, resolved {count} in {refname}"
                logger.log(
                    f"Resolved {count} RIIS records, expecting " +
                    f"{RIIS.SPECIES_GEO_DATA_COUNT}", refname=refname,
                    log_level=logging.ERROR)
    return report


# .............................................................................
if __name__ == "__main__":
    # Test number of rows and columns in authority and species files
    pass