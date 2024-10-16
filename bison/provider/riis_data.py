"""Common classes for BISON RIIS data processing."""
from logging import DEBUG, ERROR, WARNING
import os

from bison.common.constants import ENCODING, REPORT
from bison.common.util import (
    get_csv_dict_reader, get_csv_dict_writer, get_current_datadate_str
)

from bison.provider.constants import APPEND_TO_RIIS, GBIF, LINENO_FLD, RIIS_DATA
from bison.provider.gbif_api import GbifSvc


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
        self.data[APPEND_TO_RIIS.GBIF_KEY] = new_gbif_key
        self.data[APPEND_TO_RIIS.GBIF_SCINAME] = new_gbif_name
        self.data[LINENO_FLD] = line_num
        self.name = standardize_name(
            record[RIIS_DATA.SCINAME_FLD], record[RIIS_DATA.SCIAUTHOR_FLD])

        # Set missing GBIF or ITIS key to -1
        taxon_key = record[RIIS_DATA.GBIF_KEY]
        if not taxon_key:
            taxon_key = -1
        else:
            try:
                taxon_key = int(taxon_key)
            except ValueError:
                raise
        self.data[RIIS_DATA.GBIF_KEY] = taxon_key

    # ...............................................
    @property
    def locality(self):
        """Public property for locality value.

        Returns:
            locality value
        """
        return self.data[RIIS_DATA.LOCALITY_FLD]

    # ...............................................
    @property
    def assessment(self):
        """Public property for locality value.

        Returns:
            RIIS assessment value
        """
        return self.data[RIIS_DATA.ASSESSMENT_FLD].lower()

    # ...............................................
    @property
    def riis_id(self):
        """Public property for RIIS occurrenceID value.

        Returns:
            RIIS occurrence_id value
        """
        return self.data[RIIS_DATA.RIIS_ID]

    # ...............................................
    @property
    def gbif_taxon_key(self):
        """Public property for GBIF accepted taxon key value.

        Returns:
            latest resolved GBIF accepted taxon key value
        """
        return self.data[APPEND_TO_RIIS.GBIF_KEY]

    # ...............................................
    def update_data(self, gbif_key, gbif_sciname):
        """Update the new gbif resolution fields in the data dictionary.

        Args:
            gbif_key (int): current GBIF accepted taxonKey, in the GBIF Backbone
                Taxonomy, for a scientific name
            gbif_sciname (str):  current GBIF accepted scientific name, in the GBIF
                Backbone Taxonomy, for a scientific name
        """
        self.data[APPEND_TO_RIIS.GBIF_KEY] = gbif_key
        self.data[APPEND_TO_RIIS.GBIF_SCINAME] = gbif_sciname

    # ...............................................
    def is_name_match(self, rrec):
        """Test equality of scientific name, author and kingdom fields.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS species record

        Returns:
            True if self and rrec scientific name, author, and kingdom match.
        """
        return (self.data[RIIS_DATA.SCINAME_FLD] == rrec.data[RIIS_DATA.SCINAME_FLD]
                and self.data[RIIS_DATA.KINGDOM_FLD] == rrec.data[RIIS_DATA.KINGDOM_FLD]
                and self.data[RIIS_DATA.SCIAUTHOR_FLD] == rrec.data[RIIS_DATA.SCIAUTHOR_FLD])

    # ...............................................
    def is_duplicate(self, rrec):
        """Test equality of all fields except the occurrenceID and linenum.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec scientific name, author, kingdom, GBIF key,
                ITIS TSN, assessment, and location match.
        """
        result = (
            self.is_name_match(rrec)
            and self.data[RIIS_DATA.GBIF_KEY] == rrec.data[RIIS_DATA.GBIF_KEY]
            and self.data[RIIS_DATA.ASSESSMENT_FLD] == rrec.data[RIIS_DATA.ASSESSMENT_FLD]
            and self.data[RIIS_DATA.LOCALITY_FLD] == rrec.data[RIIS_DATA.LOCALITY_FLD]
        )
        return result

    # ...............................................
    def is_duplicate_locality(self, rrec):
        """Test equality of scientific name, author and kingdom fields and locality.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec scientific name, author, kingdom, and location match.
        """
        return (
            self.is_name_match(rrec) and
            self.data[RIIS_DATA.LOCALITY_FLD] == rrec.data[RIIS_DATA.LOCALITY_FLD]
        )

    # ...............................................
    def is_assessment_locality_match(self, rrec):
        """Test equality of assessment and locality match.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec assessment and location match.
        """
        return (
            self.data[RIIS_DATA.ASSESSMENT_FLD] == rrec.data[RIIS_DATA.ASSESSMENT_FLD]
            and self.data[RIIS_DATA.LOCALITY_FLD] == rrec.data[RIIS_DATA.LOCALITY_FLD]
        )

    # ...............................................
    def is_gbif_match(self, rrec):
        """Test equality of GBIF taxonKey.

        Args:
            rrec (bison.riis.RIISRec): object containing a USGS RIIS record

        Returns:
            True if self and rrec GBIF key match.
        """
        return (self.data[RIIS_DATA.GBIF_KEY] == rrec.data[RIIS_DATA.GBIF_KEY])

    # ...............................................
    def consistent_gbif_resolution(self):
        """Test equality of existing and new gbif taxon key resolution.

        Returns:
            True if self and rrec GBIF key match.
        """
        return (self.data[RIIS_DATA.GBIF_KEY] == self.data[APPEND_TO_RIIS.GBIF_KEY])


# .............................................................................
class RIIS:
    """Class for reading, writing, comparing RIIS species data records."""

    # ...............................................
    def __init__(self, riis_filename, logger=None):
        """Set the authority and species files and headers expected for processing.

        Args:
            riis_filename (str): Path to the RIIS datafile, with or without annotations.
            logger (bison.common.log.Logger): for writing messages to file and console

        Raises:
            FileNotFoundError: on missing datafile
        """
        # Test file
        if not os.path.exists(riis_filename):
            raise FileNotFoundError(f"File {riis_filename} does not exist")

        self._riis_filename = riis_filename
        self._header_flds = self._get_fields(riis_filename, delimiter=RIIS_DATA.DELIMITER)
        if APPEND_TO_RIIS.GBIF_KEY in self._header_flds:
            self._is_annotated = True
        else:
            self._is_annotated = False
        self._log = logger

        # Trimmed and updated Non-native Species List, built from RIIS
        self.by_taxon = None
        self.by_riis_id = None
        self.bad_species = None

    # ----------------------------------------------------
    @classmethod
    def get_annotated_riis_filename(cls, input_riis_filename, datestr):
        """Construct a filename for current annotated version of the USGS RIIS records.

        Args:
            input_riis_filename (str): full or base filename of the original RIIS data.
            datestr (str): date for current dataset processing in YYYY_MM_DD format.

        Returns:
            out_filename: full filename for the output file.
            datestr (str): date of the current dataset, in YYYY_MM_DD format

        Note:
            If only the basename is provided, the filename with extension but no path
                will be returned.
        """
        pth, fname = os.path.split(input_riis_filename)
        basename, _ = os.path.splitext(fname)
        out_filename = os.path.join(pth, f"{basename}_annotated_{datestr}.csv")
        return out_filename

    # ...............................................
    def _get_fields(self, csvfile, delimiter):
        # GBIF data and output files all has the default, utf-8, encoding
        # RIIS data is unknown
        try:
            f = open(csvfile, "r", newline="", encoding=ENCODING)
        except Exception:
            raise
        else:
            line = f.readline()
            line = line.strip()
            fields = line.split(delimiter)
        finally:
            try:
                f.close()
            except Exception:
                pass
        return fields

    # # ...............................................
    # def _read_authorities(self) -> set:
    #     """Assemble a set of unique authority identifiers for joining the MasterList.
    #
    #     Returns:
    #         Set of authority identifiers valid for use as foreign keys in related datasets.
    #     """
    #     datapath, _ = os.path.split(self._riis_filename)
    #     auth_fname = f"{os.path.join(datapath, RIIS_DATA.AUTHORITY_FNAME)}.{RIIS_DATA.DATA_EXT}"
    #     authorities = set()
    #     rdr, f = get_csv_dict_reader(auth_fname, RIIS_DATA.DELIMITER)
    #     for row in rdr:
    #         authorities.add(row[RIIS_DATA.AUTHORITY_KEY])
    #     return authorities

    # ...............................................
    # @property
    def is_annotated(self):
        """Flag indicating whether the current dataset has been annotated with GBIF.

        Returns:
            boolean value indicating whether the data in memory has been annotated.
        """
        return self._is_annotated

    # ...............................................
    def get_annotated_data_fields(self):
        """Construct the expected header for the resolved RIIS.

        Returns:
            updated_riis_header: fieldnames for the updated file
        """
        header = self._header_flds.copy()
        header.append(APPEND_TO_RIIS.GBIF_KEY)
        header.append(APPEND_TO_RIIS.GBIF_SCINAME)
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
            assessments[riis.data[RIIS_DATA.LOCALITY_FLD]] = riis.data[RIIS_DATA.ASSESSMENT_FLD]
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
                recid = riis.riis_id
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
            Exception: on unexpected, missing, or out-of-order header elements
            Exception: on unknown read error
        """
        # Reset data
        self.bad_species = {}
        self.by_taxon = {}
        self.by_riis_id = {}

        header = self._header_flds
        # if self._is_annotated is False:
        #     header = self.get_annotated_data_fields()

        rdr, inf = get_csv_dict_reader(
            self._riis_filename, RIIS_DATA.DELIMITER, fieldnames=header,
            quote_none=False)
        self._log.log(
            f"Reading RIIS from {self._riis_filename}", refname=self.__class__.__name__)
        try:
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    # Read new gbif resolutions if they exist
                    if self._is_annotated is True:
                        new_gbif_key = row[APPEND_TO_RIIS.GBIF_KEY]
                        new_gbif_name = row[APPEND_TO_RIIS.GBIF_SCINAME]
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
                    riis_id = row[RIIS_DATA.RIIS_ID]
                    self.by_riis_id[riis_id] = rec
        except Exception:
            raise
        finally:
            inf.close()
        self._log.log(
            f"Read {lineno} lines from RIIS with {len(self.by_riis_id)} unique records",
            refname=self.__class__.__name__)

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
                    self._log.log(
                        f"Failed to get status from alternative {alt}",
                        refname=self.__class__.__name__, log_level=WARNING)
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
        try:
            # Results from fuzzy match search (species/match?name=<name>)
            match_type = gbifrec[GBIF.MATCH_FLD]
        except KeyError:
            self._log.log(
                f"Failed to get matchType from {gbifrec}",
                refname=self.__class__.__name__, log_level=ERROR)
        else:
            if match_type != 'NONE':
                try:
                    # Results from species/match?name=<name>
                    status = gbifrec["status"].lower()
                except KeyError:
                    self._log.log(
                        f"Failed to get status from {gbifrec}",
                        refname=self.__class__.__name__, log_level=ERROR)
                else:
                    if status == "accepted":
                        key = gbifrec["usageKey"]
                        name = gbifrec["scientificName"]
                    else:
                        try:
                            key = gbifrec["acceptedUsageKey"]
                        except KeyError:
                            key, name = self._get_alternatives(gbifrec)
        return key, name

    # ...............................................
    def _get_accepted_name_key_from_get(self, gbifrec):
        name = None
        key = None
        try:
            # Results from species/<key>
            status = gbifrec[GBIF.STATUS_FLD].lower()
        except KeyError:
            self._log.log(
                f"Failed to get status from {gbifrec}", refname=self.__class__.__name__,
                log_level=ERROR)
        else:
            if status == "accepted":
                key = gbifrec["key"]
                name = gbifrec["scientificName"]
            else:
                key = gbifrec["acceptedKey"]
                name = gbifrec["accepted"]
        return key, name

    # ...............................................
    def _add_msg(self, msgdict, key, msg):
        if msg:
            try:
                msgdict[key].append(msg)
            except KeyError:
                msgdict[key] = [msg]

    # ...............................................
    def _find_current_accepted_taxon(self, gbif_svc, sciname, kingdom):
        # Query GBIF with the name/kingdom
        gbifrec = gbif_svc.query_by_name(sciname, kingdom=kingdom)

        # Interpret match results
        new_key, new_name = self._get_accepted_name_key_from_match(gbifrec)

        if new_name is None:
            # Try again, query GBIF with the returned acceptedUsageKey (new_key)
            if new_key is not None and new_name is None:
                gbifrec2 = gbif_svc.query_by_namekey(taxkey=new_key)
                # Replace new key and name with results of 2nd query
                new_key, new_name = self._get_accepted_name_key_from_get(gbifrec2)

        return new_key, new_name

    # ...............................................
    def resolve_riis_to_gbif_taxa(self, output_filename, overwrite=False):
        """Annotate RIIS records with GBIF accepted taxon name/key, write to file.

        Args:
            output_filename (str): Full path to the annotated output file.
            overwrite (bool): Flag indicating to overwrite existing resolved file.

        Returns:
            name_count (int): count of updated records
            rec_count (int): count of resolved species

        Raises:
            Exception: on unknown error in resolution
        """
        report = {}
        msgdict = {}

        if not self.by_taxon:
            self.read_riis()
        # Summary of inputs
        report[REPORT.INFILE] = self._riis_filename
        report[REPORT.RIIS_IDENTIFIER] = len(self.by_riis_id)
        report[REPORT.RIIS_TAXA] = len(self.by_taxon)

        report[REPORT.OUTFILE] = output_filename

        name_count = rec_count = out_rec_count = 0
        if overwrite is True or not os.path.exists(output_filename):
            gbif_svc = GbifSvc()
            # TODO: does resolution replace the key in by_taxon dictionary from USGS
            #  sciname with GBIF sciname?
            try:
                for name, reclist in self.by_taxon.items():
                    # Resolve each name, update each record (1-3) for that name
                    # Try to match, if match is not 'accepted', repeat with returned
                    # accepted keys
                    data = reclist[0].data
                    old_key = data[RIIS_DATA.GBIF_KEY]
                    try:
                        new_key, new_name = self._find_current_accepted_taxon(
                            gbif_svc, data[RIIS_DATA.SCINAME_FLD],
                            data[RIIS_DATA.KINGDOM_FLD])
                    except Exception as e:
                        err = f"Failed to get GBIF accepted taxon for" \
                              f" {data[RIIS_DATA.SCINAME_FLD]}, {e}"
                        self._add_msg(msgdict, name, err)
                        self._log.log(
                            err, refname=self.__class__.__name__, log_level=ERROR)
                    else:
                        if new_key != old_key:
                            self._add_msg(
                                msgdict, name,
                                f"RIIS GBIF taxonKey {old_key} <> resolved "
                                f"taxonKey {new_key}")
                        name_count += 1
                        # Annotate all records for this name with GBIF accepted key/sciname
                        for rec in reclist:
                            # Update record in dictionary riis_by_species with name keys
                            rec.update_data(new_key, new_name)
                            # Update dictionary riis_by_id with Occid keys
                            self.by_riis_id[rec.data[RIIS_DATA.RIIS_ID]] = rec
                            rec_count += 1

                        if (name_count % 1000) == 0:
                            self._log.log(
                                f"*** RIIS Name {name_count} ***",
                                refname=self.__class__.__name__)
            except Exception as e:
                self._add_msg(msgdict, "unknown_error", f"{e}")
                raise

            out_rec_count = self._write_resolved_riis(output_filename, overwrite=overwrite)

        # Use pre-resolved data
        else:
            self.__init__(output_filename, self._log)
            self.read_riis()

        # Report new or existing contents
        # report[REPORT.PROCESS] = LMBISON_PROCESS.RESOLVE["postfix"]
        # Summary of outputs
        report[REPORT.SUMMARY] = {
            REPORT.RIIS_IDENTIFIER: len(self.by_riis_id),
            REPORT.RIIS_TAXA: len(self.by_taxon),
            REPORT.RIIS_RESOLVE_FAIL: len(self.bad_species),
            REPORT.TAXA_RESOLVED: name_count,
            REPORT.RECORDS_UPDATED: rec_count,
            REPORT.RECORDS_OUTPUT: out_rec_count
        }

        return report

    # ...............................................
    def _write_resolved_riis(self, out_filename, overwrite=True):
        """Write RIIS records to file.

        Args:
            out_filename (str): Full path and output filename for annotated records.
            overwrite (bool): True to delete an existing updated RIIS file.

        Raises:
            Exception: on attempt to write unresolved records.
            Exception: on failure to get csv writer.
            Exception: on unknown record-write error.
            Exception: on unknown dictionary-read error.

        Returns:
            count of successfully written records
        """
        # outfname = BisonNameOp.get_annotated_riis_filename(self._riis_filename)
        msgdict = {}
        # Make sure data is present
        if not self.by_riis_id:
            raise Exception("RIIS dictionary is not present")
        else:
            keys = list(self.by_riis_id.keys())
            tstrec = self.by_riis_id[keys[0]]
            try:
                tstrec.data[APPEND_TO_RIIS.GBIF_KEY]
            except KeyError:
                raise Exception(
                    "RIIS records have not been resolved to GBIF accepted taxa")

        new_header = self.get_annotated_data_fields()
        try:
            writer, outf = get_csv_dict_writer(
                out_filename, new_header, RIIS_DATA.DELIMITER, fmode="w",
                overwrite=overwrite)
        except Exception:
            raise

        self._log.log(
            f"Writing resolved RIIS to {out_filename}", refname=self.__class__.__name__)
        rec_count = 0
        try:
            for name, rec in self.by_riis_id.items():
                # write each record
                try:
                    writer.writerow(rec.data)
                    rec_count += 1
                except Exception as e:
                    msg = f"Failed to write {rec.data}, {e}"
                    self._log.log(
                        msg, refname=self.__class__.__name__, log_level=ERROR)
                    self._add_msg(msgdict, name, msg)
                    raise

        except Exception as e:
            self._add_msg(msgdict, "unknown_error", f"{e}")
            raise
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


# .............................................................................
def resolve_riis_taxa(riis_filename, logger, overwrite=True):
    """Resolve and write GBIF accepted names and taxonKeys in RIIS records.

    Args:
        riis_filename (str): full filename for original RIIS data records.
        logger (object): logger for saving relevant processing messages
        overwrite (bool): True to delete an existing updated RIIS file.

    Returns:
        dictionary report of metadata

    Raises:
        Exception: on error in RIIS.resolve_riis_to_gbif_taxa
    """
    refname = "resolve_riis_taxa"
    report = {
        refname: {}
    }
    riis = RIIS(riis_filename, logger)
    datestr = get_current_datadate_str()
    annotated_filename = RIIS.get_annotated_riis_filename(riis_filename, datestr)
    # Update species data
    try:
        report = riis.resolve_riis_to_gbif_taxa(annotated_filename, overwrite=overwrite)
        rec_count = report[REPORT.RECORDS_UPDATED]
        name_count = report[REPORT.TAXA_RESOLVED]
        out_rec_count = report[REPORT.RECORDS_OUTPUT]
    except Exception as e:
        report[refname]["error"] = f"Failed to resolve {refname}: {e}"
        logger.log(
            f"Unexpected failure {e} in resolve_riis_taxa", refname=refname,
            log_level=ERROR)
        raise

    logger.log(
        f"Resolved {name_count} taxa in {rec_count} records in {riis_filename} to "
        f"{out_rec_count} in {report[REPORT.OUTFILE]}.",
        refname=refname)

    # Debug statements for inconsistent counts
    logger.log(
        f"by_riis_id {len(riis.by_riis_id)} ?= {rec_count} records read",
        refname=refname, log_level=DEBUG)

    logger.log(
        f"by_taxon {len(riis.by_taxon)} ?= {name_count} names resolved.",
        refname=refname, log_level=DEBUG)

    return report


# .............................................................................
__all__ = [
    "resolve_riis_taxa",
    "RIIS",
    "RIISRec",
    "standardize_name"
]
