"""Common class to setup test environment for US-RIIS data processing."""
import csv
import os

from bison.common.constants import (ERR_SEPARATOR, LINENO_FLD, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)
from bison.common.gbif_api import GbifAPI
from bison.common.util import get_csv_dict_writer

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
    def __init__(
            self, occ_key, kingdom, sci_name, sci_author, taxon_authority, gbif_key, itis_key, assessment, locality,
            line_num, new_gbif_key=None, new_gbif_name=None):
        """Construct a small record to hold relevant data for a RIIS species/locality record.

        Args:
            occ_key (str): identifier for this record
            kingdom (str): kingdom for this scientific name
            sci_name (str): canonical scientific name
            sci_author (str): authorship for scientific name
            taxon_authority (str): status (Accepted) and authority to use for taxonomic resolution (ITIS or GBIF)
            gbif_key (int): GBIF TaxonKey, unique identifier for the accepted taxon
            itis_key (int): ITIS TSN, unique identifier for the accepted taxon
            assessment (str): Determination as `Introduced` or `Invasive` species for locality
            locality (str): US locality of `AK`, `HI`, or `L48` (Alaska, Hawaii, or Lower 48)
            line_num (int): line number in input data file for this record

        Raises:
            ValueError on non-integer GBIF taxonKey or non-integer ITIS TSN
        """
        # Check GBIF taxon key
        if not gbif_key:
            gbif_key = -1
        else:
            try:
                gbif_key = int(gbif_key)
            except ValueError:
                raise
        # Check ITIS TSN
        if not itis_key:
            itis_key = -1
        else:
            try:
                itis_key = int(itis_key)
            except ValueError:
                raise
        # Trim taxonomy authority
        prefix = 'Accepted '
        if not taxon_authority.startswith(prefix):
            print('taxon_authority {} in line {}'.format(taxon_authority, line_num))
        else:
            taxon_authority = taxon_authority[len(prefix):]

        self.name = standardize_name(sci_name, sci_author)
        self.data = {
            RIIS_SPECIES.KEY: occ_key,
            RIIS_SPECIES.KINGDOM_FLD: kingdom,
            RIIS_SPECIES.SCINAME_FLD: sci_name,
            RIIS_SPECIES.SCIAUTHOR_FLD: sci_author,
            RIIS_SPECIES.TAXON_AUTHORITY_FLD: taxon_authority,
            RIIS_SPECIES.GBIF_KEY: gbif_key,
            RIIS_SPECIES.ITIS_KEY: itis_key,
            RIIS_SPECIES.ASSESSMENT_FLD: assessment,
            RIIS_SPECIES.LOCALITY_FLD: locality,
            RIIS_SPECIES.NEW_GBIF_KEY: new_gbif_key,
            RIIS_SPECIES.NEW_GBIF_SCINAME_FLD: new_gbif_name,
            LINENO_FLD: line_num}

    # ...............................................
    def update_gbif_resolution(self, gbif_key, gbif_sciname):
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
class BisonRIIS:
    """Class for reading, writing, comparing RIIS species data records."""

    # ...............................................
    def __init__(self, base_path):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing."""
        self._base_path = base_path
        self.auth_fname = "{}.{}".format(
            os.path.join(self._base_path, RIIS.DATA_DIR, RIIS_AUTHORITY.FNAME), RIIS.DATA_EXT
        )
        self.riis_fname = "{}.{}".format(
            os.path.join(self._base_path, RIIS.DATA_DIR, RIIS_SPECIES.FNAME), RIIS.DATA_EXT
        )
        self.test_riis_fname = "{}.{}".format(
            os.path.join(self._base_path, RIIS.DATA_DIR, RIIS_SPECIES.DEV_FNAME), RIIS.DATA_EXT
        )

        # Test and clean headers of non-ascii characters
        self.auth_header = self._clean_header(self.auth_fname, RIIS_AUTHORITY.HEADER)
        self.riis_header = self._clean_header(
            self.riis_fname, RIIS_SPECIES.HEADER
        )
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
    def read_species(self):
        """Assemble a dictionary of species records and a dictionary of records with invalid ITIS or GBIF keys.

        Returns:
            species: Dictionary of species records, with keys = scientific name and authorship,
                and values = list of streamlined records for that species
            bad_species: Dictionary of species records

        Raises:
            ValueError on bad input data
        """
        self.bad_species = {}
        self.nnsl = {}
        with open(self.riis_fname, "r", newline="") as csvfile:
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
                        rec = RIISRec(
                            row[RIIS_SPECIES.KEY], row[RIIS_SPECIES.KINGDOM_FLD], row[RIIS_SPECIES.SCINAME_FLD],
                            row[RIIS_SPECIES.SCIAUTHOR_FLD], row[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                            row[RIIS_SPECIES.GBIF_KEY], row[RIIS_SPECIES.ITIS_KEY],
                            row[RIIS_SPECIES.ASSESSMENT_FLD], row[RIIS_SPECIES.LOCALITY_FLD], lineno,
                            new_gbif_key=new_gbif_key, new_gbif_name=new_gbif_name)
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
        if gbifrec:
            sci_name_fld = "scientificName"
            try:
                # Results from species/match?name=<name>
                status = gbifrec["status"].lower()
                key_fld = "usageKey"
                accepted_key_fld = "acceptedUsageKey"
            except KeyError:
                # Results from species/<key>
                status = gbifrec["taxonomicStatus"].lower()
                key_fld = "key"
                accepted_key_fld = "nubKey"

            if status == "accepted":
                key = gbifrec[key_fld]
                name = gbifrec[sci_name_fld]
            else:
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
        """Test whether any full scientific names have more than one GBIF taxonKey"""
        msgdict = {}
        # New fields for GBIF resolution in species records

        if not self.nnsl:
            self.read_species()
        gbif_svc = GbifAPI()
        for spname, reclist in self.nnsl.items():
            # Just get name/kingdom from first record
            rec1 = reclist[0]
            gbifrec = gbif_svc.query_for_name(
                sciname=spname, kingdom=rec1.data[RIIS_SPECIES.KINGDOM_FLD])

            # Get match results
            new_key, new_name, msg = self._get_accepted_name_key(gbifrec)

            # If match results are not 'accepted', query for the returned acceptedUsageKey
            if new_key and new_name is None:
                self._add_msg(msgdict, spname, msg)
                gbifrec2 = gbif_svc.query_for_name(taxkey=new_key)
                # Replace new key and name with results of 2nd query
                new_key, new_name, msg = self._get_accepted_name_key(gbifrec2)

            # Supplement all records for this species with GBIF accepted key and name
            for sprec in reclist:
                sprec.update_gbif_resolution(new_key, new_name)
                # sprec.data[RIIS_SPECIES.NEW_GBIF_KEY] = new_key
                # sprec.data[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD] = new_name

    # ...............................................
    def write_species(self, outfname):
        """Write out species data with updated taxonKey and scientificName for current GBIF backbone taxonomy."""
        if not self.nnsl:
            self.read_species()

        # First name in keys, first record in value-list, keys from data attribute
        names = list(self.nnsl.keys())
        rec1 = self.nnsl[names[0]][0]
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
        bad = []
        for ch in name:
            if ch.isascii():
                good.append(ch)
            else:
                bad.append(ch)
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
