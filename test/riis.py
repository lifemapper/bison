"""Common class to setup test environment for US-RIIS data processing."""
import csv
import os

from bison.common.constants import (ERR_SEPARATOR, LINENO_FLD, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)

def standardize_name(sciname, sciauthor):
    return "{} {}".format(sciname, sciauthor)


# .............................................................................
class RIISRec():
    """Class for comparing relevant fields in species data records"""
    def __init__(self, occ_key, sci_name, sci_author, taxon_authority, gbif_key, itis_key, assessment, locality, line_num):
        """

        Args:
            occ_key (str): identifier for this record
            sci_name (str): canonical scientific name
            sci_author (str): authorship for scientific name
            taxon_authority (str): status (Accepted) and authority to use for taxonomic resolution (ITIS or GBIF)
            gbif_key (int): GBIF TaxonKey, unique identifier for the accepted taxon
            itis_key (int): ITIS TSN, unique identifier for the accepted taxon
            assessment (str): Determination as `Introduced` or `Invasive` species for locality
            locality (str): US locality of `AK`, `HI`, or `L48` (Alaska, Hawaii, or Lower 48)
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
            RIIS_SPECIES.SCINAME_FLD: sci_name,
            RIIS_SPECIES.SCIAUTHOR_FLD: sci_author,
            RIIS_SPECIES.TAXON_AUTHORITY_FLD: taxon_authority,
            RIIS_SPECIES.GBIF_KEY: gbif_key,
            RIIS_SPECIES.ITIS_KEY: itis_key,
            RIIS_SPECIES.ASSESSMENT_FLD: assessment,
            RIIS_SPECIES.LOCALITY_FLD: locality,
            LINENO_FLD: line_num}

    # ...............................................
    def is_name_match(self, rrec):
        """Name fields equal"""
        return (self.data[RIIS_SPECIES.SCINAME_FLD] == rrec.data[RIIS_SPECIES.SCINAME_FLD]
            and self.data[RIIS_SPECIES.SCIAUTHOR_FLD] == rrec.data[RIIS_SPECIES.SCIAUTHOR_FLD])

    # ...............................................
    def is_duplicate(self, rrec):
        """All fields equal except the occurrenceID and linenum, effectively a duplicate"""
        return (self.is_name_match(rrec)
            and self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY]
            and self.data[RIIS_SPECIES.ITIS_KEY] == rrec.data[RIIS_SPECIES.ITIS_KEY]
            and self.data[RIIS_SPECIES.ASSESSMENT_FLD] == rrec.data[RIIS_SPECIES.ASSESSMENT_FLD]
            and self.data[RIIS_SPECIES.LOCALITY_FLD] == rrec.data[RIIS_SPECIES.LOCALITY_FLD])

    # ...............................................
    def is_duplicate_locality(self, rrec):
        """All fields equal except the occurrenceID and linenum, effectively a duplicate"""
        return (self.is_name_match(rrec)
            and self.data[RIIS_SPECIES.LOCALITY_FLD] == rrec.data[RIIS_SPECIES.LOCALITY_FLD])

    # ...............................................
    def is_assessment_locality_match(self, rrec):
        """Assessment for locality match"""
        return (self.data[RIIS_SPECIES.ASSESSMENT_FLD] == rrec.data[RIIS_SPECIES.ASSESSMENT_FLD]
            and self.data[RIIS_SPECIES.LOCALITY_FLD] == rrec.data[RIIS_SPECIES.LOCALITY_FLD])

    # ...............................................
    def is_gbif_match(self, rrec):
        """Same gbif taxon resolution"""
        return (self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY])

    # ...............................................
    def is_taxauthority_match(self, rrec):
        """Same gbif taxon resolution"""
        return (self.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD] == "GBIF"
                and self.data[RIIS_SPECIES.GBIF_KEY] == rrec.data[RIIS_SPECIES.GBIF_KEY])

    # ...............................................
    def is_itis_match(self, rrec):
        """Same ITIS taxon resolution"""
        return (self.data[RIIS_SPECIES.ITIS_KEY] == rrec.data[RIIS_SPECIES.ITIS_KEY])


# .............................................................................
class BisonRIIS:
    """Class for testing input authority and species files"""

    # ...............................................
    def __init__(self, bison_pth):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing."""
        self._base_path = bison_pth
        self.auth_fname = "{}.{}".format(
            os.path.join(bison_pth, RIIS.DATA_DIR, RIIS_AUTHORITY.FNAME), RIIS.DATA_EXT
        )
        self.species_fname = "{}.{}".format(
            os.path.join(bison_pth, RIIS.DATA_DIR, RIIS_SPECIES.FNAME), RIIS.DATA_EXT
        )

        # Test and clean headers of non-ascii characters
        self.auth_header = self._clean_header(self.auth_fname, RIIS_AUTHORITY.HEADER)
        self.species_header = self._clean_header(
            self.species_fname, RIIS_SPECIES.HEADER
        )

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
        """
        bad_species = {}
        species = {}
        with open(self.species_fname, "r", newline="") as csvfile:
            rdr = csv.DictReader(
                csvfile,
                fieldnames=self.species_header,
                delimiter=RIIS.DELIMITER,
                quotechar=RIIS.QUOTECHAR,
            )
            for row in rdr:
                lineno = rdr.line_num
                if lineno > 1:
                    try:
                        rec = RIISRec(
                            row[RIIS_SPECIES.KEY], row[RIIS_SPECIES.SCINAME_FLD],
                            row[RIIS_SPECIES.SCIAUTHOR_FLD], row[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                            row[RIIS_SPECIES.GBIF_KEY], row[RIIS_SPECIES.ITIS_KEY],
                            row[RIIS_SPECIES.ASSESSMENT_FLD], row[RIIS_SPECIES.LOCALITY_FLD], lineno)
                    except ValueError:
                        row[LINENO_FLD] = lineno
                        bad_species[lineno] = row
                    else:
                        try:
                            species[rec.name].append(rec)
                        except KeyError:
                            species[rec.name] = [rec]

        return species, bad_species

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


