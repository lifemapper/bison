"""Test the structure, size, and content of input US-RIIS data files."""
import csv

from bison.common.constants import (ERR_SEPARATOR, RIIS, RIIS_AUTHORITY, RIIS_SPECIES)
from bison.common.riis import BisonRIIS


class TestRIISInput(BisonRIIS):
    """Class for testing input authority and species files"""

    # .............................................................................
    def __init__(self):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing."""
        # Construct data filenames
        BisonRIIS.__init__(self)

    # .............................................................................
    def _check_species_authorities(self):
        """Test each authority in the species file references an authority in the authorities file.

        Returns:
            Dictionary of authority identifiers (key) referenced in the species file, with
            list of values that are the line numbers in the species file where they occur.
        """
        missing_authorities = {}
        authorities = self._read_authorities()
        with open(self.species_fname, "r", newline="") as csvfile:
            rdr = csv.DictReader(
                csvfile,
                fieldnames=self.species_header,
                delimiter=RIIS.DELIMITER,
                quotechar=RIIS.QUOTECHAR,
            )
            for row in rdr:
                # Skip header
                if not rdr.line_num == 1:
                    curr_auth = row[RIIS_AUTHORITY.KEY]
                    if curr_auth not in authorities:
                        try:
                            missing_authorities[curr_auth].append(rdr.line_num)
                        except KeyError:
                            missing_authorities[curr_auth] = [rdr.line_num]
        return missing_authorities

    # .............................................................................
    def _examine_structure(self, fname, header, expected_row_count):
        """Check that datafile column length and number of rows matches the expected values.

        Print warnings, but do not fail, if the number of columns in every row, and the
        number of rows are not as expected.

        Args:
            fname (str): datafile to examine
            header (list): clean header for the datafile
            expected_row_count (int): number of data rows, not including the header,
                expected to be in the file

        Returns:
            row_count (int): Number of data rows, not counting the header, found in datafile.
            short_lines (list): List of line numbers of lines that have fewer fields than expected.
            long_lines (list): List of line numbers of lines that have more fields than expected.
        """
        expected_colcount = len(header)
        short_lines = []
        long_lines = []
        row_count = 0

        with open(fname, "r", newline="") as csvfile:
            rdr = csv.DictReader(
                csvfile,
                fieldnames=header,
                delimiter=RIIS.DELIMITER,
                quotechar=RIIS.QUOTECHAR,
            )
            for row in rdr:
                # skip header
                if not rdr.line_num == 1:
                    # Check column count matches header count
                    row_count += 1
                if len(row) < expected_colcount:
                    short_lines.append(rdr.line_num)
                elif len(row) > expected_colcount:
                    long_lines.append(rdr.line_num)
        # Warning only for changed data
        if row_count != expected_row_count:
            print(ERR_SEPARATOR)
            print(
                "[Warning] File {} found {} data rows != {} expected".format(
                    fname, row_count, expected_row_count
                )
            )
        # Print warning on incorrect number of columns
        if short_lines or long_lines:
            if short_lines:
                print(ERR_SEPARATOR)
                print(
                    "[Warning] File {}, found {} lines with fewer than {} columns: {}".format(
                        fname, len(short_lines), expected_colcount, short_lines
                    )
                )
            if long_lines:
                print(ERR_SEPARATOR)
                print(
                    "[Warning] File {}, found {} lines with more than {} columns: {}".format(
                        fname, len(long_lines), expected_colcount, long_lines
                    )
                )
        return row_count, short_lines, long_lines

    # .............................................................................
    def test_authority_structure(self):
        """Test the structure of the authority reference file."""
        row_count, short_lines, long_lines = self._examine_structure(
            self.auth_fname, self.auth_header, RIIS_AUTHORITY.COUNT
        )
        assert len(short_lines) == 0 and len(long_lines) == 0

    # .............................................................................
    def test_species_structure(self):
        """Test the structure of the species data file."""
        row_count, short_lines, long_lines = self._examine_structure(
            self.species_fname, self.species_header, RIIS_SPECIES.COUNT
        )
        assert len(short_lines) == 0 and len(long_lines) == 0

    # .............................................................................
    def test_authority_keys(self):
        """Test that all foreign authority keys in the species file exist in the authority reference file."""
        missing_authorities = self._check_species_authorities()
        if missing_authorities:
            print(ERR_SEPARATOR)
            print("[Error] Missing authority:  Line number/s in species file")
            for auth, line_nums in missing_authorities.items():
                print('"{}": {}'.format(auth, line_nums))
        assert len(missing_authorities) == 0

    # .............................................................................
    def test_species_records(self):
        """Test that all foreign authority keys in the species file exist in the authority reference file."""
        missing_authorities = self._check_species_authorities()
        if missing_authorities:
            print(ERR_SEPARATOR)
            print("[Error] Missing authority:  Line number/s in species file")
            for auth, line_nums in missing_authorities.items():
                print('"{}": {}'.format(auth, line_nums))
        assert len(missing_authorities) == 0


# .............................................................................
if __name__ == "__main__":
    # Test number of rows and columns in authority and species files
    bison_pth = '/home/astewart/git/bison'
    Tst = TestRIISInput(bison_pth)
    Tst.test_authority_structure()
    Tst.test_species_structure()

    # Test that authority foreign keys in species datafile are present in authorities file
    Tst.test_authority_keys()
