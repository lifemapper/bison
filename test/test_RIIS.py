"""Test the structure, size, and content of input US-RIIS data files."""
import csv
import os

from riis.common.constants import RIIS

ERR_SEPARATOR = "------------"


class TestRIISInput:
    """Class for testing input authority and species files"""

    # .............................................................................
    def __init__(self):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing."""
        # Construct data filenames
        self.auth_fname = "{}.{}".format(
            os.path.join(RIIS.DATA_DIR, RIIS.AUTHORITY_FNAME), RIIS.DATA_EXT
        )
        self.species_fname = "{}.{}".format(
            os.path.join(RIIS.DATA_DIR, RIIS.SPECIES_FNAME), RIIS.DATA_EXT
        )

        # Test and clean headers of non-ascii characters
        self.auth_header = self._clean_header(self.auth_fname, RIIS.AUTHORITY_HEADER)
        self.species_header = self._clean_header(
            self.species_fname, RIIS.SPECIES_HEADER
        )

    # .............................................................................
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
                authorities.add(row[RIIS.AUTHORITY_KEY])
        return authorities

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
                    curr_auth = row[RIIS.AUTHORITY_KEY]
                    if curr_auth not in authorities:
                        try:
                            missing_authorities[curr_auth].append(rdr.line_num)
                        except KeyError:
                            missing_authorities[curr_auth] = [rdr.line_num]
        return missing_authorities

    # .............................................................................
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

    # .............................................................................
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
            print(
                "[Error] Header has {} fields, != {} expected count".format(
                    fld_count, len(expected_header)
                )
            )
        # Test header fieldnames
        for i in range(len(header)):
            good_fieldname = self._only_ascii(header[i])
            if header[i] != good_fieldname:
                print(ERR_SEPARATOR)
                print(
                    '[Warning] File {}, header field {} "{}" has non-ascii characters'.format(
                        fname, i, header[i]
                    )
                )

            clean_header.append(good_fieldname)
            if good_fieldname != expected_header[i]:
                print(ERR_SEPARATOR)
                print(
                    "[Error] Header fieldname {} != {} expected fieldname".format(
                        header[i], expected_header[i]
                    )
                )
        return clean_header

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
            self.auth_fname, self.auth_header, RIIS.AUTHORITY_COUNT
        )
        assert len(short_lines) == 0 and len(long_lines) == 0

    # .............................................................................
    def test_species_structure(self):
        """Test the structure of the species data file."""
        row_count, short_lines, long_lines = self._examine_structure(
            self.species_fname, self.species_header, RIIS.SPECIES_COUNT
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
if __name__ == "__main__":
    # Test number of rows and columns in authority and species files
    Tst = TestRIISInput()
    Tst.test_authority_structure()
    Tst.test_species_structure()

    # Test that authority foreign keys in species datafile are present in authorities file
    Tst.test_authority_keys()
