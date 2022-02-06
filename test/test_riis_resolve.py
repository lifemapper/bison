"""Test the GBIF and ITIS taxonomic resolution provided in the US-RIIS table."""
import os

from bison.common.constants import (DATA_PATH, ERR_SEPARATOR, LINENO_FLD, RIIS, RIIS_SPECIES)
from bison.common.riis import NNSL
from bison.tools.util import get_csv_dict_reader, ready_filename


class TestRIISTaxonomy(NNSL):
    """Class for testing input authority and species files."""

    # .............................................................................
    def __init__(self, base_path):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            base_path (str): base file path for project execution
        """
        NNSL.__init__(self, base_path)
        self.read_species()

    # ...............................................
    def test_taxonomy_keys(self):
        """Test whether any records contain non-integer GBIF taxonKeys or ITIS TSNs."""
        for k, v in self.bad_species.items():
            print(k, v)
        assert len(self.bad_species) == 0

    # ...............................................
    def test_duplicate_name_localities(self):
        """Test whether any full scientific names have more than one record for a locality."""
        err_msgs = []
        for sciname, reclist in self.nnsl.items():
            count = len(reclist)
            i = 0
            while i < count:
                j = i + 1
                while j < count:
                    rec1 = reclist[i]
                    rec2 = reclist[j]
                    if rec1.is_duplicate_locality(rec2):
                        msg = ('Sciname {} has {} on line {} and line {}'.format(
                            sciname, rec1.data[RIIS_SPECIES.LOCALITY_FLD],
                            rec1.data[LINENO_FLD], rec2.data[LINENO_FLD]))
                        err_msgs.append(msg)
                    # assert not rec1.is_duplicate_locality(rec2)
                    j += 1
                i += 1
        self._print_errors("Duplicate Name-Locality records", err_msgs)

    # ...............................................
    def test_gbif_resolution_inconsistency(self):
        """Test whether any full scientific names have more than one GBIF taxonKey."""
        err_msgs = []
        for sciname, reclist in self.nnsl.items():
            count = len(reclist)
            i = 0
            while i < count:
                j = i + 1
                while j < count:
                    rec1 = reclist[i]
                    rec2 = reclist[j]
                    if not rec1.is_gbif_match(rec2):
                        auth1 = rec1.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
                        auth2 = rec2.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
                        msg = 'Sciname {} has record1 taxon authority {}, with GBIF key {} (line {})'.format(
                            sciname, auth1, rec1.data[RIIS_SPECIES.GBIF_KEY], rec1.data[LINENO_FLD])
                        msg += ' and record2 taxon authority {}, with GBIF key {} (line {})'.format(
                            auth2, rec2.data[RIIS_SPECIES.GBIF_KEY], rec2.data[LINENO_FLD])
                        err_msgs.append(msg)
                    # assert reclist[i].is_gbif_match(reclist[j])
                    j += 1
                i += 1
        self._print_errors("GBIF taxonKey conflicts", err_msgs)

    # ...............................................
    def test_missing_taxon_authority_resolution(self):
        """Test whether any full scientific names have more than one GBIF taxonKey."""
        err_msgs = []
        for sciname, reclist in self.nnsl.items():
            for rec in reclist:
                auth = rec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
                if (auth == "Accepted GBIF" and rec.data[RIIS_SPECIES.GBIF_KEY] <= 0):
                    err_msgs.append(
                        'Sciname {} has GBIF authority with key {} (line {})'.format(
                            sciname, rec.data[RIIS_SPECIES.GBIF_KEY], rec.data[LINENO_FLD]))
                elif (auth == "Accepted ITIS" and rec.data[RIIS_SPECIES.ITIS_KEY] <= 0):
                    err_msgs.append(
                        'Sciname {} has ITIS authority with key {} (line {})'.format(
                            sciname, rec.data[RIIS_SPECIES.GBIF_KEY], rec.data[LINENO_FLD]))
        self._print_errors("Missing authority resolution", err_msgs)

    # ...............................................
    def _print_errors(self, header, msgs):
        if msgs:
            print(ERR_SEPARATOR)
            print("--- {} ---".format(header))
            for msg in msgs:
                print(msg)

    # ...............................................
    def test_itis_resolution_inconsistency(self):
        """Test whether any full scientific names have more than one ITIS TSN."""
        err_msgs = []
        for sciname, reclist in self.nnsl.items():
            count = len(reclist)
            i = 0
            while i < count:
                j = i + 1
                while j < count:
                    rec1 = reclist[i]
                    rec2 = reclist[j]
                    if not rec1.is_itis_match(rec2):
                        auth1 = rec1.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
                        auth2 = rec2.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
                        msg = 'Sciname {} has record1 taxon authority {}, with ITIS key {} (line {})'.format(
                            sciname, auth1, rec1.data[RIIS_SPECIES.ITIS_KEY], rec1.data[LINENO_FLD])
                        msg += ' and record2 taxon authority {}, with ITIS key {} (line {})'.format(
                            auth2, rec2.data[RIIS_SPECIES.ITIS_KEY], rec2.data[LINENO_FLD])
                        err_msgs.append(msg)
                    # assert reclist[i].is_itis_match(reclist[j])
                    j += 1
                i += 1
        self._print_errors("ITIS tsn conflicts", err_msgs)

    # ...............................................
    def test_resolve_gbif(self, test_fname=None):
        """Record changed GBIF taxonomic resolutions and write updated records.

        Args:
            test_fname (str): full filename for testing input RIIS file.
        """
        err_msgs = []

        if test_fname is not None:
            # Clear species data, switch to test data, read
            self.nnsl = {}
            production_species_fname = self.riis_fname
            self.riis_fname = "{}.{}".format(
                os.path.join(self._datapath, test_fname), RIIS.DATA_EXT
            )

            # Delete existing resolved test data
            overwrite = True
            if ready_filename(self.gbif_resolved_riis_fname, overwrite=overwrite):
                self.read_species()
        else:
            overwrite = False

        # Update species data
        self._print_errors("Re-resolve to accepted GBIF taxon", err_msgs)
        self.resolve_write_gbif_taxa(overwrite=overwrite)

        # Find mismatches
        for key, reclist in self.nnsl.items():
            rec1 = reclist[0]
            try:
                rec1.data[RIIS_SPECIES.NEW_GBIF_KEY]
            except KeyError:
                print('Failed to add field {} to {} records'.format(
                    RIIS_SPECIES.NEW_GBIF_KEY, rec1.name))
            else:
                if not rec1.consistent_gbif_resolution():
                    msg = "File key {} GBIF taxonKey {} / {} conflicts with API GBIF taxonKey {} / {}".format(
                        key,
                        rec1.data[RIIS_SPECIES.GBIF_KEY],
                        rec1.data[RIIS_SPECIES.SCINAME_FLD],

                        rec1.data[RIIS_SPECIES.NEW_GBIF_KEY],
                        rec1.data[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD])
                    err_msgs.append(msg)

        if test_fname is not None:
            # Switch back to production species data
            self.riis_fname = production_species_fname
            self.read_species()

    # ...............................................
    def test_resolution_output(self, test_fname=None):
        """Record changed GBIF taxonomic resolutions and write updated records.

        Args:
            test_fname (str): full filename for testing input RIIS file.
        """
        nnsl2 = NNSL(DATA_PATH)
        nnsl2.read_species()

        # Count originals
        orig_rec_count = 0
        orig_key_count = 0
        for _key, reclist in self.nnsl.items():
            orig_key_count += 1
            for _rec in reclist:
                orig_rec_count += 1

            # Count updated
            upd_rec_count = 0
            upd_key_count = 0
            for _key, reclist in nnsl2.nnsl.items():
                upd_key_count += 1
                for _rec in reclist:
                    upd_rec_count += 1

        if orig_rec_count != upd_rec_count:
            print("Original records {}, updated records {}".format(
                orig_rec_count, upd_rec_count))
        if orig_key_count != upd_key_count:
            print("Original taxa {}, updated taxa {}".format(
                orig_key_count, upd_key_count))

    # ...............................................
    def test_missing_resolved_records(self):
        """Read the original and updated RIIS records and find missing records in the updated file."""
        orig_fname = self.riis_fname
        upd_fname = self.gbif_resolved_riis_fname

        orig_rdr, origf = get_csv_dict_reader(orig_fname, RIIS.DELIMITER, fieldnames=self.riis_header)
        upd_rdr, updf = get_csv_dict_reader(upd_fname, RIIS.DELIMITER, fieldnames=self.riis_header)
        finished = False
        orec = orig_rdr.next()
        urec = upd_rdr.next()
        try:
            while not finished:
                if orec is not None and urec is not None:
                    do_match = orec[RIIS_SPECIES.KEY] == urec[RIIS_SPECIES.KEY]
                    if do_match:
                        pass
                    else:
                        while not do_match:
                            print("Failed to match original to updated on line {}".format(orig_rdr.line_num))
                            # move to next orec
                            orec = orig_rdr.next()
                            if orec is not None:
                                do_match = orec[RIIS_SPECIES.KEY] == urec[RIIS_SPECIES.KEY]
                            else:
                                finished = True
                                do_match = True
                orec = orig_rdr.next()
                urec = upd_rdr.next()
        except Exception:
            raise
        finally:
            origf.close()
            updf.close()




# .............................................................................
if __name__ == "__main__":
    tt = TestRIISTaxonomy(DATA_PATH)
    tt.test_missing_taxon_authority_resolution()
    tt.test_taxonomy_keys()
    tt.test_duplicate_name_localities()
    tt.test_gbif_resolution_inconsistency()
    tt.test_itis_resolution_inconsistency()
    # tt.test_resolve_gbif(test_fname=RIIS_SPECIES.TEST_FNAME)
    # tt.test_resolve_gbif()
    tt.test_resolution_output()
    tt.test_missing_resolved_records()

"""
from test.test_riis_resolve import *

tt = TestRIISTaxonomy(DATA_PATH)
tt.test_missing_taxon_authority_resolution()
tt.test_taxonomy_keys()
tt.test_duplicate_name_localities()
tt.test_gbif_resolution_inconsistency()
tt.test_itis_resolution_inconsistency()
# tt.test_resolve_gbif()
tt.test_resolution_output()
tt.test_missing_resolved_records()
"""
