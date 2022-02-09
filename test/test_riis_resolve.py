"""Test the GBIF and ITIS taxonomic resolution provided in the US-RIIS table."""
from bison.common.constants import (DATA_PATH, ERR_SEPARATOR, LINENO_FLD, RIIS_SPECIES)
from bison.common.riis import NNSL
from bison.tools.util import logit, get_logger


class TestRIISTaxonomy(NNSL):
    """Class for testing input authority and species files."""

    # .............................................................................
    def __init__(self, base_path, test_fname=None, logger=None):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing.

        Args:
            base_path (str): base file path for project execution
            test_fname (str): RIIS file with fewer records for testing
            logger (object): logger for writing messages to file and console
        """
        NNSL.__init__(self, base_path, test_fname=test_fname, logger=logger)

    # ...............................................
    def test_taxonomy_keys(self):
        """Test whether any records contain non-integer GBIF taxonKeys or ITIS TSNs."""
        logit(self._log, "*** test_taxonomy_keys ***")
        if self.bad_species is None:
            self.read_riis(read_resolved=False)
        for k, v in self.bad_species.items():
            logit(self._log, "{} {}".format(k, v))
        assert len(self.bad_species) == 0

    # ...............................................
    def test_duplicate_name_localities(self):
        """Test whether any full scientific names have more than one record for a locality."""
        logit(self._log, "*** test_duplicate_name_localities ***")
        err_msgs = []
        if self.nnsl_by_species is None:
            self.read_riis(read_resolved=False)

        for sciname, reclist in self.nnsl_by_species.items():
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
        logit(self._log, "*** test_gbif_resolution_inconsistency ***")
        err_msgs = []
        if self.nnsl_by_species is None:
            self.read_riis(read_resolved=False)
        for sciname, reclist in self.nnsl_by_species.items():
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
        logit(self._log, "*** test_missing_taxon_authority_resolution ***")
        err_msgs = []
        if self.nnsl_by_species is None:
            self.read_riis(read_resolved=False)
        for sciname, reclist in self.nnsl_by_species.items():
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
            logit(self._log, ERR_SEPARATOR)
            logit(self._log, "--- {} ---".format(header))
            for msg in msgs:
                logit(self._log, msg)

    # ...............................................
    def test_itis_resolution_inconsistency(self):
        """Test whether any full scientific names have more than one ITIS TSN."""
        logit(self._log, "*** test_itis_resolution_inconsistency ***")
        err_msgs = []
        if self.nnsl_by_species is None:
            self.read_riis(read_resolved=False)
        for sciname, reclist in self.nnsl_by_species.items():
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
    def test_resolve_gbif(self):
        """Record changed GBIF taxonomic resolutions and write updated records."""
        logit(self._log, "*** test_resolve_gbif ***")
        err_msgs = []
        self.read_riis(read_resolved=False)

        # Update species data
        self._print_errors("Re-resolve to accepted GBIF taxon", err_msgs)
        self.resolve_riis_to_gbif_taxa(overwrite=True)

        # Find mismatches
        for key, reclist in self.nnsl_by_species.items():
            rec1 = reclist[0]
            try:
                rec1.data[RIIS_SPECIES.NEW_GBIF_KEY]
            except KeyError:
                logit(self._log, 'Failed to add field {} to {} records'.format(
                    RIIS_SPECIES.NEW_GBIF_KEY, rec1.name))
            else:
                if not rec1.consistent_gbif_resolution():
                    msg = "Record {} old GBIF taxonKey {} / {} conflicts with new GBIF taxonKey {} / {}".format(
                        key,
                        rec1.data[RIIS_SPECIES.GBIF_KEY],
                        rec1.data[RIIS_SPECIES.SCINAME_FLD],

                        rec1.data[RIIS_SPECIES.NEW_GBIF_KEY],
                        rec1.data[RIIS_SPECIES.NEW_GBIF_SCINAME_FLD])
                    err_msgs.append(msg)

    # ...............................................
    def test_resolution_output(self, is_test=True):
        """Record changed GBIF taxonomic resolutions and write updated records.

        Args:
            is_test (bool): True if testing smaller test data file.
        """
        logit(self._log, "*** test_resolution_output ***")
        # Re-read original data
        self.read_riis(read_resolved=False)

        # resolved data
        test_fname = None
        if is_test:
            test_fname = RIIS_SPECIES.TEST_FNAME
        resolved_nnsl = NNSL(DATA_PATH, test_fname=test_fname)
        resolved_nnsl.read_riis(read_resolved=True)

        orig_rec_count = 0
        res_rec_count = 0
        # Find in original
        for occid in self.nnsl_by_id.keys():
            orig_rec_count += 1
            # Find in resolved
            try:
                resolved_nnsl.nnsl_by_id[occid]
            except KeyError:
                logit(
                    self._log,
                    "Failed to find occurrenceID {} in resolved dictionary".format(occid))
            else:
                res_rec_count += 1

        if orig_rec_count != res_rec_count:
            logit(self._log, "Original records {}, updated records {}".format(
                orig_rec_count, res_rec_count))

    # ...............................................
    def test_missing_resolved_records(self, is_test=True):
        """Read the original and updated RIIS records and find missing records in the updated file.

        Args:
            is_test (bool): True if testing smaller test data file.
        """
        logit(self._log, "*** test_missing_resolved_records ***")
        # Re-read original data
        self.read_riis(read_resolved=False)

        # resolved data
        test_fname = None
        if is_test:
            test_fname = RIIS_SPECIES.TEST_FNAME
        resolved_nnsl = NNSL(DATA_PATH, test_fname=test_fname)
        resolved_nnsl.read_riis(read_resolved=True)

        # Count originals
        for occid in self.nnsl_by_id.keys():
            try:
                resolved_nnsl.nnsl_by_id[occid]
            except KeyError:
                logit(self._log, "Missing record {}".format(occid))


# .............................................................................
if __name__ == "__main__":
    logger = get_logger(DATA_PATH)

    tt = TestRIISTaxonomy(DATA_PATH, logger=logger)
    tt.test_missing_taxon_authority_resolution()
    tt.test_taxonomy_keys()
    tt.test_duplicate_name_localities()
    tt.test_gbif_resolution_inconsistency()
    tt.test_itis_resolution_inconsistency()
    tt = None

    # These overwrite resolved test data RIIS species w/ 100 records
    tt = TestRIISTaxonomy(DATA_PATH, test_fname=RIIS_SPECIES.TEST_FNAME, logger=logger)
    tt.test_resolve_gbif()
    tt.test_resolution_output(is_test=True)
    tt.test_missing_resolved_records(is_test=True)
    tt = None

    # # Resolving all the data (> 12K species) takes ~ 1-3 hours
    # tt = TestRIISTaxonomy(DATA_PATH, logger=logger)
    # tt.test_resolve_gbif()
    # tt.test_resolution_output(is_test=False)
    # tt.test_missing_resolved_records(is_test=False)

"""
from test.test_riis_resolve import *

tt = TestRIISTaxonomy(DATA_PATH)
tt.test_missing_taxon_authority_resolution()
tt.test_taxonomy_keys()
tt.test_duplicate_name_localities()
tt.test_gbif_resolution_inconsistency()
tt.test_itis_resolution_inconsistency()
tt = None

# These overwrite resolved test data RIIS species w/ 100 records
tt = TestRIISTaxonomy(DATA_PATH, test_fname=RIIS_SPECIES.TEST_FNAME)
tt.test_resolve_gbif()
tt.test_resolution_output(is_test=True)
tt.test_missing_resolved_records(is_test=True)
tt = None

# Resolve all the data (12K records) takes ~ 3 hours
tt = TestRIISTaxonomy(DATA_PATH)
tt.test_resolve_gbif()
tt.test_resolution_output(is_test=False)
tt.test_missing_resolved_records(is_test=False)
"""
