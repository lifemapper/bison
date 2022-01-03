"""Test the GBIF and ITIS taxonomic resolution provided in the US-RIIS table"""

from bison.common.constants import (ERR_SEPARATOR, LINENO_FLD, RIIS_SPECIES)
from test.riis import BisonRIIS

class TestRIISTaxonomy(BisonRIIS):
    """Class for testing input authority and species files"""

    # .............................................................................
    def __init__(self, bison_pth):
        """Constructor sets the authority and species files and headers expected for BISON-RIIS processing."""
        BisonRIIS.__init__(self, bison_pth)
        self.species, self.bad_species = self.read_species()

    # ...............................................
    def test_taxonomy_keys(self):
        """Test whether any records contain non-integer GBIF taxonKeys or ITIS TSNs"""
        for k, v in self.bad_species.items():
            print(k, v)
        assert len(self.bad_species) == 0

    # ...............................................
    def test_duplicate_name_localities(self):
        """Test whether any full scientific names have more than one record for a locality"""
        err_msgs = []
        for sciname, reclist in self.species.items():
            count = len(reclist)
            i = 0
            while i < count:
                j = i + 1
                while j < count:
                    rec1 = reclist[i]
                    rec2 = reclist[j]
                    if rec1.is_duplicate_locality(rec2):
                        msg = ('Sciname has {} on line {} and line {}'.format(
                            sciname, rec1.data[RIIS_SPECIES.LOCALITY_FLD],
                            rec1.data[LINENO_FLD], rec2.data[LINENO_FLD]))
                        err_msgs.append(msg)
                    # assert not rec1.is_duplicate_locality(rec2)
                    j += 1
                i += 1
        if err_msgs:
            print(ERR_SEPARATOR)
            for msg in err_msgs:
                print(msg)


    # ...............................................
    def test_gbif_resolution_conflict(self):
        """Test whether any full scientific names have more than one GBIF taxonKey"""
        err_msgs = []
        for sciname, reclist in self.species.items():
            count = len(reclist)
            i = 0
            while i < count:
                j = i + 1
                while j < count:
                    rec1 = reclist[i]
                    rec2 = reclist[j]
                    if not rec1.is_gbif_match(rec2):
                        msg = 'Sciname {} has authority {} GBIF key {} (line {})'.format(
                            sciname, rec1.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                            rec1.data[RIIS_SPECIES.GBIF_KEY], rec1.data[LINENO_FLD])
                        msg += ' and authority {} GBIF key {} (line {})'.format(
                            rec2.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                            rec2.data[RIIS_SPECIES.GBIF_KEY], rec2.data[LINENO_FLD])
                        err_msgs.append(msg)
                    # assert reclist[i].is_gbif_match(reclist[j])
                    j += 1
                i += 1
        if err_msgs:
            print(ERR_SEPARATOR)
            print("--- GBIF key conflicts ---")
            for msg in err_msgs:
                print(msg)

    # ...............................................
    def test_missing_taxon_authority_resolution(self):
        """Test whether any full scientific names have more than one GBIF taxonKey"""
        err_msgs = []
        for sciname, reclist in self.species.items():
            for rec in reclist:
                auth = rec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD]
                if (auth == "GBIF" and rec.data[RIIS_SPECIES.GBIF_KEY] <= 0):
                    err_msgs.append(
                        'Sciname {} has GBIF authority with key {} (line {})'.format(
                        sciname, rec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                        rec.data[RIIS_SPECIES.GBIF_KEY], rec.data[LINENO_FLD]))
                elif (auth == "ITIS" and rec.data[RIIS_SPECIES.ITIS_KEY] <= 0):
                    err_msgs.append(
                        'Sciname {} has ITIS authority with key {} (line {})'.format(
                        sciname, rec.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                        rec.data[RIIS_SPECIES.GBIF_KEY], rec.data[LINENO_FLD]))
        if err_msgs:
            print(ERR_SEPARATOR)
            print("--- missing authority resolution ---")
            for msg in err_msgs:
                print(msg)

    # ...............................................
    def test_itis_resolution_conflict(self):
        """Test whether any full scientific names have more than one GBIF taxonKey"""
        err_msgs = []
        for sciname, reclist in self.species.items():
            count = len(reclist)
            i = 0
            while i < count:
                j = i + 1
                while j < count:
                    rec1 = reclist[i]
                    rec2 = reclist[j]
                    if not rec1.is_itis_match(rec2):
                        msg = 'Sciname {} has authority {} ITIS key {} (line {})'.format(
                            sciname, rec1.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                            rec1.data[RIIS_SPECIES.ITIS_KEY], rec1.data[LINENO_FLD])
                        msg += ' and authority {} ITIS key {} (line {})'.format(
                            rec1.data[RIIS_SPECIES.TAXON_AUTHORITY_FLD],
                            rec2.data[RIIS_SPECIES.ITIS_KEY], rec2.data[LINENO_FLD])
                        err_msgs.append(msg)
                    # assert reclist[i].is_itis_match(reclist[j])
                    j += 1
                i += 1
        if err_msgs:
            print(ERR_SEPARATOR)
            print("--- ITIS tsn conflicts ---")
            for msg in err_msgs:
                print(msg)


# .............................................................................
if __name__ == "__main__":
    bison_pth = '/home/astewart/git/bison'
    tt = TestRIISTaxonomy(bison_pth)
    tt.test_taxonomy_keys()
    tt.test_duplicate_name_localities()
    tt.test_gbif_resolution_conflict()
    tt.test_itis_resolution_conflict()
    tt.test_missing_taxon_authority_resolution()
