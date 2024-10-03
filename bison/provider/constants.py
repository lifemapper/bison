"""Constants used to annotate USGS RIIS data with GBIF accepted taxon names and keys."""
LINENO_FLD = "LINENO"

RIIS_BASENAME = "USRIISv2_MasterList"
INPUT_RIIS_FILENAME = f"./data/{RIIS_BASENAME}.csv"


# .............................................................................
class APPEND_TO_RIIS:
    """New fields to add to RIIS records for GBIF accepted taxa and key values."""
    GBIF_KEY = "gbif_res_taxonkey"
    GBIF_SCINAME = "gbif_res_scientificName"


# .............................................................................
class RIIS_DATA:
    """Constants for the US Register of Introduced and Invasive Species, RIIS data."""
    DATA_EXT = "csv"
    DELIMITER = ","
    QUOTECHAR = '"'
    # RIIS Identifier
    RIIS_ID = "occurrenceID"
    # USGS resolution of GBIF accepted taxon (may be obsolete)
    GBIF_KEY = "GBIF_taxonKey"
    # ITIS_KEY = "ITIS_TSN"
    # Region: AK, HI, L48
    LOCALITY_FLD = "locality"
    # Assist with taxonomic resolution
    KINGDOM_FLD = "kingdom"
    SCINAME_FLD = "scientificName"
    SCIAUTHOR_FLD = "scientificNameAuthorship"
    RANK_FLD = "taxonRank"
    # Introduced or invasive status
    ASSESSMENT_FLD = "degreeOfEstablishment"
    RIIS_STATUS = {
        "established (category C3)": "established",
        "invasive (category D2)": "invasive",
        "widespread invasive (category E)": "widespread invasive"
    }


# .............................................................................
class GBIF:
    """Constants for GBIF DWCA fields, APIs, and their request and response objects."""
    URL = "http://api.gbif.org/v1"
    UUID_KEY = "key"
    ID_FLD = "gbifID"
    NAME_FLD = "scientificName"
    TAXON_FLD = "taxonKey"
    RANK_FLD = "taxonRank"
    ACC_NAME_FLD = "acceptedScientificName"
    ACC_TAXON_FLD = "acceptedTaxonKey"
    LAT_FLD = "decimalLatitude"
    LON_FLD = "decimalLongitude"
    MATCH_FLD = "matchType"
    STATUS_FLD = "taxonomicStatus"
    ORG_FOREIGN_KEY = "publishingOrganizationKey"
    NAMEKEY_FLD = "taxonKey"

    @classmethod
    def DATASET_URL(cls):
        """GBIF Dataset API base URL.

        Returns:
            base URL for the GBIF Dataset API
        """
        return f"{cls.URL}/dataset/"

    @classmethod
    def ORGANIZATION_URL(cls):
        """GBIF Organization API base URL.

        Returns:
            base URL for the GBIF Organization API
        """
        return f"{cls.URL}/organization/"

    @classmethod
    def TAXON_URL(cls):
        """GBIF Taxon/Species API base URL.

        Returns:
            base URL for the GBIF Species API
        """
        return f"{cls.URL}/species/"

    @classmethod
    def FUZZY_TAXON_URL(cls):
        """GBIF Taxon/Species API base URL.

        Returns:
            base URL for the GBIF Species Match API
        """
        return f"{cls.URL}/species/match"
