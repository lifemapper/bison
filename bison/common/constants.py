"""Constants for GBIF, BISON, RIIS, and processed outputs, used across modules."""

BISON_DELIMITER = "$"
ENCODING = "utf-8"
LINENO_FLD = "LINENO"
ERR_SEPARATOR = "------------"
ITIS_SOLR_URL = 'https://services.itis.gov/'
ITIS_NAME_KEY = 'nameWOInd'
ITIS_TSN_KEY = 'tsn'
ITIS_URL_ESCAPES = [[" ", r"\%20"]]
ITIS_VERNACULAR_QUERY = 'https://www.itis.gov/ITISWebService/services/ITISService/getCommonNamesFromTSN?tsn='
ITIS_NAMESPACE = '{http://itis_service.itis.usgs.gov}'
ITIS_DATA_NAMESPACE = '{http://data.itis_service.itis.usgs.gov/xsd}'


class LOG:
    """Constants for logging across the project."""
    INTERVAL = 1000000
    FORMAT = ' '.join([
        "%(asctime)s",
        "%(funcName)s",
        "line",
        "%(lineno)d",
        "%(levelname)-8s",
        "%(message)s"])
    DATE_FORMAT = '%d %b %Y %H:%M'
    FILE_MAX_BYTES = 52000000
    FILE_BACKUP_COUNT = 5


# .............................................................................
class GBIF:
    """Constants for GBIF APIs, and their request and response objects."""

    URL = "http://api.gbif.org/v1"
    UUID_KEY = "key"
    OCCID_FLD = "gbifID"
    NAME_FLD = "scientificName"
    TAXON_FLD = "taxonKey"
    MATCH_FLD = "matchType"
    STATUS_FLD = "taxonomicStatus"
    ORG_FOREIGN_KEY = "publishingOrganizationKey"
    TEST_DATA = "gbif_2022-01-07_100k.csv"
    DWCA_DATASET_DIR = "dataset"
    DWCA_META_FNAME = "meta.xml"
    DWCA_INTERPRETED = "occurrence"
    DWCA_VERBATIM = "verbatim"
    DWCA_DELIMITER = "\t"
    NAMEKEY_FIELD = "taxonKey"
    ORG_KEYS = {
        "apitype": "organization",
        "saveme": ["key", "title", "description", "created", "modified", "homepage"],
        "preserve_format": ["description", "homepage"],
    }
    DSET_KEYS = {
        "apitype": "dataset",
        "saveme": [
            "key",
            "publishingOrganizationKey",
            "title",
            "description",
            "citation",
            "rights",
            "logoUrl",
            "created",
            "modified",
            "homepage",
        ],
        "preserve_format": ["title", "rights", "logoUrl", "description", "homepage"],
    }
    TERM_CONVERT = {
        "FOSSIL_SPECIMEN": "fossil",
        "LITERATURE": "literature",
        "LIVING_SPECIMEN": "living",
        "HUMAN_OBSERVATION": "observation",
        "MACHINE_OBSERVATION": "observation",
        "OBSERVATION": "observation",
        "MATERIAL_SAMPLE": "specimen",
        "PRESERVED_SPECIMEN": "specimen",
        "UNKNOWN": "unknown",
    }
    SUBSET_PREFIX = "_lines_"
    SUBSET = "0-5000"

    @classmethod
    def DATASET_URL(cls):
        """GBIF Dataset API base URL.

        Returns:
            base URL for the GBIF Dataset API
        """
        return "{}/{}/".format(GBIF.URL, GBIF.DSET_KEYS.apitype)

    @classmethod
    def ORGANIZATION_URL(cls):
        """GBIF Organization API base URL.

        Returns:
            base URL for the GBIF Organization API
        """
        return "{}/{}/".format(GBIF.URL, GBIF.ORG_KEYS.apitype)

    @classmethod
    def BATCH_PARSER_URL(cls):
        """GBIF batch Parser API base URL.

        Returns:
            base URL for the GBIF Batch Name Parser API
        """
        return GBIF.URL + "/parser/name/"

    @classmethod
    def SINGLE_PARSER_URL(cls):
        """GBIF individual parser API URL prefix.

        Returns:
            base URL for the GBIF Name Parser API
        """
        return GBIF.URL + "/species/parser/name?name="

    @classmethod
    def TAXON_URL(cls):
        """GBIF Taxon/Species API base URL.

        Returns:
            base URL for the GBIF Species API
        """
        return GBIF.URL + "/species/"

    @classmethod
    def FUZZY_TAXON_URL(cls):
        """GBIF Taxon/Species API base URL.

        Returns:
            base URL for the GBIF Species Match API
        """
        return GBIF.URL + "/species/match"

# http://api.gbif.org/v1/parser/name?name=quercus%20berberidifolia
# http://api.gbif.org/v1/organization?identifier=362
# http://api.gbif.org/v1/organization/c3ad790a-d426-4ac1-8e32-da61f81f0117


# .............................................................................
class NS:
    """Biodiversity Informatics Community namespaces."""

    tdwg = "http://rs.tdwg.org/dwc/text/"
    gbif = "http://rs.gbif.org/terms/1.0/"
    eml = "eml://ecoinformatics.org/eml-2.1.1"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    dublin = "http://purl.org/dc/terms/"
    dc = "http://purl.org/dc/terms/"
    dwc = "http://rs.tdwg.org/dwc/terms/"
    gbif = "http://rs.gbif.org/terms/1.0/"


# .............................................................................
class RIIS:
    """Constants for the US Register of Introduced and Invasive Species, US-RIIS data."""
    DATA_DIR = "data"
    DATA_EXT = "csv"
    DELIMITER = ","
    QUOTECHAR = '"'
    # Metadata about fields
    DATA_DICT_FNAME = "US-RIIS_DataDictionary"


# .............................................................................
class RIIS_AUTHORITY:
    """Authority References Metadata."""
    FNAME = "US-RIIS_AuthorityReferences"
    KEY = "Authority"
    DATA_COUNT = 5951
    HEADER = [
        "Authority",
        "associatedReferences",
        "Source Type",
        "Source",
        "Version",
        "Reference Author",
        "Title",
        "Publication Name",
        "Listed Publication Date",
        "Publisher",
        "Publication Place",
        "ISBN",
        "ISSN",
        "Pages",
        "Publication Remarks",
    ]


# .............................................................................
class RIIS_SPECIES:
    """Introduced or Invasive Species List."""
    FNAME = "US-RIIS_MasterList"
    DEV_FNAME = "US-RIIS_MasterList_10"
    DATA_COUNT = 15264
    KEY = "occurrenceID"
    GBIF_KEY = "GBIF taxonKey"
    NEW_GBIF_KEY = "gbif_res_taxonkey"
    NEW_GBIF_SCINAME_FLD = "gbif_res_scientificName"
    ITIS_KEY = "ITIS TSN"
    LOCALITY_FLD = "locality"
    KINGDOM_FLD = "kingdom"
    SCINAME_FLD = "scientificName"
    SCIAUTHOR_FLD = "scientificNameAuthorship"
    ASSESSMENT_FLD = "Introduced or Invasive"
    TAXON_AUTHORITY_FLD = "taxonomicStatus"
    HEADER = [
        "locality",
        "scientificName",
        "scientificNameAuthorship",
        "vernacularName",
        "taxonRank",
        "Introduced or Invasive",
        "Biocontrol",
        "associatedTaxa",
        "Approximate Introduction Date",
        "IntroDateNumber",
        "Other Names",
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "taxonomicStatus",
        "ITIS TSN",
        "GBIF taxonKey",
        "Authority",
        "associatedReferences",
        "Acquisition Date",
        "modified",
        "Update Remarks",
        "occurrenceRemarks",
        "occurrenceID",
    ]
