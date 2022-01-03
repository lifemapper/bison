"""Constants for GBIF, BISON, RIIS, and processed outputs, used across modules."""

BISON_DELIMITER = "$"
ENCODING = "utf-8"
LINENO_FLD = "LINENO"
ERR_SEPARATOR = "------------"


class GBIF:
    """Constants for GBIF APIs, and their request and response objects."""

    URL = "http://api.gbif.org/v1"
    UUID_KEY = "key"
    ORG_FOREIGN_KEY = "publishingOrganizationKey"
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

    @property
    def DATASET_URL():
        """GBIF Dataset API base URL."""
        return "{}/{}/".format(GBIF.URL, GBIF.DSET_KEYS.apitype)

    @property
    def ORGANIZATION_URL():
        """GBIF Organization API base URL."""
        return "{}/{}/".format(GBIF.URL, GBIF.ORG_KEYS.apitype)

    @property
    def BATCH_PARSER_URL():
        """GBIF batch Parser API base URL."""
        return GBIF.URL + "/parser/name/"

    @property
    def SINGLE_PARSER_URL():
        """GBIF individual parser API URL prefix."""
        return GBIF.URL + "/species/parser/name?name="

    @property
    def TAXON_URL():
        """GBIF Taxon/Species API base URL."""
        return GBIF.URL + "/species/"


# http://api.gbif.org/v1/parser/name?name=quercus%20berberidifolia
# http://api.gbif.org/v1/organization?identifier=362
# http://api.gbif.org/v1/organization/c3ad790a-d426-4ac1-8e32-da61f81f0117


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


class RIIS:
    """Constants for the US Register of Introduced and Invasive Species, US-RIIS data."""
    DATA_DIR = "data"
    DATA_EXT = "csv"
    DELIMITER = ","
    QUOTECHAR = '"'
    # Metadata about fields
    DATA_DICT_FNAME = "US-RIIS_DataDictionary"


class RIIS_AUTHORITY:
    """Authority References Metadata"""
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


class RIIS_SPECIES:
    """Introduced or Invasive Species List"""
    FNAME = "US-RIIS_MasterList"
    DATA_COUNT = 15264
    KEY = "occurrenceID"
    GBIF_KEY = "GBIF taxonKey"
    ITIS_KEY = "ITIS TSN"
    LOCALITY_FLD = "locality"
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
