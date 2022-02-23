"""Constants for GBIF, BISON, RIIS, and processed outputs, used across modules."""

DATA_PATH = '/home/astewart/git/bison/data'
BISON_DELIMITER = "$"
ENCODING = "utf-8"
LINENO_FLD = "LINENO"
ERR_SEPARATOR = "------------"
NEW_RIIS_KEY_FLD = "riis_occurrence_id"
NEW_RIIS_ASSESSMENT_FLD = "riis_assessment"

RANKS = [
    "ABERRATION", "CLASS", "COHORT", "CONVARIETY", "CULTIVAR", "CULTIVAR_GROUP",
    "FAMILY", "FORM", "FORMA_SPECIALIS", "GENUS", "GRANDORDER", "GREX", "INFRACLASS",
    "INFRACOHORT", "INFRAFAMILY", "INFRAGENERIC_NAME", "INFRAGENUS", "INFRAKINGDOM",
    "INFRALEGION", "INFRAORDER", "INFRAPHYLUM", "INFRASPECIFIC_NAME",
    "INFRASUBSPECIFIC_NAME", "INFRATRIBE", "KINGDOM", "LEGION", "MORPH", "NATIO",
    "ORDER", "OTHER", "PARVCLASS", "PARVORDER", "PATHOVAR", "PHAGOVAR", "PHYLUM",
    "PROLES", "RACE", "SECTION", "SERIES", "SEROVAR", "SPECIES", "SPECIES_AGGREGATE",
    "STRAIN", "SUBCLASS", "SUBCOHORT", "SUBFAMILY", "SUBFORM", "SUBGENUS", "SUBKINGDOM",
    "SUBLEGION", "SUBORDER", "SUBPHYLUM", "SUBSECTION", "SUBSERIES", "SUBSPECIES",
    "SUBTRIBE", "SUBVARIETY", "SUPERCLASS", "SUPERCOHORT", "SUPERFAMILY", "SUPERKINGDOM",
    "SUPERLEGION", "SUPERORDER", "SUPERPHYLUM", "SUPERTRIBE", "SUPRAGENERIC_NAME",
    "TRIBE", "UNRANKED", "VARIETY"]
RANKS_BELOW_SPECIES = [
    "FORM", "SUBFORM",
    "FORMA_SPECIALIS",
    "SUBSPECIES",
    "SUBVARIETY", "VARIETY"]

STATES = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

#
class US_COUNTY:
    FILE =  'cb_2020_us_county_500k',
    CENSUS_BISON_MAP = {"NAME": "census_cty", "STATE_NAME": "census_st"}

# .............................................................................
class ITIS:
    """Constants for ITIS APIs, and their request and response objects."""
    SOLR_URL = 'https://services.itis.gov/'
    NAME_KEY = 'nameWOInd'
    TSN_KEY = 'tsn'
    URL_ESCAPES = [[" ", r"\%20"]]
    VERNACULAR_QUERY = 'https://www.itis.gov/ITISWebService/services/ITISService/getCommonNamesFromTSN?tsn='
    NAMESPACE = '{http://itis_service.itis.usgs.gov}'
    DATA_NAMESPACE = '{http://data.itis_service.itis.usgs.gov/xsd}'


# .............................................................................
class GBIF:
    """Constants for GBIF DWCA fields, APIs, and their request and response objects."""
    URL = "http://api.gbif.org/v1"
    UUID_KEY = "key"
    ID_FLD = "gbifID"
    NAME_FLD = "scientificName"
    TAXON_FLD = "taxonKey"
    ACC_NAME_FLD = "acceptedScientificName"
    ACC_TAXON_FLD = "acceptedTaxonKey"
    STATE_FLD = "stateProvince"
    COUNTY_FLD = "county"
    MATCH_FLD = "matchType"
    STATUS_FLD = "taxonomicStatus"
    ORG_FOREIGN_KEY = "publishingOrganizationKey"
    TEST_DATA = "gbif_2022-02-15.csv"
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


class LOG:
    """Constants for logging across the project."""
    INTERVAL = 1000
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
    """Constants for the US Register of Introduced and Invasive Species, "RIIS data."""
    DATA_DIR = "data"
    DATA_EXT = "csv"
    DELIMITER = ","
    QUOTECHAR = '"'
    # Metadata about fields
    DATA_DICT_FNAME = "US_RIIS_DataDictionary"


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
    TEST_FNAME = "US-RIIS_MasterList_100"
    DATA_COUNT = 15264
    KEY = "occurrenceID"
    GBIF_KEY = "GBIF taxonKey"
    ITIS_KEY = "ITIS TSN"
    LOCALITY_FLD = "locality"
    KINGDOM_FLD = "kingdom"
    SCINAME_FLD = "scientificName"
    SCIAUTHOR_FLD = "scientificNameAuthorship"
    RANK_FLD = "taxonRank"
    ASSESSMENT_FLD = "Introduced or Invasive"
    TAXON_AUTHORITY_FLD = "taxonomicStatus"
    NEW_GBIF_KEY_FLD = "gbif_res_taxonkey"
    NEW_GBIF_SCINAME_FLD = "gbif_res_scientificName"
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
