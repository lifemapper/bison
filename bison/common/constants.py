"""Constants for GBIF, BISON, RIIS, and processed outputs, used across modules."""
BIG_DATA_PATH = "/home/astewart/git/bison/big_data"
DATA_PATH = "/home/astewart/git/bison/data"
INPUT_DIR = "input"
OUT_DIR = "out"
ENCODING = "utf-8"
LINENO_FLD = "LINENO"
ERR_SEPARATOR = "------------"
# Geospatial data for intersecting with points to identify state and county for points
POINT_BUFFER_RANGE = [(i / 10.0) for i in range(1, 11)]

AGGREGATOR_DELIMITER = "__"
EXTRA_CSV_FIELD = "rest_values"


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
    "SUBTRIBE", "SUBVARIETY", "SUPERCLASS", "SUPERCOHORT", "SUPERFAMILY",
    "SUPERKINGDOM", "SUPERLEGION", "SUPERORDER", "SUPERPHYLUM", "SUPERTRIBE",
    "SUPRAGENERIC_NAME", "TRIBE", "UNRANKED", "VARIETY"]
RANKS_BELOW_SPECIES = [
    "FORM", "SUBFORM",
    "FORMA_SPECIALIS",
    "SUBSPECIES",
    "SUBVARIETY", "VARIETY"]

US_STATES = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
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
    "Puerto Rico": "PR",
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


# .............................................................................
class DWC_PROCESS:
    """Process steps and associated filename postfixes indicating completion."""
    CHUNK = {"step": 0, "postfix": None, "prefix": "chunk"}
    ANNOTATE = {"step": 1, "postfix": "georiis"}
    SUMMARIZE = {"step": 2, "postfix": "summary"}
    AGGREGATE = {"step": 3, "postfix": "aggregate"}
    SEP = "_"

    @staticmethod
    def process_types():
        """Return all DWC Process types.

        Returns:
            List of all DWC_Process types.
        """
        return (
            DWC_PROCESS.CHUNK, DWC_PROCESS.ANNOTATE,
            DWC_PROCESS.SUMMARIZE, DWC_PROCESS.AGGREGATE
        )

    @staticmethod
    def get_postfix(step):
        """For a given step number, return the postfix.

        Args:
            step (int): Numerical stage of processing completed.

        Returns:
            String for filename postfix for the given step.
        """
        for pt in DWC_PROCESS.process_types():
            if pt["step"] == step:
                return pt["postfix"]
        return None

    @staticmethod
    def get_step(postfix):
        """For a given postfix, return the step number.

        Args:
            postfix (str): String appended to the end of a filename (before the
                extension) to indicate the stage of processing completed.

        Returns:
            Integer for step corresponding to the given filename postfix.
        """
        for pt in DWC_PROCESS.process_types():
            if pt["postfix"] == postfix:
                return pt["step"]
        return -1

    @staticmethod
    def get_process(postfix=None, step=None):
        """For a given postfix or step number, return the DWC_PROCESS object.

        Args:
            postfix (str): String appended to the end of a filename (before the
                extension) to indicate the stage of processing completed.
            step (int): Numerical stage of processing completed.

        Returns:
            DWC_Process type for the given filename postfix or step.
        """
        for pt in DWC_PROCESS.process_types():
            if postfix is not None and pt["postfix"] == postfix:
                return pt
            elif pt["step"] == step:
                return pt
        return None


# .............................................................................
class CONFIG_PARAM:
    """Parameter keys for CLI tool configuration files."""
    FILE = "config_file"
    IS_INPUT_DIR = "is_input_dir"
    IS_OUPUT_DIR = "is_output_dir"
    IS_INPUT_FILE = "is_input_file"
    HELP = "help"
    TYPE = "type"


# .............................................................................
class LMBISON:
    """Headers for temporary and final output files."""
    SPECIES_KEY = "species_key"
    SCIENTIFIC_NAME_KEY = "accepted_scientific_name"
    SPECIES_NAME_KEY = "species_name"
    ASSESS_KEY = "assessment"
    STATE_KEY = "state"
    COUNTY_KEY = "county"
    LOCATION_KEY = "location"
    COUNT_KEY = "count"

    INTRODUCED_SPECIES = "introduced_species"
    INVASIVE_SPECIES = "invasive_species"
    NATIVE_SPECIES = "presumed_native_species"
    TOTAL_SPECIES = "all_species"
    PCT_INTRODUCED_SPECIES = "pct_introduced_all_species"
    PCT_INVASIVE_SPECIES = "pct_invasive_all_species"
    PCT_NATIVE_SPECIES = "pct_presumed_native_species"

    INTRODUCED_OCCS = "introduced_occurrences"
    INVASIVE_OCCS = "invasive_occurrences"
    NATIVE_OCCS = "presumed_native_occurrences"
    TOTAL_OCCS = "all_occurrences"
    PCT_INTRODUCED_OCCS = "pct_introduced_all_occurrences"
    PCT_INVASIVE_OCCS = "pct_invasive_all_occurrences"
    PCT_NATIVE_OCCS = "pct_presumed_native_occurrences"

    ASSESS_VALUES = ("introduced", "invasive", "presumed_native")
    # temporary summary of an annotated DwC file (subset/chunk of data)
    SUMMARY_FILE = [LOCATION_KEY, SPECIES_KEY, SPECIES_NAME_KEY, COUNT_KEY]
    # output summary of region
    REGION_FILE = [
        SPECIES_KEY, SCIENTIFIC_NAME_KEY, SPECIES_NAME_KEY, COUNT_KEY, ASSESS_KEY]
    # output summary of all data
    GBIF_RIIS_SUMMARY_FILE = [
        STATE_KEY, COUNTY_KEY,
        INTRODUCED_SPECIES, INVASIVE_SPECIES, NATIVE_SPECIES, TOTAL_SPECIES,
        PCT_INTRODUCED_SPECIES, PCT_INVASIVE_SPECIES, PCT_NATIVE_SPECIES,
        INTRODUCED_OCCS, INVASIVE_OCCS, NATIVE_OCCS, TOTAL_OCCS,
        PCT_INTRODUCED_OCCS, PCT_INVASIVE_OCCS, PCT_NATIVE_OCCS]


# Append these to RIIS data for GBIF accepted taxon resolution
# .............................................................................
class APPEND_TO_RIIS:
    """New fields to add to RIIS records for GBIF accepted taxa and key values."""
    GBIF_KEY = "gbif_res_taxonkey"
    GBIF_SCINAME = "gbif_res_scientificName"


# .............................................................................
class APPEND_TO_DWC:
    """New fields to add to DwC data for geospatial and RIIS attributes."""
    RESOLVED_CTY = "georef_cty"
    RESOLVED_ST = "georef_st"
    RIIS_KEY = "riis_occurrence_id"
    RIIS_ASSESSMENT = "riis_assessment"
    AIANNH_NAME = "aiannh_name"
    AIANNH_GEOID = "aiannh_geoid"
    PAD_NAME = "pad_unit_name"
    PAD_MGMT = "pad_mgmt_name"
    PAD_GAP_STATUS = "GAP_Sts"
    PAD_GAP_STATUS_DESC = "d_GAP_Sts"
    DOI_REGION = "doi_region"
    # FILTER_FLAG = "do_summarize"

    @staticmethod
    def annotation_fields():
        """All fields added to DwC records for analysis by geospatial region.

        Returns:
            list of all the fields to be added to the annotated Dwc occurrence file.
        """
        return (
            APPEND_TO_DWC.RESOLVED_CTY, APPEND_TO_DWC.RESOLVED_ST,
            APPEND_TO_DWC.RIIS_KEY, APPEND_TO_DWC.RIIS_ASSESSMENT,
            APPEND_TO_DWC.AIANNH_GEOID, APPEND_TO_DWC.AIANNH_NAME,
            APPEND_TO_DWC.PAD_NAME, APPEND_TO_DWC.PAD_MGMT,
            APPEND_TO_DWC.PAD_GAP_STATUS, APPEND_TO_DWC.PAD_GAP_STATUS_DESC,
            APPEND_TO_DWC.DOI_REGION,
            # APPEND_TO_DWC.FILTER_FLAG
        )


# .............................................................................
class US_CENSUS_COUNTY:
    """File and fieldnames for census county boundary data, map to bison fieldnames."""
    FILE = "census/cb_2021_us_county_500k.shp"
    GEO_BISON_MAP = {
        "NAME": APPEND_TO_DWC.RESOLVED_CTY,
        "STUSPS": APPEND_TO_DWC.RESOLVED_ST
    }


# .............................................................................
class US_AIANNH:
    """Relative filename and fieldname map for AIANNH.

    Notes:
        American Indian/Alaska Native Areas/Hawaiian Home Lands
    """
    FILE = "census/cb_2021_us_aiannh_500k.shp"
    GEO_BISON_MAP = {
        "NAMELSAD": APPEND_TO_DWC.AIANNH_NAME,
        "GEOID": APPEND_TO_DWC.AIANNH_GEOID
    }


# .............................................................................
class US_PAD:
    """Region/relative filename and fieldname map for US Protected Areas Database."""
    FILES = [
        ("1", "PADUS3_0_Region_1_SHP/PADUS3_0Combined_Region1.shp"),
        ("6", "PADUS3_0_Region_6_SHP/PADUS3_0Combined_Region6.shp")
    ]
    GEO_BISON_MAP = {
        "Unit_Nm": APPEND_TO_DWC.PAD_NAME,
        "d_Mang_Nam": APPEND_TO_DWC.PAD_MGMT,
        "GAP_Sts": APPEND_TO_DWC.PAD_GAP_STATUS,
        "d_GAP_Sts": APPEND_TO_DWC.PAD_GAP_STATUS_DESC
    }


# .............................................................................
class US_DOI:
    """Relative filename and fieldname map for of Dept of Interior Regions."""
    FILE = "DOI_12_Unified_Regions_20180801.shp"
    GEO_BISON_MAP = {
        "REG_NUM": APPEND_TO_DWC.DOI_REGION
    }


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
    INPUT_DATA = "gbif_2023-01-26.csv"
    # 730772042 lines, 1st is header + 730772041 records
    INPUT_RECORD_COUNT = 730772042
    URL = "http://api.gbif.org/v1"
    UUID_KEY = "key"
    ID_FLD = "gbifID"
    NAME_FLD = "scientificName"
    TAXON_FLD = "taxonKey"
    RANK_FLD = "taxonRank"
    ACC_NAME_FLD = "acceptedScientificName"
    ACC_TAXON_FLD = "acceptedTaxonKey"
    STATE_FLD = "stateProvince"
    SPECIES_NAME_FLD = "species"
    SPECIES_KEY_FLD = "speciesKey"
    COUNTY_FLD = "county"
    LAT_FLD = "decimalLatitude"
    LON_FLD = "decimalLongitude"
    MATCH_FLD = "matchType"
    STATUS_FLD = "taxonomicStatus"
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
    ACCEPT_RANK_VALUES = [
        "species", "subspecies", "variety", "form", "infraspecific_name",
        "infrasubspecific_name"]
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
        return "{}/{}/".format(GBIF.URL, GBIF.DSET_KEYS["apitype"])

    @classmethod
    def ORGANIZATION_URL(cls):
        """GBIF Organization API base URL.

        Returns:
            base URL for the GBIF Organization API
        """
        return "{}/{}/".format(GBIF.URL, GBIF.ORG_KEYS["apitype"])

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


# .............................................................................
class LOG:
    """Constants for logging across the project."""
    DIR = "log"
    INTERVAL = 1000000
    # FORMAT = " ".join([
    #     "%(asctime)s",
    #     "%(funcName)s",
    #     "line",
    #     "%(lineno)d",
    #     "%(levelname)-8s",
    #     "%(message)s"])
    FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
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
class RIIS_DATA:
    """Constants for the US Register of Introduced and Invasive Species, RIIS data."""
    DATA_EXT = "csv"
    DELIMITER = ","
    QUOTECHAR = '"'
    # Metadata about fields
    DATA_DICT_FNAME = "US_RIIS_DataDictionary"
    # Authority References Metadata.
    AUTHORITY_FNAME = "US-RIIS_AuthorityReferences"
    AUTHORITY_KEY = "Authority"
    AUTHORITY_DATA_COUNT = 5951
    AUTHORITY_HEADER = [
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
    # Introduced or Invasive Species List.
    SPECIES_GEO_FNAME = "US-RIIS_MasterList.csv"
    SPECIES_GEO_TEST_FNAME = "US-RIIS_MasterList_100.csv"
    SPECIES_GEO_DATA_COUNT = 15264
    SPECIES_GEO_KEY = "occurrenceID"
    GBIF_KEY = "GBIF taxonKey"
    ITIS_KEY = "ITIS TSN"
    LOCALITY_FLD = "locality"
    KINGDOM_FLD = "kingdom"
    SCINAME_FLD = "scientificName"
    SCIAUTHOR_FLD = "scientificNameAuthorship"
    RANK_FLD = "taxonRank"
    ASSESSMENT_FLD = "Introduced or Invasive"
    TAXON_AUTHORITY_FLD = "taxonomicStatus"
    SPECIES_GEO_HEADER = [
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


# # .............................................................................
# class RIIS:
#     """Constants for the US Register of Introduced and Invasive Species, RIIS."""
#     DATA_DIR = "data"
#     DATA_EXT = "csv"
#     DELIMITER = ","
#     QUOTECHAR = '"'
#     # Metadata about fields
#     DATA_DICT_FNAME = "US_RIIS_DataDictionary"
#
#
# # .............................................................................
# class RIIS_AUTHORITY:
#     """Authority References Metadata."""
#     FNAME = "US-RIIS_AuthorityReferences"
#     KEY = "Authority"
#     DATA_COUNT = 5951
#     HEADER = [
#         "Authority",
#         "associatedReferences",
#         "Source Type",
#         "Source",
#         "Version",
#         "Reference Author",
#         "Title",
#         "Publication Name",
#         "Listed Publication Date",
#         "Publisher",
#         "Publication Place",
#         "ISBN",
#         "ISSN",
#         "Pages",
#         "Publication Remarks",
#     ]
#
#
# # .............................................................................
# class RIIS_SPECIES:
#     """Introduced or Invasive Species List."""
#     FNAME = "US-RIIS_MasterList.csv"
#     TEST_FNAME = "US-RIIS_MasterList_100.csv"
#     DATA_COUNT = 15264
#     KEY = "occurrenceID"
#     GBIF_KEY = "GBIF taxonKey"
#     ITIS_KEY = "ITIS TSN"
#     LOCALITY_FLD = "locality"
#     KINGDOM_FLD = "kingdom"
#     SCINAME_FLD = "scientificName"
#     SCIAUTHOR_FLD = "scientificNameAuthorship"
#     RANK_FLD = "taxonRank"
#     ASSESSMENT_FLD = "Introduced or Invasive"
#     TAXON_AUTHORITY_FLD = "taxonomicStatus"
#     HEADER = [
#         "locality",
#         "scientificName",
#         "scientificNameAuthorship",
#         "vernacularName",
#         "taxonRank",
#         "Introduced or Invasive",
#         "Biocontrol",
#         "associatedTaxa",
#         "Approximate Introduction Date",
#         "IntroDateNumber",
#         "Other Names",
#         "kingdom",
#         "phylum",
#         "class",
#         "order",
#         "family",
#         "taxonomicStatus",
#         "ITIS TSN",
#         "GBIF taxonKey",
#         "Authority",
#         "associatedReferences",
#         "Acquisition Date",
#         "modified",
#         "Update Remarks",
#         "occurrenceRemarks",
#         "occurrenceID",
#     ]
