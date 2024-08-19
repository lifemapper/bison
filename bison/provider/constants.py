"""Constants used to annotate USGS RIIS data with GBIF accepted taxon names and keys."""
LINENO_FLD = "LINENO"

_data_dir = "/home/astewart/git/bison/data"
RIIS_FILENAME = f"{_data_dir}/USRIISv2_MasterList.csv"


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
    # Introduced or Invasive Species List.
    SPECIES_GEO_KEY = "occurrenceID"
    GBIF_KEY = "GBIF_taxonKey"
    ITIS_KEY = "ITIS_TSN"
    LOCALITY_FLD = "locality"
    KINGDOM_FLD = "kingdom"
    SCINAME_FLD = "scientificName"
    SCIAUTHOR_FLD = "scientificNameAuthorship"
    RANK_FLD = "taxonRank"
    ASSESSMENT_FLD = "degreeOfEstablishment"
    TAXON_AUTHORITY_FLD = "taxonomicStatus"
    RIIS_STATUS = {
        "established (category C3)": "established",
        "invasive (category D2)": "invasive",
        "widespread invasive (category E)": "widespread invasive"
    }
    SPECIES_GEO_HEADER = [
        "locality",
        "scientificName",
        "scientificNameAuthorship",
        "vernacularName",
        "taxonRank",
        "establishmentMeans",
        "degreeOfEstablishment",
        "isHybrid",
        "pathway",
        "habitat",
        "Biocontrol",
        "associatedTaxa",
        "eventRemarks",
        "IntroDateNumber",
        "taxonRemarks",
        "kingdom",
        "phylum	class",
        "order",
        "family",
        "taxonomicStatus",
        "ITIS_TSN",
        "GBIF_taxonKey",
        "taxonID",
        "Authority",
        "WebLink",
        "associatedReferences",
        "eventDate",
        "modified",
        "Update_Remarks",
        "occurrenceRemarks",
        "occurrenceID"
    ]


# .............................................................................
class GBIF:
    """Constants for GBIF DWCA fields, APIs, and their request and response objects."""
    INPUT_DATA = "gbif_2023-08-23.csv"
    INPUT_LINE_COUNT = 904441801        # from wc -l
    # REPORTED_RECORD_COUNT = 904377770   # in GBIF metadata
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
    NAMEKEY_FLD = "taxonKey"
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
    def REQUIRED_FIELDS(cls):
        """Data fields required to process records for BISON.

        Returns:
            set of fieldnames needed for BISON processing.
        """
        return (
            GBIF.ID_FLD, GBIF.TAXON_FLD, GBIF.SPECIES_KEY_FLD, GBIF.RANK_FLD,
            GBIF.ACC_NAME_FLD, GBIF.ACC_TAXON_FLD, GBIF.LON_FLD, GBIF.LAT_FLD
        )

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
