"""Constants for GBIF, BISON, RIIS, and processed outputs, used across modules."""

BIG_DATA_PATH = "/home/astewart/git/bison/big_data"
DATA_PATH = "/data"
INPUT_DIR = "input"
OUT_DIR = "out"
ENCODING = "utf-8"
LINENO_FLD = "LINENO"
ERR_SEPARATOR = "------------"
# Geospatial data for intersecting with points to identify state and county for points
COARSE_BUFFER_RANGE = [(i / 10.0) for i in range(1, 11)]
PAD_BUFFER = COARSE_BUFFER_RANGE[0]

AGGREGATOR_DELIMITER = "__"
EXTRA_CSV_FIELD = "rest_values"

COMMANDS = (
    "resolve", "split", "annotate", "summarize", "aggregate", "check_counts",
    "heat_matrix", "pam_stats")

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
class REPORT:
    """Common keys for process report dictionary."""
    PROCESS = "process"
    RIIS_IDENTIFIER = "riis_ids"
    RIIS_TAXA = "riis_taxa"
    RIIS_RESOLVE_FAIL = "riis_bad_species"
    TAXA_RESOLVED = "names_resolved"
    RECORDS_UPDATED = "records_updated"
    RECORDS_OUTPUT = "records_output"
    INFILE = "input_filename"
    OUTFILE = "output_filename"
    LOGFILE = "log_filename"
    REPORTFILE = "report_filename"
    SUMMARY = "summary"
    REGION = "region"
    MIN_VAL = "min_val_for_presence"
    MAX_VAL = "min_val_for_presence"
    LOCATION = "locations"
    AGGREGATION = "riis_assessments_by_location"
    SPECIES = "species"
    OCCURRENCE = "occurrences"
    HEATMATRIX = "heatmatrix"
    ROWS = "rows"
    COLUMNS = "columns"
    ANNOTATE_FAIL = "annotate_gbifid_failed"
    ANNOTATE_FAIL_COUNT = "records_failed_annotate"
    RANK_FAIL = "rank_failed"
    RANK_FAIL_COUNT = "records_failed_rank"
    MESSAGE = "message"


# .............................................................................
class LMBISON_PROCESS:
    """Process steps and associated filename postfixes indicating completion."""
    RESOLVE = {"step": 0, "postfix": "resolve"}
    CHUNK = {"step": 1, "postfix": "raw", "prefix": "chunk"}
    ANNOTATE = {"step": 2, "postfix": "annotate"}
    GROUP_BY = {"step": 2, "postfix": "group"}
    UPDATE = {"step": 2, "postfix": "update"}
    SUMMARIZE = {"step": 3, "postfix": "summary"}
    COMBINE = {"step": 4, "postfix": "combine"}
    AGGREGATE = {"step": 5, "postfix": "aggregate"}
    CHECK_COUNTS = {"step": 6, "postfix": "check_counts"}
    HEATMATRIX = {"step": 7, "postfix": "heatmatrix"}
    PAM = {"step": 8, "postfix": "pam"}

    @staticmethod
    def process_types():
        """Return all DWC Process types.

        Returns:
            List of all DWC_Process types.
        """
        return (
            LMBISON_PROCESS.CHUNK, LMBISON_PROCESS.ANNOTATE,
            LMBISON_PROCESS.SUMMARIZE, LMBISON_PROCESS.COMBINE,
            LMBISON_PROCESS.AGGREGATE, LMBISON_PROCESS.HEATMATRIX,
            LMBISON_PROCESS.PAM
        )

    @staticmethod
    def postfixes():
        """Return all DWC Process types.

        Returns:
            List of all DWC_Process types.
        """
        postfixes = []
        for p in LMBISON_PROCESS.process_types():
            try:
                postfixes.append(p["postfix"])
            except KeyError:
                pass
        return postfixes

    @staticmethod
    def _is_instance(obj):
        try:
            keys = obj.keys()
        except Exception:
            is_dwc_proc = False
        else:
            is_dwc_proc = ("step" in keys and "postfix" in keys)
        return is_dwc_proc

    @staticmethod
    def get_postfix(step_or_process):
        """For a given step number or DWC_PROCESS, return the postfix.

        Args:
            step_or_process (int): Numerical stage of processing completed
                or DWC_PROCESS.

        Returns:
            String for filename postfix for the given step.
        """
        is_dp_obj = LMBISON_PROCESS._is_instance(step_or_process)
        for pt in LMBISON_PROCESS.process_types():
            if ((isinstance(step_or_process, int) and pt["postfix"] == step_or_process)
                    or is_dp_obj and pt == step_or_process):
                return pt["postfix"]
        return None

    @staticmethod
    def get_step(postfix_or_process):
        """For a given postfix or DWC_PROCESS, return the step number.

        Args:
            postfix_or_process (str or LMBISON_PROCESS): String appended to the end of a
                filename (before the extension) to indicate the stage of processing
                completed, or DWC_PROCESS.

        Returns:
            Integer for step corresponding to the given filename postfix.
        """
        is_dp_obj = LMBISON_PROCESS._is_instance(postfix_or_process)
        for pt in LMBISON_PROCESS.process_types():
            if ((isinstance(postfix_or_process, str) and
                 pt["postfix"] == postfix_or_process)
                    or is_dp_obj and pt == postfix_or_process):
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

        Raises:
            Exception: if neither postfix, not step is provided.
        """
        if postfix is None and step is None:
            raise Exception("Must provide either prefix or step for process.")
        for pt in LMBISON_PROCESS.process_types():
            if postfix is not None and pt["postfix"] == postfix:
                return pt
            elif step is not None and pt["step"] == step:
                return pt
        return None

    @staticmethod
    def get_next_process(postfix=None, step=None):
        """For a given postfix or step number, return the DWC_PROCESS object.

        Args:
            postfix (str): String appended to the end of a filename (before the
                extension) to indicate the stage of processing completed.
            step (int): Numerical stage of processing completed.

        Returns:
            DWC_Process type for the given filename postfix or step.

        Raises:
            Exception: if neither postfix, not step is provided.
        """
        # If nothing provided, return the first
        if postfix is None and step is None:
            raise Exception("Must provide either prefix or step for next process.")

        for pt in LMBISON_PROCESS.process_types():
            if postfix is not None and pt["postfix"] == postfix:
                this_process = pt
            elif step is not None and pt["step"] == step:
                this_process = pt
        if this_process is not None:
            next_step = this_process["step"] + 1
            next_process = LMBISON_PROCESS.get_process(step=next_step)
            return next_process

        return None


# .............................................................................
class CONFIG_PARAM:
    """Parameter keys for CLI tool configuration files."""
    FILE = "config_file"
    COMMAND = "command"
    IS_INPUT_DIR = "is_input_dir"
    IS_OUPUT_DIR = "is_output_dir"
    IS_INPUT_FILE = "is_input_file"
    CHOICES = "choices"
    HELP = "help"
    TYPE = "type"


# .............................................................................
PARAMETERS = {
    "required":
        {
            "riis_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Full filename of input USGS RIIS data in CSV format."
                },
            "gbif_filename":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "Full filename of input GBIF occurrence data in CSV format."
                },
            "do_split":
                {
                    CONFIG_PARAM.TYPE: bool,
                    CONFIG_PARAM.HELP:
                        "Flag indicating whether the GBIF data is to be (or has been) "
                        "split into smaller subsets."
                },
            "run_parallel":
                {
                    CONFIG_PARAM.TYPE: bool,
                    CONFIG_PARAM.HELP:
                        "Flag indicating whether the annotation process is to be "
                        "run in parallel threads."
                },
            "geo_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_INPUT_DIR: True,
                    CONFIG_PARAM.HELP:
                        "Source directory containing geospatial input data."
                },
            "process_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Large destination directory for temporary data."
                },
            "output_path":
                {
                    CONFIG_PARAM.TYPE: str,
                    CONFIG_PARAM.IS_OUPUT_DIR: True,
                    CONFIG_PARAM.HELP: "Large destination directory for output data."
                }
        },
    "optional":
        {
            "chunk_count":
                {
                    CONFIG_PARAM.TYPE: int,
                    CONFIG_PARAM.HELP:
                        "Number of files to split a large CSV into."
                },
            "gbif_id":
                {
                    CONFIG_PARAM.TYPE: int,
                    CONFIG_PARAM.HELP:
                        "Identifier, gbifId, of troublesome record in original or "
                        "annotated occurrence file."
                },
            "line_num":
                {
                    CONFIG_PARAM.TYPE: int,
                    CONFIG_PARAM.HELP:
                        "Line number of record to examine in original or "
                        "annotated occurrence file."
                },
            "examine_filenames":
                {
                    CONFIG_PARAM.TYPE: list,
                    CONFIG_PARAM.IS_INPUT_FILE: True,
                    CONFIG_PARAM.HELP:
                        "List of full filenames of input occurrence files to inspect."
                }
        }
}


# .............................................................................
class LMBISON:
    """Headers for temporary and final output files."""
    RR_SPECIES_KEY = "riisregion_specieskey"
    SCIENTIFIC_NAME_KEY = "accepted_scientific_name"
    SPECIES_NAME_KEY = "species_name"
    ASSESS_KEY = "assessment"
    STATE_KEY = "state"
    COUNTY_KEY = "county"
    LOCATION_TYPE_KEY = "location_type"
    LOCATION_KEY = "location"
    COUNT_KEY = "count"

    NOT_APPLICABLE = "na"
    INTRODUCED_VALUE = "introduced"
    INVASIVE_VALUE = "invasive"
    NATIVE_VALUE = "presumed_native"
    SUMMARY_FILTER_HEADING = "filtered"

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

    @staticmethod
    def assess_values():
        """Value in annotated GBIF records indicating the RIIS determination.

        Returns:
            list of all values used to assign RIIS determination to records.
        """
        return (LMBISON.INTRODUCED_VALUE, LMBISON.INVASIVE_VALUE, LMBISON.NATIVE_VALUE)

    @staticmethod
    def summary_header_temp():
        """Header in temporary file summarizing the contents of an annotated Dwc file.

        Returns:
            list of all fieldnames used in temporary summary file.
        """
        return (
            LMBISON.LOCATION_TYPE_KEY, LMBISON.LOCATION_KEY, LMBISON.RR_SPECIES_KEY,
            LMBISON.SCIENTIFIC_NAME_KEY, LMBISON.SPECIES_NAME_KEY, LMBISON.COUNT_KEY)

    @staticmethod
    def region_summary_header():
        """Header in output file summarizing a region.

        Returns:
            list of all fieldnames used in region summary file.
        """
        return (
            LMBISON.RR_SPECIES_KEY, LMBISON.SCIENTIFIC_NAME_KEY, LMBISON.SPECIES_NAME_KEY,
            LMBISON.COUNT_KEY, LMBISON.ASSESS_KEY
        )

    @staticmethod
    def all_summary_header():
        """Header in output file summarizing all data.

        Returns:
            list of all fieldnames used in the composite summary file.
        """
        return (
            LMBISON.STATE_KEY, LMBISON.COUNTY_KEY, LMBISON.INTRODUCED_SPECIES,
            LMBISON.INVASIVE_SPECIES, LMBISON.NATIVE_SPECIES, LMBISON.TOTAL_SPECIES,
            LMBISON.PCT_INTRODUCED_SPECIES, LMBISON.PCT_INVASIVE_SPECIES,
            LMBISON.PCT_NATIVE_SPECIES, LMBISON.INTRODUCED_OCCS, LMBISON.INVASIVE_OCCS,
            LMBISON.NATIVE_OCCS, LMBISON.TOTAL_OCCS, LMBISON.PCT_INTRODUCED_OCCS,
            LMBISON.PCT_INVASIVE_OCCS, LMBISON.PCT_NATIVE_OCCS)


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
    PAD_GAP_STATUS = "GAP_Sts"
    PAD_GAP_STATUS_DESC = "d_GAP_Sts"

    @staticmethod
    def annotation_fields():
        """All fields added to DwC records for analysis by geospatial region.

        Returns:
            list of all the fields to be added to the annotated Dwc occurrence file.
        """
        return (
            APPEND_TO_DWC.RESOLVED_CTY,
            APPEND_TO_DWC.RESOLVED_ST,
            APPEND_TO_DWC.RIIS_KEY,
            APPEND_TO_DWC.RIIS_ASSESSMENT,
            APPEND_TO_DWC.AIANNH_GEOID,
            APPEND_TO_DWC.AIANNH_NAME,
            APPEND_TO_DWC.PAD_NAME,
            APPEND_TO_DWC.PAD_GAP_STATUS,
            # APPEND_TO_DWC.PAD_GAP_STATUS_DESC,
        )


CENSUS_STATE_FLDNAME = "STUSPS"


class REGION:
    """Geospatial regions to add to DwC data and summarize with RIIS determinations."""
    COUNTY = {
        "file": "census/cb_2021_us_county_500k.shp",
        "buffer": COARSE_BUFFER_RANGE,
        "is_disjoint": False,
        "summary": [
            ("county", (APPEND_TO_DWC.RESOLVED_ST, APPEND_TO_DWC.RESOLVED_CTY)),
            ("state", APPEND_TO_DWC.RESOLVED_ST)
        ],
        "map": {
            "NAME": APPEND_TO_DWC.RESOLVED_CTY,
            CENSUS_STATE_FLDNAME: APPEND_TO_DWC.RESOLVED_ST
        }
    }
    AIANNH = {
        "file": "census/cb_2021_us_aiannh_500k.shp",
        "buffer": (),
        "is_disjoint": True,
        "summary": [("aiannh", APPEND_TO_DWC.AIANNH_NAME)],
        "map": {
            "NAMELSAD": APPEND_TO_DWC.AIANNH_NAME,
            "GEOID": APPEND_TO_DWC.AIANNH_GEOID
        }
    }
    PAD = {
        "filepattern": "pad_st/PADUS3_0Designation_State??_4326.shp",
        "buffer": (),
        "is_disjoint": True,
        "subset_field": APPEND_TO_DWC.RESOLVED_ST,
        # file prefix, field name
        "summary": [("pad", APPEND_TO_DWC.PAD_NAME)],
        "map": {
            "Unit_Nm": APPEND_TO_DWC.PAD_NAME,
            "GAP_Sts": APPEND_TO_DWC.PAD_GAP_STATUS,
            # "d_GAP_Sts": APPEND_TO_DWC.PAD_GAP_STATUS_DESC
            }
        }

    @staticmethod
    def summary_fields():
        """Return fields to summarize data on, and the prefix for the summary filename.

        Returns:
            Dictionary of field and prefix as keys/values.

        Raises:
            Exception: on unexpected field for region type:
                Expects a single fieldname, or tuple of 2 fieldnames for concatenation.
        """
        summarize_by_fields = {}
        regions = REGION.for_summary()
        for reg in regions:
            for prefix, flds in reg["summary"]:
                if isinstance(flds, str):
                    summarize_by_fields[prefix] = flds
                elif isinstance(flds, tuple) and len(flds) == 2:
                    summarize_by_fields[prefix] = flds
                else:
                    raise Exception(f"Bad metadata for {prefix} summary fields")
        return summarize_by_fields

    @staticmethod
    def region_disjoint():
        """Return fields to summarize data on, and the prefix for the summary filename.

        Returns:
            Dictionary of field and prefix as keys/values.
        """
        region_disjoint = {}
        regions = REGION.for_summary()
        for reg in regions:
            for prefix, _ in reg["summary"]:
                region_disjoint[prefix] = reg["is_disjoint"]
        # region_disjoint[LMBISON.SUMMARY_FILTER_HEADING] = False
        return region_disjoint

    @staticmethod
    def for_resolve():
        """Return all REGION types to use to annotate records.

        Returns:
            sequence of all REGION types.
        """
        return (REGION.COUNTY, REGION.AIANNH, REGION.PAD)

    @staticmethod
    def full_region():
        """Return all REGION types that cover the entire US.

        Returns:
            List of all REGION types that enclose the entire region.
        """
        return (REGION.COUNTY, REGION.AIANNH)

    @staticmethod
    def for_summary():
        """Return all REGION types to be summarized.

        Returns:
            List of all REGION types that together enclose the entire region.
        """
        return (REGION.COUNTY, REGION.AIANNH, REGION.PAD)

    # .............................................................................
    @staticmethod
    def get_state_files_from_pattern(geo_path):
        """Get state codes and full filenames for the PAD-by-state shapefiles.

        Args:
            geo_path (str): base path for geospatial data.

        Returns:
            st_files (dict): Dictionary of codes and absolute filenames.
        """
        import os
        import glob
        st_files = {}
        geo_pattern = os.path.join(geo_path, REGION.PAD["filepattern"])
        geo_files = glob.glob(geo_pattern)
        for gfname in geo_files:
            basename = os.path.basename(gfname)
            st = basename.split("_")[2][-2:]
            st_files[st] = gfname
        return st_files


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
    DATA_DICT_FNAME = "US-RIISv2_DataDictionary.csv"
    # Authority References Metadata.
    AUTHORITY_FNAME = "USRIISv2_AuthorityReferences.csv"
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


# .............................................................................
__all__ = [
    "AGGREGATOR_DELIMITER",
    "APPEND_TO_DWC",
    "APPEND_TO_RIIS",
    "BIG_DATA_PATH",
    "CENSUS_STATE_FLDNAME",
    "COARSE_BUFFER_RANGE",
    "COMMANDS",
    "CONFIG_PARAM",
    "DATA_PATH",
    "ENCODING",
    "ERR_SEPARATOR",
    "EXTRA_CSV_FIELD",
    "GBIF",
    "INPUT_DIR",
    "ITIS",
    "LINENO_FLD",
    "LMBISON",
    "LMBISON_PROCESS",
    "NS",
    "OUT_DIR",
    "PAD_BUFFER",
    "RANKS",
    "RANKS_BELOW_SPECIES",
    "REGION",
    "REPORT",
    "RIIS_DATA",
    "US_STATES"
]
