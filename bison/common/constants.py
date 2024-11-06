"""Constants to use locally initiate BISON AWS EC2 Spot Instances."""
import copy
from enum import Enum
import os

PROJECT = "bison"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
AWS_METADATA_URL = "http://169.254.169.254/latest/"

WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"
WORKFLOW_ROLE_ARN = f"arn:aws:iam::{PROJECT}:role/service-role/{WORKFLOW_ROLE_NAME}"
WORKFLOW_USER = f"project.{PROJECT}"
WORKFLOW_SECRET_NAME = f"{PROJECT}_workflow_user"

GBIF_BUCKET = f"gbif-open-data-{REGION}"
GBIF_ARN = f"arn:aws:s3:::{GBIF_BUCKET}"
GBIF_ODR_FNAME = "occurrence.parquet"

EC2_SPOT_TEMPLATE = "bison_spot_task_template"
EC2_ROLE_NAME = f"{PROJECT}_ec2_s3_role"
# Instance types: https://aws.amazon.com/ec2/spot/pricing/
EC2_INSTANCE_TYPE = "t4g.micro"

S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"

# .............................................................................
# Docker compose files for tasks
# .............................................................................
# Assumes project repo directory
USERDATA_DIR = "./aws/userdata"

# .............................................................................
# Data constants
# .............................................................................
COUNT_FLD = "count"
TOTAL_FLD = "total"

OCCURRENCE_COUNT_FLD = "occ_count"
SPECIES_COUNT_FLD = "species_count"

UNIQUE_SPECIES_FLD = "taxonkey_species"
UNIQUE_COUNTY_FLD = "state_county"
OCCURRENCE_STATUS = "riis"
OCCURRENCE_STATUS_FLD = "riis_assessment"

# .............................................................................
# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

TMP_PATH = "/tmp"
ENCODING = "utf-8"
ERR_SEPARATOR = "------------"
USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"
CSV_DELIMITER = ","

class TASK:
    """Workflow tasks to be executed on EC2 instances."""
    TEST = "test_task"
    ANNOTATE_RIIS = "annotate_riis"
    SUMMARIZE = "summarize"
    BUILD_HEATMAP = "build_heatmap"
    userdata_extension = ".userdata.sh"

    # ...........................
    @classmethod
    def tasks(cls):
        """Get all valid tasks.

        Returns:
            (list of str): all valid tasks
        """
        return (cls.TEST, cls.ANNOTATE_RIIS, cls.SUMMARIZE, cls.BUILD_HEATMAP)

    # ...........................
    @classmethod
    def get_userdata_filename(cls, task, pth=None):
        """Get the filename containing userdata to execute this task.

        Args:
            task (str): task
            pth (str): local path for file.

        Returns:
            fname (str): filename for EC2 userdata to execute task.
        """
        if task not in cls.tasks():
            raise Exception(f"Unknown task {task}")
        fname = f"{task}{cls.userdata_extension}"
        if pth is not None:
            fname = os.path.join(pth, fname)
        return fname

    # ...........................
    @classmethod
    def get_task_from_userdata_filename(cls, fname):
        """Get the task for the userdata file.

        Args:
            fname (str): filename for EC2 userdata to execute task.

        Returns:
            task (str): task

        Raises:
            Exception: on no task for this filename.
        """
        task = fname.rstrip(cls.userdata_extension)
        if task not in cls.tasks():
            raise Exception(f"Unknown task {task} from userdata file {fname}")
        return task


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
class ANALYSIS_DIM:
    """All dimensions (besides species) with columns used for data analyses."""
    STATE = {
        "code": "state",
        # In summary records
        "fields": [
            "census_state", UNIQUE_SPECIES_FLD, OCCURRENCE_STATUS_FLD, OCCURRENCE_COUNT_FLD
        ],
        "key_fld": "census_state",
    }
    COUNTY = {
        "code": "county",
        "fields": [
            UNIQUE_COUNTY_FLD, UNIQUE_SPECIES_FLD, OCCURRENCE_STATUS_FLD,
            OCCURRENCE_COUNT_FLD
        ],
        "key_fld": UNIQUE_COUNTY_FLD,
    }
    AIANNH = {
        "code": "aiannh",
        "fields": [
            "aiannh_name", UNIQUE_SPECIES_FLD, OCCURRENCE_STATUS_FLD, OCCURRENCE_COUNT_FLD
        ],
        "key_fld": "aiannh_name",
    }
    SPECIES = {
        "code": "species",
        "key_fld": UNIQUE_COUNTY_FLD,
        # Species status for each occurrence
        "status": OCCURRENCE_STATUS,
        "status_fld": OCCURRENCE_STATUS_FLD
    }

    # ...........................
    @classmethod
    def species(cls):
        """Get the data species analyses dimension.

        Returns:
            Data dimension relating to species.
        """
        return ANALYSIS_DIM.SPECIES

    # ...........................
    @classmethod
    def species_code(cls):
        """Get the code for the data species analyses dimension.

        Returns:
            Code for the data dimension relating to species.
        """
        return ANALYSIS_DIM.SPECIES["code"]

    # ...........................
    @classmethod
    def analysis(cls, code=None):
        """Get one or all data analyses dimensions to be analyzed for species.

        Args:
            code (str): Code for the analysis dimension to be returned.

        Returns:
            dim_lst (list): List of data dimension(s) to be analyzed for species.

        Raises:
            Exception: on unknown code.
        """
        if code is None:
            dim_lst = [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]
        else:
            try:
                dim_lst = [cls.get(code)]
            except KeyError:
                raise Exception(f"No dimension `{code}` in ANALYSIS_DIM")
        return dim_lst

    # ...........................
    @classmethod
    def analysis_code(cls, code=None):
        """Get one or all codes for data analyses dimensions to be analyzed for species.

        Args:
            code (str): Code for the analysis dimension to be returned.

        Returns:
            code_lst (list): Codes of data dimension(s) to be analyzed for species.

        Raises:
            Exception: on unknown code.
        """
        if code is None:
            code_lst = [
                ANALYSIS_DIM.STATE["code"], ANALYSIS_DIM.COUNTY["code"],
                ANALYSIS_DIM.AIANNH["code"]
            ]
        else:
            try:
                code_lst = [cls.get(code)["code"]]
            except KeyError:
                raise Exception(f"No dimension `{code}` in ANALYSIS_DIM")
        return code_lst

    # ...........................
    @classmethod
    def get(cls, code):
        """Get the data analyses dimension for the code.

        Args:
            code (str): Code for the analysis dimension to be returned.

        Returns:
            Data dimension.

        Raises:
            Exception: on unknown code.
        """
        for dim in (
                ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH,
                ANALYSIS_DIM.SPECIES):
            if code == dim["code"]:
                return dim
        raise Exception(f"No dimension `{code}` in ANALYSIS_DIM")


# .............................................................................
class SUMMARY:
    """Types of tables stored in S3 for aggregate species data analyses."""
    dt_token = "YYYY_MM_DD"
    sep = "_"
    dim_sep = f"{sep}x{sep}"
    DATATYPES = ("list", "counts", "summary", "matrix")
    SPECIES_DIMENSION = ANALYSIS_DIM.species_code()
    ANALYSIS_DIMENSIONS = ANALYSIS_DIM.analysis_code()

    # ...........................
    @classmethod
    def get_table_type(cls, datatype, dim0, dim1):
        """Get the table_type string for the analysis dimension and datatype.

        Args:
            datatype (SUMMARY.DATAYPES): type of aggregated data.
            dim0 (str): code for primary dimension (bison.common.constants.ANALYSIS_DIM)
                of analysis
            dim1 (str): code for secondary dimension of analysis

        Note:
            BISON Table types include:
                list: region_x_species_list
                counts: region_counts
                summary: region_x_species_summary
                         species_x_region_summary
                matrix:  species_x_region_matrix

        Note: for matrix, dimension1 corresponds to Axis 0 (rows) and dimension2
            corresponds to Axis 1 (columns).

        Returns:
            table_type (str): code for data type and contents

        Raises:
            Exception: on datatype not one of: "counts", "list", "summary", "matrix"
            Exception: on datatype "counts", dim0 not in ANALYSIS_DIMENSIONS
            Exception: on datatype "counts", dim1 not OCCURRENCE_STATUS or None
            Exception: on dim0 in ANALYSIS_DIMENSIONS and dim1 != SPECIES_DIMENSION
            Exception: on dim0 == SPECIES_DIMENSION and dim1 not in ANALYSIS_DIMENSIONS
            Exception: on dim0 != SPECIES_DIMENSION and not in ANALYSIS_DIMENSIONS
        """
        if datatype not in cls.DATATYPES:
            raise Exception(f"Datatype {datatype} is not in {cls.DATATYPES}.")

        if datatype == "counts":
            if dim0 in cls.ANALYSIS_DIMENSIONS:
                if dim1 == OCCURRENCE_STATUS:
                    # ex: state-x-riis_counts
                    table_type = f"{dim0}{cls.dim_sep}{dim1}{cls.sep}{datatype}"
                elif dim1 is None:
                    # ex: state_counts
                    table_type = f"{dim0}{cls.sep}{datatype}"
                else:
                    raise Exception(
                        f"Second dimension must be {OCCURRENCE_STATUS} or None")
            else:
                raise Exception(
                    f"First dimension for counts must be in {cls.ANALYSIS_DIMENSIONS}.")
        else:
            if dim0 == cls.SPECIES_DIMENSION and dim1 not in cls.ANALYSIS_DIMENSIONS:
                raise Exception(
                    f"Second dimension must be in {cls.ANALYSIS_DIMENSIONS}")
            elif dim0 not in cls.ANALYSIS_DIMENSIONS and dim0 != cls.SPECIES_DIMENSION:
                raise Exception(
                    f"First dimension must be {cls.SPECIES_DIMENSION} or "
                    f"in {cls.ANALYSIS_DIMENSIONS}.")

            table_type = f"{dim0}{cls.dim_sep}{dim1}{cls.sep}{datatype}"
        return table_type

    # ...........................
    @classmethod
    def list(cls):
        """Records of dimension, species, occ count for each dimension in project.

        Returns:
            list (dict): dict of dictionaries for each list table defined by the
                project.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        list = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            table_type = cls.get_table_type(
                "list", analysis_code, cls.SPECIES_DIMENSION)
            dim = ANALYSIS_DIM.analysis(code=analysis_code)[0]
            # name == table_type, ex: county-x-species_list
            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}{cls.sep}000",
                "format": "Parquet",
                "fields": dim["fields"],
                "key_fld": dim["key_fld"],
                "species_fld": UNIQUE_SPECIES_FLD,
                "value_fld": OCCURRENCE_COUNT_FLD
            }
            list[table_type] = meta
        return list

    # ...........................
    @classmethod
    def counts(cls):
        """Records of dimension, species count, occ count for each dimension in project.

        Returns:
            list (dict): dict of dictionaries for each list table defined by the
                project.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        counts = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            dim0 = ANALYSIS_DIM.analysis(code=analysis_code)[0]
            # Analysis (aka region) alone or with RIIS species status
            for dim1 in (None, OCCURRENCE_STATUS):
                table_type = cls.get_table_type(
                    "counts", analysis_code, dim1)

                fields = copy.deepcopy(dim0["fields"])
                # add species count
                fields.append(SPECIES_COUNT_FLD)
                # if just dim counts, remove RIIS status
                if dim1 is None:
                    fields.remove(OCCURRENCE_STATUS_FLD)

                meta = {
                    "code": table_type,
                    "fname": f"{table_type}{cls.sep}{cls.dt_token}{cls.sep}000",
                    "format": "Parquet",
                    "key_fld": dim0["key_fld"],
                    "fields": fields,
                    "occurrence_count_fld": OCCURRENCE_COUNT_FLD,
                    "species_count_fld": SPECIES_COUNT_FLD
                }
                counts[table_type] = meta
        return counts

    # ...........................
    @classmethod
    def summary(cls):
        """Summary tables of species count and occurrence count for each dimension in project.

        Returns:
            sums (dict): dict of dictionaries for each summary table defined by the
                project.

        Note:
            table contains stacked records summarizing original data:
                dim0, dim1, rec count of dim1 in dim0
                ex: county, species, occ_count
        """
        sums = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            # Analysis first
            table_type1 = cls.get_table_type(
                "summary", analysis_code, cls.SPECIES_DIMENSION)
            # Species first
            table_type2 = cls.get_table_type(
                "summary", cls.SPECIES_DIMENSION, analysis_code)

            for tt in (table_type1, table_type2):
                meta = {
                    "code": tt,
                    "fname": f"{tt}{cls.sep}{cls.dt_token}",
                    "table_format": "Zip",
                    "matrix_extension": ".csv",
                    # Axis 1
                    "column": "measurement_type",
                    "fields": [COUNT_FLD, TOTAL_FLD],
                    # Matrix values
                    "value": "measure"}
                sums[tt] = meta
        return sums

    # ...........................
    @classmethod
    def matrix(cls):
        """Species by <dimension> matrix defined for this project.

        Returns:
            mtxs (dict): dict of dictionaries for each matrix/table defined for this
                project.

        Note:
            Rows will always have species, columns will have analysis dimension (region)
        """
        mtxs = {}
        for analysis_code in cls.ANALYSIS_DIMENSIONS:
            dim0 = cls.SPECIES_DIMENSION
            dim0_dict = ANALYSIS_DIM.species()
            dim1 = analysis_code
            dim1_dict = ANALYSIS_DIM.analysis(code=analysis_code)[0]
            table_type = cls.get_table_type("matrix", dim0, dim1)
            row_input = cls.get_table_type("summary", dim0, dim1)
            col_input = cls.get_table_type("summary", dim1, dim0)

            # Dimension 0/row is always species
            meta = {
                "code": table_type,
                "fname": f"{table_type}{cls.sep}{cls.dt_token}",
                "table_format": "Zip",
                "matrix_extension": ".npz",
                "data_type": "matrix",

                # Dimension 1 is row (aka Axis 0)
                "row": dim0_dict["key_fld"],
                "row_input": row_input,
                # Dimension 2 is column (aka Axis 1)
                "column": dim1_dict["key_fld"],
                "column_input": col_input,

                # Matrix values
                "value": OCCURRENCE_COUNT_FLD,
            }
            mtxs[table_type] = meta
        return mtxs

    # ...............................................
    @classmethod
    def tables(cls, datestr=None):
        """All tables of species count and occurrence count, summary, and matrix.

        Args:
            datestr (str): String in the format YYYY_MM_DD.

        Returns:
            sums (dict): dict of dictionaries for each table defined by the project.
                If datestr is provided, the token in the filename is replaced with that.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        tables = cls.list()
        tables.update(cls.counts())
        tables.update(cls.summary())
        tables.update(cls.matrix())
        if datestr is not None:
            # Update filename in summary tables
            for key, meta in tables.items():
                meta_cpy = copy.deepcopy(meta)
                fname_tmpl = meta["fname"]
                meta_cpy["fname"] = fname_tmpl.replace(cls.dt_token, datestr)
                tables[key] = meta_cpy
        return tables

    # ...............................................
    @classmethod
    def get_table(cls, table_type, datestr=None):
        """Update the filename in a metadata dictionary for one table, and return.

        Args:
            table_type: type of summary table to return.
            datestr: Datestring contained in the filename indicating the current version
                of the data.

        Returns:
            tables: dictionary of summary table metadata.
        """
        try:
            table = cls.tables()[table_type]
        except KeyError:
            return None
        cpy_table = copy.deepcopy(table)
        if datestr is not None:
            fname_tmpl = cpy_table["fname"]
            cpy_table["fname"] = fname_tmpl.replace(cls.dt_token, datestr)
        return cpy_table

    # ...............................................
    @classmethod
    def get_tabletype_from_filename_prefix(cls, datacontents, datatype):
        """Get the table type from the file prefixes.

        Args:
            datacontents (str): first part of filename indicating data in table.
            datatype (str): second part of filename indicating form of data in table
                (records, list, matrix, etc).

        Returns:
            table_type (SUMMARY_TABLE_TYPES type): type of table.

        Raises:
            Exception: on invalid file prefix.
        """
        tables = cls.tables()
        table_type = None
        for key, meta in tables.items():
            fname = meta["fname"]
            contents, dtp, _, _ = cls._parse_filename(fname)
            if datacontents == contents and datatype == dtp:
                table_type = key
        if table_type is None:
            raise Exception(
                f"Table with filename prefix {datacontents}_{datatype} does not exist")
        return table_type

    # ...............................................
    @classmethod
    def get_filename(cls, table_type, datestr):
        """Update the filename in a metadata dictionary for one table, and return.

        Args:
            table_type (str): predefined type of data indicating type and contents.
            datestr: Datestring contained in the filename indicating the current version
                of the data.

        Returns:
            tables: dictionary of summary table metadata.
        """
        tables = cls.tables()
        fname_tmpl = tables[table_type]["fname"]
        fname = fname_tmpl.replace(cls.dt_token, datestr)
        return fname

    # ...............................................
    @classmethod
    def parse_table_type(cls, table_type):
        """Parse the table_type into datacontents (dim0, dim1) and datatype.

        Args:
            table_type: String identifying the type of data and dimensions.

        Returns:
            datacontents (str): type of data contents
            dim0 (str): first dimension (rows/axis 0) of data in the table
            dim1 (str): second dimension (columns/axis 1) of data in the table
            datatype (str): type of data structure: summary table, stacked records
                (list or count), or matrix.

        Raises:
            Exception: on failure to parse table_type into 2 strings.
        """
        fn_parts = table_type.split(cls.sep)
        if len(fn_parts) == 2:
            datacontents, datatype = fn_parts
        else:
            raise Exception(f"Failed to parse {table_type} into datacontents, datetype.")
        # Some data has 2 dimensions
        dim_parts = datacontents.split(cls.dim_sep)
        if len(dim_parts) == 1:
            dim0 = dim_parts[0]
            dim1 = None
        elif len(dim_parts) == 2:
            dim0, dim1 = dim_parts
        else:
            dim0 = dim1 = None
        return datacontents, dim0, dim1, datatype

    # ...............................................
    @classmethod
    def _parse_filename(cls, filename):
        # <datacontents>_<datatype>_<YYYY_MM_DD><_optional parquet extension>
        fname = os.path.basename(filename)
        fname_noext, _ext = os.path.splitext(fname)
        fn_parts = fname_noext.split("_")
        if len(fn_parts) >= 5:
            datacontents = fn_parts[0]
            try:
                dim0, dim1 = datacontents.split(cls.dim_sep)
            except ValueError:
                dim0 = dim1 = None
            datatype = fn_parts[1]
            yr = fn_parts[2]
            mo = fn_parts[3]
            day = fn_parts[4]
            rest = fn_parts[5:]
            if len(yr) == 4 and len(mo) == 2 and len(day) == 2:
                data_datestr = f"{yr}_{mo}_{day}"
            else:
                raise Exception(
                    f"Length of elements year, month, day ({yr}, {mo}. {day}) should "
                    "be 4, 2, and 2")
        else:
            raise Exception(f"{fname_noext} does not follow the expected pattern")
        return datacontents, dim0, dim1, datatype, data_datestr, rest

    # ...............................................
    @classmethod
    def get_tabletype_datestring_from_filename(cls, filename):
        """Get the table type from the filename.

        Args:
            filename: relative or absolute filename of a SUMMARY data file.

        Returns:
            table_type (SUMMARY_TABLE_TYPES type): type of table.
            data_datestr (str): date of data in "YYYY_MM_DD" format.

        Raises:
            Exception: on failure to get tabletype and datestring from this filename.
        """
        try:
            datacontents, dim0, dim1, datatype, data_datestr, _rest = \
                cls._parse_filename(filename)
            table_type = f"{datacontents}{cls.sep}{datatype}"
        except Exception:
            raise
        return table_type, data_datestr


# .............................................................................
class SNKeys(Enum):
    """Dictionary keys to use for describing RowColumnComparisons of SUMMARY data.

    Note: All keys refer to the relationship between rows, columns and values.  Missing
        values in a dataset dictionary indicate that the measure is not meaningful.
    """
    # ----------------------------------------------------------------------
    # Column: type of aggregation
    (COL_TYPE,) = range(5000, 5001)
    # Column: One x
    (COL_LABEL, COL_COUNT, COL_TOTAL,
     COL_MIN_TOTAL, COL_MIN_TOTAL_NUMBER, COL_MAX_TOTAL, COL_MAX_TOTAL_LABELS,
     ) = range(5100, 5107)
    # Column: All x
    (COLS_TOTAL,
     COLS_MIN_TOTAL, COLS_MIN_TOTAL_NUMBER, COLS_MEAN_TOTAL, COLS_MEDIAN_TOTAL,
     COLS_MAX_TOTAL, COLS_MAX_TOTAL_LABELS,
     COLS_COUNT,
     COLS_MIN_COUNT, COLS_MIN_COUNT_NUMBER, COLS_MEAN_COUNT, COLS_MEDIAN_COUNT,
     COLS_MAX_COUNT, COLS_MAX_COUNT_LABELS
     ) = range(5200, 5214)
    # Row: aggregation of what type of data
    (ROW_TYPE,) = range(6000, 6001)
    # Row: One y
    (ROW_LABEL, ROW_COUNT, ROW_TOTAL,
     ROW_MIN_TOTAL, ROW_MIN_TOTAL_NUMBER, ROW_MAX_TOTAL, ROW_MAX_TOTAL_LABELS,
     ) = range(6100, 6107)
    # Rows: All y
    (ROWS_TOTAL,
     ROWS_MIN_TOTAL, ROWS_MIN_TOTAL_NUMBER, ROWS_MEAN_TOTAL, ROWS_MEDIAN_TOTAL,
     ROWS_MAX_TOTAL, ROWS_MAX_TOTAL_LABELS,
     ROWS_COUNT,
     ROWS_MIN_COUNT, ROWS_MIN_COUNT_NUMBER, ROWS_MEAN_COUNT, ROWS_MEDIAN_COUNT,
     ROWS_MAX_COUNT, ROWS_MAX_COUNT_LABELS
     ) = range(6200, 6214)
    # Type of aggregation
    (TYPE,) = range(0, 1)
    # One field of row/column header
    (ONE_LABEL, ONE_COUNT, ONE_TOTAL,
     ONE_MIN_COUNT, ONE_MIN_COUNT_NUMBER,
     ONE_MAX_COUNT, ONE_MAX_COUNT_LABELS
     ) = range(100, 107)
    # Column: All row/column headers
    (ALL_TOTAL,
     ALL_MIN_TOTAL, ALL_MIN_TOTAL_NUMBER, ALL_MEAN_TOTAL, ALL_MEDIAN_TOTAL,
     ALL_MAX_TOTAL, ALL_MAX_TOTAL_LABELS,
     ALL_COUNT,
     ALL_MIN_COUNT, ALL_MIN_COUNT_NUMBER, ALL_MEAN_COUNT, ALL_MEDIAN_COUNT,
     ALL_MAX_COUNT, ALL_MAX_COUNT_LABELS,
     ) = range(200, 214)

    # ...............................................
    @classmethod
    def get_keys_for_table(cls, table_type):
        """Return keystrings for statistics dictionary for specific aggregation tables.

        Args:
            table_type (aws_constants.SUMMARY_TABLE_TYPES): type of aggregated data

        Returns:
            keys (dict): Dictionary of strings to be used as keys for each type of
                value in a dictionary of statistics.

        Raises:
            Exception: on un-implemented table type.
        """
        datacontents, dim0, dim1, datatype = SUMMARY.parse_table_type(table_type)
        if datatype == "matrix":
            # dim0 is row/axis0, dim1 is column/axis1, matrix values are occurrences
            keys = {
                # ----------------------------------------------------------------------
                # Column
                # -----------------------------
                cls.COL_TYPE: dim1,
                # One dataset
                cls.COL_LABEL: f"{dim1}_label",
                # Count (non-zero elements in column)
                cls.COL_COUNT: f"total_{dim0}_for_{dim1}",
                # Values (total of values in column)
                cls.COL_TOTAL: f"total_occurrences_for_{dim1}",
                # Values: Minimum occurrences for one dataset, species labels
                cls.COL_MIN_TOTAL: f"min_occurrences_for_{dim1}",
                cls.COL_MIN_TOTAL_NUMBER: f"number_of_{dim0}_with_min_occurrences_for_{dim1}",
                # Values: Maximum occurrence count for one dataset, species labels
                cls.COL_MAX_TOTAL: f"max_occurrences_for_{dim1}",
                cls.COL_MAX_TOTAL_LABELS: f"{dim0}_with_max_occurrences_for_{dim1}",
                # -----------------------------
                # All dimension1
                # ------------
                # Values: Total of all occurrences for all dim1 - stats
                cls.COLS_TOTAL: f"total_occurrences_of_all_{dim1}",
                cls.COLS_MIN_TOTAL: f"min_occurrences_of_all_{dim1}",
                cls.COLS_MIN_TOTAL_NUMBER: f"number_of_{dim1}_with_min_occurrences_of_all",
                cls.COLS_MEAN_TOTAL: f"mean_occurrences_of_all_{dim1}",
                cls.COLS_MEDIAN_TOTAL: f"median_occurrences_of_all_{dim1}",
                cls.COLS_MAX_TOTAL: f"max_occurrences_of_all_{dim1}",
                cls.COLS_MAX_TOTAL_LABELS: f"{dim1}_with_max_occurrences_of_all",
                # ------------
                # Counts: Count of all species (from all columns/dim1)
                cls.COLS_COUNT: f"total_{dim1}_count",
                # Species count for all datasets - stats
                cls.COLS_MIN_COUNT: f"min_{dim0}_count_of_all_{dim1}",
                cls.COLS_MIN_COUNT_NUMBER: f"number_of_{dim1}_with_min_{dim0}_count_of_all",
                cls.COLS_MEAN_COUNT: f"mean_{dim0}_count_of_all_{dim1}",
                cls.COLS_MEDIAN_COUNT: f"median_{dim0}_count_of_all_{dim1}",
                cls.COLS_MAX_COUNT: f"max_{dim0}_count_of_all_{dim1}",
                cls.COLS_MAX_COUNT_LABELS: f"{dim1}_with_max_{dim0}_count_of_all",
                # ----------------------------------------------------------------------
                # Row
                # -----------------------------
                cls.ROW_TYPE: dim0,
                # One species
                cls.ROW_LABEL: f"{dim0}_label",
                # Count (non-zero elements in row)
                cls.ROW_COUNT: f"total_{dim1}_for_{dim0}",
                # Values (total of values in row)
                cls.ROW_TOTAL: f"total_occurrences_for_{dim0}",
                # Values: Minimum occurrence count for one species, dataset labels, indexes
                cls.ROW_MIN_TOTAL: f"min_occurrences_for_{dim0}",
                # Values: Maximum occurrence count for one species, dataset labels, indexes
                cls.ROW_MAX_TOTAL: f"max_occurrences_for_{dim0}",
                cls.ROW_MAX_TOTAL_LABELS: f"{dim1}_with_max_occurrences_for_{dim0}",
                # -----------------------------
                # All species
                # ------------
                # Values: Total of all occurrences for all dim0 - stats
                cls.ROWS_TOTAL: f"total_occurrences_of_all_{dim0}",
                cls.ROWS_MIN_TOTAL: f"min_occurrences_of_all_{dim0}",
                cls.ROWS_MIN_TOTAL_NUMBER: f"number_of_{dim0}_with_max_occurrences_of_all",
                cls.ROWS_MEAN_TOTAL: f"mean_occurrences_of_all_{dim0}",
                cls.ROWS_MEDIAN_TOTAL: f"median_occurrences_of_all_{dim0}",
                cls.ROWS_MAX_TOTAL: f"max_occurrences_of_all_{dim0}",
                cls.ROWS_MAX_TOTAL_LABELS: f"{dim0}_with_max_occurrences_of_all",
                # ------------
                # Counts: Count of all datasets (from all rows/species)
                cls.ROWS_COUNT: f"total_{dim0}_count",
                # Dataset count for all species - stats
                cls.ROWS_MIN_COUNT: f"min_{dim1}_count_of_all_{dim0}",
                cls.ROWS_MIN_COUNT_NUMBER: f"{dim0}_with_min_{dim1}_count_of_all",
                cls.ROWS_MEAN_COUNT: f"mean_{dim1}_count_of_all_{dim0}",
                cls.ROWS_MEDIAN_COUNT: f"median_{dim1}_count_of_all_{dim0}",
                cls.ROWS_MAX_COUNT: f"max_{dim1}_count_of_all_{dim0}",
                cls.ROWS_MAX_COUNT_LABELS: f"{dim0}_with_max_{dim1}_count_of_all",
            }
        elif datatype == "summary":
            # dim0 is the summary dimension
            keys = {
                # ----------------------------------------------------------------------
                # Column
                # -----------------------------
                cls.TYPE: dim0,
                # One dataset
                cls.ONE_LABEL: f"{dim0}_label",
                # Count (non-zero elements in column)
                cls.ONE_COUNT: f"total_{dim1}_for_{dim0}",
                # Values (total of values in column)
                cls.ONE_TOTAL: f"total_occurrences_for_{dim0}",
                # Values: Minimum occurrence count for one dataset
                cls.ONE_MIN_COUNT: f"min_occurrences_for_{dim0}",
                cls.ONE_MIN_COUNT_NUMBER: f"number_of_{dim0}_with_min_occurrences",
                # Values: Maximum occurrence count for one dataset, species labels, indexes
                cls.ONE_MAX_COUNT: f"max_occurrences_for_{dim0}",
                cls.ONE_MAX_COUNT_LABELS: f"{dim0}_with_max_occurrences",
                # -----------------------------
                # All datasets
                # ------------
                # Values: Total of all occurrences for all datasets - stats
                cls.ALL_TOTAL: f"total_occurrences_of_all_{dim0}",
                cls.ALL_MIN_TOTAL: f"min_occurrences_of_all_{dim0}",
                cls.ALL_MIN_TOTAL_NUMBER: f"number_of_{dim0}_with_min_occurrences_of_all",
                cls.ALL_MEAN_TOTAL: f"mean_occurrences_of_all_{dim0}",
                cls.ALL_MEDIAN_TOTAL: f"median_occurrences_of_all_{dim0}",
                cls.ALL_MAX_TOTAL: f"max_occurrences_of_all_{dim0}",
                # ------------
                # Counts: Count of all species (from all columns/datasets)
                cls.ALL_COUNT: f"total_{dim1}_count",
                # Species count for all datasets - stats
                cls.ALL_MIN_COUNT: f"min_{dim1}_count_of_all_{dim0}",
                cls.ALL_MEAN_COUNT: f"mean_{dim1}_count_of_all_{dim0}",
                cls.ALL_MEDIAN_COUNT: f"median_{dim1}_count_of_all_{dim0}",
                cls.ALL_MAX_COUNT: f"max_{dim1}_count_of_all_{dim0}",
            }
        else:
            raise Exception(f"Keys not defined for {datatype} table")
        return keys
