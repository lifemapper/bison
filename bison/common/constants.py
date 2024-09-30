"""Constants to use locally initiate BISON AWS EC2 Spot Instances."""
import copy
from enum import Enum
import os

PROJ_NAME = "bison"
ENCODING = "utf-8"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

TMP_PATH = "/tmp"

S3_BUCKET = f"{PROJ_NAME}-321942852011-us-east-1"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_SUMMARY_DIR = "summary"

WORKFLOW_ROLE = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
SPOT_TEMPLATE_BASENAME = "launch_template"

KEY_NAME = "aimee-aws-key"
REGION = "us-east-1"
# Allows KU Dyche hall
SECURITY_GROUP_ID = "sg-0b379fdb3e37389d1"
SECRET_NAME = "admin_bison-db-test"

# S3
TRIGGER_PATH = "trigger"
TRIGGER_FILENAME = "go.txt"

# EC2 Spot Instance
# List of instance types at https://aws.amazon.com/ec2/spot/pricing/
INSTANCE_TYPE = "t2.micro"
# INSTANCE_TYPE = "a1.large"

# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5
ERR_SEPARATOR = "------------"

USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"

COUNT_FLD = "count"
TOTAL_FLD =  "total"

CSV_DELIMITER = ","

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
SPECIES_DIM = {
    "name": "species",
    "key_fld": "taxonkey_species"
}


# .............................................................................
class ANALYSIS_DIM:
    STATE = {
        "name": "state",
        "fields"
        "key_fld": "census_state"
    }
    COUNTY = {
        "name": "county",
        "key_fld": "census_county"
    }
    AIANNH = {
        "name": "aiannh",
        "key_fld": "aiannh_name"
    }


# .............................................................................
class SUMMARY:
    """Types of tables stored in S3 for aggregate species data analyses."""
    dt_token = "YYYY_MM_DD"
    sep = "_"
    dim_sep = "-x-"

    @classmethod
    def lists(cls):
        """Records of dimension, species, occ counts for each dimension in project.

        Returns:
            lists (dict): dict of dictionaries for each lists table defined by the
                project.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        lists = {}
        for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
            # name == table_type, ex: county-x-species_list
            name = f"{dim['name']}{cls.dim_sep}{SPECIES_DIM['name']}{cls.sep}list"
            meta = {
                "code": name,
                "fname": f"{name}{cls.sep}{cls.dt_token}{cls.sep}000",
                "format": "Parquet",

                # "fields": [DATASET_GBIF_KEY, "taxonkey", "species", "occ_count"],
                # "key_fld": DATASET_GBIF_KEY,
                # "species_fld": "species",
                # "combine_fields": {"taxonkey_species": ("taxonkey", "species")},
                # "value_fld": "occ_count",

                "fields": dim["fields"],
                "key_fld": dim["key_fld"]
            }
            lists[name] = meta
        return lists

    # # ...........................
    # @classmethod
    # def counts(cls):
    #     """Tables of species and occurrence counts for each dimension in project.
    #
    #     Returns:
    #         counts (dict): dict of dictionaries for each counts table defined by the
    #             project.
    #     """
    #     counts = {}
    #     for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
    #         # name == table_type
    #         name = f"{dim['name']}{cls.sep}count"
    #         meta = {
    #             "code": name,
    #             "fname": f"{name}{cls.sep}{cls.dt_token}{cls.sep}000",
    #             "format": "Parquet",
    #             "fields": [],
    #             "key_fld": "???"
    #         }
    #         counts[name] = meta
    #     return counts

    # ...........................
    @classmethod
    def summaries(cls):
        """Summary tables of species and occurrence counts for each dimension in project.

        Returns:
            sums (dict): dict of dictionaries for each summary table defined by the
                project.
        """
        sums = {}
        for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
            # Species in rows, ex: species-x-county_summary
            name1 = f"{SPECIES_DIM['name']}{cls.dim_sep}{dim['name']}_summary"
            meta1 = {
                "code": name1,
                "fname": f"{name1}{cls.sep}{cls.dt_token}",
                "aggregate_type": f"{SPECIES_DIM['name']}_summary",
                # Axis 0, matches row (axis 0) in SPECIES_<dimension>_MATRIX
                "row": SPECIES_DIM["key_fld"],
            }
            # Dimension X in rows, ex: county-x-species_summary
            name2 = f"{dim['name']}{cls.dim_sep}{SPECIES_DIM['name']}_summary"
            meta2 = {
                "code": name2,
                "fname": f"{name2}{cls.sep}{cls.dt_token}",
                "aggregate_type": f"{dim['name']}_summary",
                # Axis 0, matches column (axis 1) in SPECIES_DATASET_MATRIX
                "row": dim["key_fld"],
            }
            for m in meta1, meta2:
                m.update({
                    "table_format": "Zip",
                    "matrix_extension": ".csv",
                    # Axis 1
                    "column": "measurement_type",
                    "fields": [COUNT_FLD, TOTAL_FLD],
                    # Matrix values
                    "value": "measure"
                })
                sums[name1] = meta1
                sums[name2] = meta2
        return sums

    # ...........................
    @classmethod
    def matrices(cls):
        """Species by <dimension> matrices defined for this project.

        Returns:
            mtxs (dict): dict of dictionaries for each matrix/table defined for this
                project.
        """
        mtxs = {}
        for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
            # ex: species-x-county_matrix
            name = f"{SPECIES_DIM['name']}{cls.dim_sep}{dim['name']}{cls.sep}matrix"
            row_input = f"{SPECIES_DIM['name']}{cls.dim_sep}{dim['name']}{cls.sep}summary"
            col_input = f"{dim['name']}{cls.dim_sep}{SPECIES_DIM['name']}{cls.sep}summary"
            # Dimension 0/row is always species
            meta = {
                "code": name,
                "fname": f"{name}{cls.sep}{cls.dt_token}",
                "table_format": "Zip",
                "matrix_extension": ".npz",
                "data_type": "matrix",
                # Axis 0
                "row": SPECIES_DIM["key_fld"],
                "row_input": cls.summaries()[row_input],
                # Axis 1
                "column": dim["key_fld"],
                "column_summary_table": cls.summaries()[col_input],
                # Matrix values
                "value": "occ_count",
            }
            mtxs[name] = meta
        return mtxs

    # ...............................................
    @classmethod
    def parse_table_type(cls, table_type):
        tbl = cls.get_table(table_type)
        parts = table_type.split("_")
        if len(parts) == 2:
            datacontents, datetype = parts
        else:
            raise Exception(f"Failed to parse {table_type} into datacontents, datetype.")
        return datacontents, datetype

    # ...............................................
    @classmethod
    def tables(cls, datestr=None):
        """All tables of species and occurrence counts, summaries, and matrices.

        Returns:
            sums (dict): dict of dictionaries for each table defined by the project.
                If datestr is provided, the token in the filename is replaced with that.

        Note:
            The keys for the dictionary (and code in the metadata values) are table_type
        """
        tables = cls.lists()
        tables.update(cls.summaries())
        tables.update(cls.matrices())
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
                (records, lists, matrix, etc).

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
            table_type: type of summary table to return.
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
        fn_parts = table_type.split(cls.sep)
        if len(fn_parts) >= 2:
            datacontents = fn_parts[0]
            try:
                dim0, dim1 = datacontents.split(cls.dim_sep)
            except ValueError:
                dim0 = dim1 = None
            datatype = fn_parts[1]
        else:
            raise Exception(f"{table_type} does not follow the expected pattern")
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
                # Species counts for all datasets - stats
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
                # Dataset counts for all species - stats
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
                # Species counts for all datasets - stats
                cls.ALL_MIN_COUNT: f"min_{dim1}_count_of_all_{dim0}",
                cls.ALL_MEAN_COUNT: f"mean_{dim1}_count_of_all_{dim0}",
                cls.ALL_MEDIAN_COUNT: f"median_{dim1}_count_of_all_{dim0}",
                cls.ALL_MAX_COUNT: f"max_{dim1}_count_of_all_{dim0}",
            }
        else:
            raise Exception(f"Keys not defined for {datatype} table")
        return keys
