"""Constants to use locally initiate BISON AWS EC2 Spot Instances."""
import copy
import os

PROJ_NAME = "bison"
ENCODING = "utf-8"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

PROJ_BUCKET = f"{PROJ_NAME}-321942852011-us-east-1"
PROJ_INPUT_PATH = "input"
PROJ_ROLE = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
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

COUNT_FLD = "count"
TOTAL_FLD =  "total"

# .............................................................................
SPECIES_DIM = {
    "name": "species",
    "key_fld": "taxonkey_species"
}


# .............................................................................
class ANALYSIS_DIM:
    STATE = {
        "name": "state",
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
    @classmethod
    def counts(cls):
        counts = {}
        for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
            name = f"{dim['name']}_count"
            meta = {
                "code": name,
                "fname": f"{name}_{cls.dt_token}_000",
                "format": "Parquet",
                "fields": [],
                "key_fld": "???"
            }
            counts[name] = meta
        return counts
    # ...........................
    @classmethod
    def summaries(cls):
        sums = {}
        for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
            # Species in rows
            name1 = f"{SPECIES_DIM['name']}_{dim['name']}_summary"
            meta1 = {
                "code": name1,
                "fname": f"{name1}_{cls.dt_token}",
                # Axis 0, matches row (axis 0) in SPECIES_<dimension>_MATRIX
                "row": SPECIES_DIM["key_fld"],
            }
        # Dimension X in rows
        name2 = f"{dim['name']}_{SPECIES_DIM['name']}_summary"
        meta2 = {
            "code": name2,
            "fname": f"{name2}_{cls.dt_token}",
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
        mtxs = {}
        for dim in [ANALYSIS_DIM.STATE, ANALYSIS_DIM.COUNTY, ANALYSIS_DIM.AIANNH]:
            name = f"{SPECIES_DIM['name']}_{dim.name}_matrix"
            row_input = f"{SPECIES_DIM['name']}_{dim['name']}_summary"
            col_input = f"{dim['name']}_{SPECIES_DIM['name']}_summary"
            # Dimension 0/row is always species
            meta = {
                "code": name,
                "fname": f"{name}_{cls.dt_token}",
                "table_format": "Zip",
                "matrix_extension": ".npz",
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

    # DATASET_COUNTS = "dataset_counts"
    # DATASET_SPECIES_LISTS = "dataset_species_lists"
    # DATASET_META = "dataset_meta"
    # SPECIES_DATASET_MATRIX = "species_dataset_matrix"
    # SPECIES_DATASET_SUMMARY = "species_dataset_summary"
    # DATASET_SPECIES_SUMMARY = "dataset_species_summary"

    # ...............................................
    @classmethod
    def update_summary_tables(cls, datestr):
        """Update filenames in the metadata dictionary and return.

        Args:
            datestr: Datestring contained in the filename indicating the current version
                of the data.

        Returns:
            tables: dictionary of summary table metadata.
        """
        tables = {}
        # Update filename in summary tables
        for key, meta in cls.TABLES.items():
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
            cpy_table = copy.deepcopy(cls.TABLES[table_type])
        except KeyError:
            return None
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
        table_type = None
        for key, meta in cls.TABLES.items():
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
        fname_tmpl = cls.TABLES[table_type]["fname"]
        fname = fname_tmpl.replace(cls.dt_token, datestr)
        return fname

    # ...............................................
    @classmethod
    def _parse_filename(cls, filename):
        # <datacontents>_<datatype>_<YYYY_MM_DD><_optional parquet extension>
        fname = os.path.basename(filename)
        fname_noext, _ext = os.path.splitext(fname)
        fn_parts = fname_noext.split("_")
        if len(fn_parts) >= 5:
            datacontents = fn_parts[0]
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
        return datacontents, datatype, data_datestr, rest

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
            datacontents, datatype, data_datestr, _rest = cls._parse_filename(filename)
            table_type = cls.get_tabletype_from_filename_prefix(datacontents, datatype)
        except Exception:
            raise
        return table_type, data_datestr
