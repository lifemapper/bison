"""Lambda function to aggregate counts and lists by region."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading function bison_s6_aggregate_region")
PROJECT = "bison"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
bison_datestr = f"{yr}_{mo:02d}_01"
bison_s3_datestr = bison_datestr.replace('_', '-')
gbif_datestr = f"{yr}-{mo:02d}-01"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"

# EC2 launch template/version
EC2_SPOT_TEMPLATE = "bison_spot_task_template"
TASK = "test_task"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"
s3_summary_prefix = f"{S3_SUMMARY_DIR}/"
s3_summary_suffix = "_000.parquet"
s3_out = f"s3://{S3_BUCKET}/summary"

# Redshift
# namespace, workgroup both = 'bison'
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5
# Name the Redshift mounted gbif data and bison table to create from it
bison_tbl = f"bison_{bison_datestr}"
mounted_gbif_name = f"{external_schema}.occurrence_{bison_datestr}_parquet"

# .............................................................................
# Initialize Botocore session and clients
# .............................................................................
timeout = 300
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=REGION)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
s3_client = session.client("s3", config=config, region_name=REGION)
rs_client = session.client("redshift-data", config=config)

# .............................................................................
# Ancillary data parameters
# .............................................................................
RIIS_BASENAME = "USRIISv2_MasterList"
riis_fname = f"{RIIS_BASENAME}_annotated_{bison_datestr}.csv"
riis_tbl = f"riisv2_{bison_datestr}"

# Each fields tuple contains original fieldname, bison fieldname and bison fieldtype
ancillary_data = {
    "aiannh": {
        "table": "aiannh2023",
        "filename": "cb_2023_us_aiannh_500k.shp",
        "fields": {
            "name": ("namelsad", "aiannh_name", "VARCHAR(100)"),
            "geoid": ("geoid", "aiannh_geoid", "VARCHAR(4)")
        }
    },
    "county": {
        "table": "county2023",
        "filename": "cb_2023_us_county_500k.shp",
        "fields": {
            "state": ("stusps", "census_state", "VARCHAR(2)"),
            "county": ("namelsad", "census_county", "VARCHAR(100)"),
            "state_county": (None, "state_county", "VARCHAR(102)")
        }
    },
    "riis": {
        "table": riis_tbl,
        "filename": riis_fname,
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

join_fld = "gbifid"
gbif_tx_fld = "taxonkey"
gbif_sp_fld = "species"
# Fields concatenated to ensure uniqueness
unique_sp_fld = "taxonkey_species"
unique_cty_fld = "state_county"
out_occcount_fld = "occ_count"
out_spcount_fld = "species_count"

# Get table, field names
cty_data = ancillary_data["county"]
b_st_fld = cty_data["fields"]["state"][1]
b_cty_fld = cty_data["fields"]["county"][1]

county_counts_tbl = f"county_counts_{bison_datestr}"
county_x_riis_counts_tbl = f"county_x_riis_counts{bison_datestr}"
county_list_tbl = f"county_x_species_list_{bison_datestr}"
# # Redshift does not accept '-', but use that for parsing S3 filenames
# county_list_s3key = county_list_tbl.replace("_", "-")
# county_x_riis_counts_s3key = county_x_riis_counts_tbl.replace("_", "-")
# county_counts_s3key = county_counts_tbl.replace("_", "-")

state_counts_tbl = f"state_counts_{bison_datestr}"
state_x_riis_counts_tbl = f"state_x_riis_counts{bison_datestr}"
state_list_tbl = f"state_x_species_list_{bison_datestr}"
# Redshift does not accept '-', but use that for parsing S3 filenames
# state_x_riis_counts_s3key = state_x_riis_counts_tbl.replace("_", "-")
# state_list_s3key = state_list_tbl.replace("_", "-")
# state_counts_s3key = state_counts_tbl.replace("_", "-")

aiannh_data = ancillary_data["aiannh"]
b_nm_fld = aiannh_data["fields"]["name"][1]
b_gid_fld = aiannh_data["fields"]["geoid"][1]

aiannh_counts_tbl = f"aiannh_counts_{bison_datestr}"
aiannh_x_riis_counts_tbl = f"aiannh_x_riis_counts{bison_datestr}"
aiannh_list_tbl = f"aiannh_x_species_list_{bison_datestr}"
# # Redshift does not accept '-', but use that for parsing S3 filenames
# aiannh_list_s3key = aiannh_list_tbl.replace("_", "-")
# aiannh_x_riis_counts_s3key = aiannh_x_riis_counts_tbl.replace("_", "-")
# aiannh_counts_s3key = aiannh_counts_tbl.replace("_", "-")

# RIIS data
riis_data = ancillary_data["riis"]
b_loc_fld = riis_data["fields"]["locality"][1]
b_occid_fld = riis_data["fields"]["occid"][1]
b_ass_fld = riis_data["fields"]["assess"][1]
# ...............................................
# Aggregate counts by region
# ...............................................
# Aggregate occurrence, species counts, RIIS status by region
# TODO: add RIIS assessment counts
state_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{state_counts_tbl} AS
        SELECT DISTINCT {b_st_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {b_st_fld};
"""
state_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_counts_tbl} ORDER BY {b_st_fld}')
        TO '{s3_out}/{state_counts_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
state_x_riis_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{state_x_riis_counts_tbl} AS
        SELECT DISTINCT {b_st_fld}, {b_ass_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {b_st_fld}, {b_ass_fld};
"""
state_x_riis_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_list_tbl} ORDER BY {b_st_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{state_list_tbl}_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
# Note: in county agggregate, include states bc county names are not unique
county_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{county_counts_tbl} AS
        SELECT DISTINCT {unique_cty_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {unique_cty_fld};
"""
county_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{county_counts_tbl} ORDER BY {unique_cty_fld}')
        TO '{s3_out}/{county_counts_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
county_x_riis_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{county_x_riis_counts_tbl} AS
        SELECT DISTINCT {unique_cty_fld}, {b_ass_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {unique_cty_fld}, {b_ass_fld};
"""
# TODO: county_x_riis_counts_export_stmt
aiannh_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{aiannh_counts_tbl} AS
        SELECT DISTINCT {b_nm_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_nm_fld} IS NOT NULL
                            AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {b_nm_fld};
"""
aiannh_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{aiannh_counts_tbl} ORDER BY {b_nm_fld}')
        TO '{s3_out}/{aiannh_counts_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
aiannh_x_riis_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{aiannh_x_riis_counts_tbl} AS
        SELECT DISTINCT {b_nm_fld}, {b_ass_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_nm_fld} IS NOT NULL
                            AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {b_nm_fld}, {b_ass_fld};
"""
# TODO: aiannh_x_riis_counts_export_stmt
# ...............................................
# Records of species, assessment, occ_count by region
# ...............................................
# Create species lists with counts and RIIS status for state, county, aiannh
state_list_stmt = f"""
    CREATE TABLE {pub_schema}.{state_list_tbl} AS
        SELECT DISTINCT {b_st_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld},
            {b_ass_fld}, COUNT(*) AS {out_occcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {b_st_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld}, {b_ass_fld};
"""
state_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_list_tbl} ORDER BY {b_st_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{state_list_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""

county_list_stmt = f"""
    CREATE TABLE {pub_schema}.{county_list_tbl} AS
        SELECT DISTINCT {unique_cty_fld}, {b_st_fld}, {b_cty_fld}, {unique_sp_fld},
            {gbif_tx_fld}, {gbif_sp_fld}, {b_ass_fld}, COUNT(*) AS {out_occcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {unique_cty_fld}, {b_st_fld}, {b_cty_fld}, {unique_sp_fld}, {gbif_tx_fld},
                 {gbif_sp_fld}, {b_ass_fld};
"""
county_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{county_list_tbl} ORDER BY {unique_cty_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{county_list_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""

aiannh_list_stmt = f"""
    CREATE TABLE {pub_schema}.{aiannh_list_tbl} AS
        SELECT DISTINCT {b_nm_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld},
            {b_ass_fld}, COUNT(*) AS {out_occcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL AND {gbif_sp_fld} IS NOT NULL
        GROUP BY {b_nm_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld}, {b_ass_fld};
"""
aiannh_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{aiannh_list_tbl} ORDER BY {b_nm_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{aiannh_list_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
# ...............................................
# CSV exports for delivery
# ...............................................
state_counts_export_stmt2 = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_counts_tbl} ORDER BY {b_st_fld}')
        TO '{s3_out}/{state_counts_tbl}_csv_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        CSV DELIMITER AS ','
        HEADER
        PARALLEL OFF;
"""
county_counts_export_stmt2 = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{county_counts_tbl} ORDER BY {unique_cty_fld}')
        TO '{s3_out}/{county_counts_tbl}_csv_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        CSV DELIMITER AS ','
        HEADER
        PARALLEL OFF;
"""
aiannh_counts_export_stmt2 = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{aiannh_counts_tbl} ORDER BY {b_nm_fld}')
        TO '{s3_out}/{aiannh_counts_tbl}_csv_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        CSV DELIMITER AS ','
        HEADER
        PARALLEL OFF;
"""
state_list_export_stmt2 = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_list_tbl} ORDER BY {b_st_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{state_list_tbl}_csv_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        CSV DELIMITER AS ','
        HEADER
        PARALLEL OFF;
"""
county_list_export_stmt2 = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{county_list_tbl} ORDER BY {unique_cty_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{county_list_tbl}_csv_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        CSV DELIMITER AS ','
        HEADER
        PARALLEL OFF;
"""
aiannh_list_export_stmt2 = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{aiannh_list_tbl} ORDER BY {b_nm_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{aiannh_list_tbl}_csv_'
        IAM_role DEFAULT
        ALLOWOVERWRITE
        CSV DELIMITER AS ','
        HEADER
        PARALLEL OFF;
"""
query_tables_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '%_{bison_datestr}';"

REDSHIFT_COMMANDS = [
    ("query_tables", query_tables_stmt, None),
    # Create tables of region with species counts, occurrence counts
    ("create_counts_by_state", state_counts_stmt, state_counts_tbl),
    ("export_state_counts", state_counts_export_stmt, state_counts_tbl),
    ("export_state_counts_csv", state_counts_export_stmt2, state_counts_tbl),
    ("create_counts_by_county", county_counts_stmt, county_counts_tbl),
    ("export_county_counts", county_counts_export_stmt, county_counts_tbl),
    ("export_county_counts_csv", county_counts_export_stmt2, county_counts_tbl),
    ("create_counts_by_aiannh", aiannh_counts_stmt, aiannh_counts_tbl),
    ("export_aiannh_counts", aiannh_counts_export_stmt, aiannh_counts_tbl),
    ("export_aiannh_counts_csv", aiannh_counts_export_stmt2, aiannh_counts_tbl),
    # Create lists of region with species, riis status, occurrence counts, then export
    ("create_list_state_species", state_list_stmt, state_list_tbl),
    ("export_state_species", state_list_export_stmt, state_list_tbl),
    ("export_state_species_csv", state_list_export_stmt2, state_list_tbl),
    ("create_list_county_species", county_list_stmt, county_list_tbl),
    ("export_county_species", county_list_export_stmt, county_list_tbl),
    ("export_county_species_csv", county_list_export_stmt2, county_list_tbl),
    ("create_list_aiannh_species", aiannh_list_stmt, aiannh_list_tbl),
    ("export_aiannh_species", aiannh_list_export_stmt, aiannh_list_tbl),
    ("export_aiannh_species_csv", aiannh_list_export_stmt2, aiannh_list_tbl),
    ]


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Aggregate BISON records to species/occurrence counts and species lists by region.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    tables_present = []
    s3objs_present = []
    # -------------------------------------
    # FIRST: List current summary data from S3
    # -------------------------------------
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=s3_summary_prefix, MaxKeys=10)
    except Exception as e:
        print(f"*** Error querying for objects in {s3_summary_prefix} ({e})")
    else:
        try:
            contents = tr_response["Contents"]
        except KeyError:
            print(f"*** No values in {S3_BUCKET}/{s3_summary_prefix}")
        else:
            prefix_len = len(s3_summary_prefix)
            suffix_len = len(s3_summary_suffix)
            print(f"*** S3 summaries of {bison_s3_datestr}")
            for rec in contents:
                fullkey = rec["Key"]
                if fullkey.find(bison_s3_datestr) > prefix_len:
                    key = fullkey[prefix_len:-suffix_len]
                    s3objs_present.append(key)
                    print(f"***      {key}")

    # -------------------------------------
    # SECOND: Check that current BISON data is in Redshift
    # NEXT: Execute remaining aggregation and export commmands in order
    # -------------------------------------
    for (cmd, stmt, out_obj) in REDSHIFT_COMMANDS:
        if (
                (cmd.startswith("create") and out_obj in tables_present) or
                (cmd.startswith("export") and out_obj in s3objs_present)
        ):
            print("*** ---------------------------------------")
            print(f"*** Skipped command {cmd.upper()}, {out_obj} is present.")
        else:
            # Do not stop after a failure
            # -------------------------------------
            try:
                submit_result = rs_client.execute_statement(
                    WorkgroupName=PROJECT, Database=database, Sql=stmt)
            except Exception:
                raise

            print("*** ---------------------------------------")
            print(f"*** {cmd.upper()} command submitted")
            print(stmt)
            submit_id = submit_result['Id']

            # -------------------------------------
            # Loop til complete
            elapsed_time = 0
            complete = False
            while not complete:
                try:
                    describe_result = rs_client.describe_statement(Id=submit_id)
                except Exception as e:
                    complete = True
                    print(f"!!! Failed to describe_statement {e}")
                else:
                    status = describe_result["Status"]
                    if status in ("ABORTED", "FAILED", "FINISHED"):
                        complete = True
                        print(f"*** Status - {status} after {elapsed_time} seconds")
                        if status in ("ABORTED", "FAILED"):
                            try:
                                err = describe_result["Error"]
                            except Exception:
                                err = "Unknown Error"
                            print(f"!!!    FAILED: {err}")
                    else:
                        time.sleep(waittime)
                        elapsed_time += waittime

            # -------------------------------------
            # First redshift statement
            # -------------------------------------
            if cmd == "query_tables":
                try:
                    stmt_result = rs_client.get_statement_result(Id=submit_id)
                except Exception as e:
                    print(f"!!! No get_statement_result {e}")
                    raise

                try:
                    records = stmt_result["Records"]
                except Exception as e:
                    print(f"!!! Failed to return any records ({e})")
                else:
                    print(f"*** Summaries of {bison_datestr}")
                    try:
                        for rec in records:
                            tbl_name = rec[2]["stringValue"]
                            tables_present.append(tbl_name)
                            print(f"***      {tbl_name}")
                    except Exception as e:
                        raise Exception(f"!!! Unexpected record result {rec}, {e}")

    return {
        "statusCode": 200,
        "body": "Executed bison_s6_aggregate_region lambda"
    }
