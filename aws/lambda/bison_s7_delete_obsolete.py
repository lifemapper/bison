"""Lambda function to delete temporary and previous month's tables."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
from pprint import pp

import time

print("*** Loading function bison_s6_delete_obsolete")
PROJECT = "bison"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
# Redshift date format
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"
# S3 date format
gbif_datestr = f"{yr}-{mo:02d}-01"
old_bison_s3_datestr = old_bison_datestr.replace('_', '-')

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"
s3_obsolete_prefix = f"{S3_SUMMARY_DIR}/"
s3_obsolete_pattern = ""
# Redshift
# namespace, workgroup both = 'bison'
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5

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
annotated_riis_key = f"{S3_IN_DIR}/{riis_fname}"
old_annotated_riis_key = f"{S3_IN_DIR}/{RIIS_BASENAME}_annotated_{old_bison_datestr}.csv"
riis_tbl = f"riisv2_{bison_datestr}"
old_riis_tbl = f"riisv2_{old_bison_datestr}"

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
            # Field constructed to ensure uniqueness
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

aiannh_fname = ancillary_data["aiannh"]["filename"]
aiannh_tbl = ancillary_data["aiannh"]["table"]
county_fname = ancillary_data["county"]["filename"]
county_tbl = ancillary_data["county"]["table"]


# Current temp tables to be deleted
tmp_prefix = "tmp_bison"

# Queries for obsolete tables to be deleted
QUERY_COMMANDS = (
    (
        "query_old",
        f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '%{old_bison_datestr}';"
    ),
    (
        "query_tmp",
        f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} "
        f"LIKE '{tmp_prefix}%{bison_datestr}';"
    )
)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Delete previous month data and current month temporary tables.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift query command.
        Exception: on failure to execute Redshift drop command.
    """
    tables_to_remove = []
    for cmd, stmt in QUERY_COMMANDS:
        # -------------------------------------
        # Submit query request
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)

        submit_id = submit_result['Id']
        print("*** ......................")
        print(f"*** {cmd.upper()} command submitted with Id {submit_id}")
        print(f"***    {stmt}")

        # -------------------------------------
        # Loop til complete, then get result status
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = rs_client.describe_statement(Id=submit_id)
            except Exception as e:
                complete = True
                print(f"Failed to describe_statement {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    complete = True
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    if status == "FAILED":
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

        # -------------------------------------
        # Get list of tables
        time.sleep(waittime * 2)
        try:
            stmt_result = rs_client.get_statement_result(Id=submit_id)
        except Exception as e:
            print(f"*** No get_statement_result {e}")
        else:
            print("*** Tables to remove:")
            try:
                records = stmt_result["Records"]
                for rec in records:
                    tbl = rec[2]["stringValue"]
                    print(f"***    {tbl}")
                    tables_to_remove.append(tbl)
            except Exception as e:
                print(f"Failed to return records ({e})")

    # -------------------------------------
    # Drop each table in list
    for tbl in tables_to_remove:
        drop_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tbl};"
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=drop_stmt)
        except Exception as e:
            raise Exception(e)

        submit_id = submit_result['Id']
        print("*** ......................")
        print(f"*** Drop table {tbl} submitted with Id: {submit_id}")
        print(f"***    {drop_stmt}")

        # -------------------------------------
        # Loop til complete, then get result status
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = rs_client.describe_statement(Id=submit_id)
            except Exception as e:
                complete = True
                print(f"*** Failed to describe_statement {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    complete = True
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    if status == "FAILED":
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

    # -------------------------------------
    # Find then remove annotated RIIS input data from S3
    # -------------------------------------
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=old_annotated_riis_key, MaxKeys=10)
    except Exception as e:
        print(f"*** Error querying for bucket/object {old_annotated_riis_key} ({e})")
    else:
        try:
            _contents = tr_response["Contents"]
        except KeyError:
            print(f"*** Object {old_annotated_riis_key} is not present")
        else:
            try:
                _ = s3_client.delete_object(
                    Bucket=S3_BUCKET, Key=old_annotated_riis_key)
            except Exception as e:
                print(f"!!! Error deleting bucket/object {old_annotated_riis_key} ({e})")
            else:
                print(f"*** Deleted {old_annotated_riis_key} with delete_object.")

    # -------------------------------------
    # Last: List then remove obsolete summary data from S3
    # -------------------------------------
    keys_to_delete = []
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=s3_obsolete_prefix, MaxKeys=10)
    except Exception as e:
        print(f"*** Error querying for objects in {s3_obsolete_prefix} ({e})")
    else:
        try:
            contents = tr_response["Contents"]
        except KeyError:
            print(f"!!! No values in {S3_BUCKET}/{s3_obsolete_prefix}")
        else:
            for rec in contents:
                key = rec["Key"]
                print("*** Keys to delete:")
                if key.find(old_bison_s3_datestr) > len(s3_obsolete_prefix):
                    keys_to_delete.append(key)
                    print(f"***      {key}")

    for old_key in keys_to_delete:
        try:
            _ = s3_client.delete_object(
                Bucket=S3_BUCKET, Key=old_key)
        except Exception as e:
            print(f"!!! Error deleting object {old_key} ({e})")
        else:
            print(f"*** Deleted {old_key} with delete_object.")

    return {
        "statusCode": 200,
        "body": "Executed bison_s6_delete_obsolete lambda"
    }
