"""Lambda function to delete temporary and previous month's tables."""
# Set lambda timeout to 5 minutes.
import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print('Loading function')

# AWS region for all services
region = "us-east-1"
# Redshift settings
workgroup = "bison"
database = "dev"
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
pub_schema = "public"
external_schema = "redshift_spectrum"
# S3 settings
bison_bucket = "bison-321942852011-us-east-1"
input_folder = "input"
# Timeout/check results times
timeout = 900
waittime = 1

# Dataload filename postfixes
yr = datetime.now().year
mo = datetime.now().month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"

# Current temp tables to be deleted
tmp_prefix = "tmp_bison"
tmp_county_tbl = f"{tmp_prefix}_x_county_{bison_datestr}"
tmp_aiannh_tbl = f"{tmp_prefix}_x_aiannh_{bison_datestr}"

# Queries for obsolete tables to be deleted
QUERY_COMMANDS = (
    (
        "query_old",
        f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '%_{old_bison_datestr}';"
    ),
    (
        "query_tmp",
        f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} "
        f"LIKE '{tmp_prefix}%{bison_datestr}';"
    )
)

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)


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
    tables = []
    for cmd, stmt in QUERY_COMMANDS:
        # -------------------------------------
        # Submit query request
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)
            print(f"*** {cmd.upper()} command submitted")
        except Exception as e:
            raise Exception(e)

        print("*** ......................")
        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {stmt}")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete, then get result status
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = client_redshift.describe_statement(Id=submit_id)
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
        try:
            stmt_result = client_redshift.get_statement_result(Id=submit_id)
        except Exception as e:
            print(f"*** No get_statement_result {e}")
        else:
            print("*** get_statement_result records")
            try:
                records = stmt_result["Records"]
                for rec in records:
                    tbl = rec[2]["stringValue"]
                    tables.append(tbl)
                print(f"*** Found tables to be deleted: {tables}")
            except Exception as e:
                print(f"Failed to return records ({e})")

    # -------------------------------------
    # Drop each table in list
    for tbl in tables:
        drop_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tbl};"
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=drop_stmt)
        except Exception as e:
            raise Exception(e)

        print("*** ......................")
        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {drop_stmt}")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete, then get result status
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = client_redshift.describe_statement(Id=submit_id)
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

    return {
        'statusCode': 200,
        'body': json.dumps("Lambda result logged")
    }
