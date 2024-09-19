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
# Timeout for connect/read
timeout = 60
# Wait time between result checks
waittime = 1

# Dataload filename postfixes
dt = datetime.now()
yr = dt.year
mo = dt.month
bison_datestr = f"{yr}_{mo:02d}_01"
gbif_datestr = f"{yr}-{mo:02d}-01"
# S3 GBIF bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"

state_aggregate_tbl = f"state_counts_{bison_datestr}"
state_aggregate_s3_export = f"output/{state_aggregate_tbl}_000.parquet"

query_export_stmt = "state_aggregate_s3_output"
query_tbl_stmt = f"""
    SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '{state_aggregate_tbl}';
"""
export_tbl_stmt = f"""
    UNLOAD (
        'SELECT * FROM {state_aggregate_tbl} ORDER BY census_state, riis_assessment')
        TO 's3://{bison_bucket}/output/{state_aggregate_tbl}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
# Queries for obsolete tables to be deleted
RS_COMMANDS = (
    ("query_table", query_tbl_stmt),
    ("export_table", export_tbl_stmt),
)
# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)
s3_client = session.client("s3", config=config, region_name=region)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Check for an S3 object, delete if exists, export from Redshift to S3.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute S3 query command.
        Exception: on failure to execute S3 delete command.
        Exception: on failure to execute Redshift export command.
    """
    print("*** ---------------------------------------")
    print("*** ---------------------------------------")
    print("Received trigger: " + json.dumps(event, indent=2))
    print("*** S3 list command submitted")

    # Look for existing S3 export
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=bison_bucket, Prefix=state_aggregate_s3_export, MaxKeys=10)
    except Exception as e:
        print(f"Error querying for bucket/object {state_aggregate_s3_export} ({e})")
        raise e
    try:
        contents = tr_response["Contents"]
    except KeyError:
        print(f"Object {state_aggregate_s3_export} is not present")
    else:
        print(f"Found object: {contents[0]['Key']}")

        # If found, delete
        print("*** ---------------------------------------")
        print("*** S3 delete command submitted")
        try:
            del_response = s3_client.delete_object(
                Bucket=bison_bucket, Key=state_aggregate_s3_export
            )
        except Exception as e:
            print(f"Error deleting s3://{bison_bucket}/{state_aggregate_s3_export} ({e})")
            raise e
        print(f"Delete response: {del_response}")

    # Execute the commmands in order
    for (cmd, stmt) in RS_COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)

        print("*** ---------------------------------------")
        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {stmt}")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = client_redshift.describe_statement(Id=submit_id)
            except Exception as e:
                print(f"*** Failed to describe_statement in {elapsed_time} secs: {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    complete = True
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
