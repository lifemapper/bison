import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print('Loading function')

region = "us-east-1"
workgroup = "bison"
database = "dev"
dbuser = "IAM:aimee.stewart"
dbuser = "arn:aws:iam::321942852011:role/service-role/bison_subset_gbif_lambda-role-9i5qvpux"
dbuser = "arn:aws:iam::321942852011:role/service-role/bison_subset_gbif_lambda-role-9i5qvpux"
bison_bucket = 'bison-321942852011-us-east-1'
timeout = 900
waittime = 2

# Define the public bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
gbif_datestr = f"{datetime.now().year}-{datetime.now().month:02d}-01"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
bison_datestr = gbif_datestr.replace("-", "_")
pub_schema = "public"
external_schema = "redshift_spectrum"

gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"
mounted_gbif_name = f"{external_schema}.occurrence_{bison_datestr}_parquet"
subset_bison_name = f"{pub_schema}.bison_{bison_datestr}"

list_external_tables_stmt = f"""
    SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
    FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
    WHERE cls.relnamespace = ns.oid
      AND schemaname = '{external_schema}';
"""

list_public_tables_stmt = f"""
    SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
    FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
    WHERE cls.relnamespace = ns.oid
      AND schemaname = '{pub_schema}';
"""

count_gbif_stmt = f"SELECT COUNT(*) from {mounted_gbif_name};"
count_bison_stmt = f"SELECT COUNT(*) FROM {subset_bison_name};"

# Initializing Botocore client
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(
    botocore_session=bc_session,
    region_name=region
)

# Initializing Redshift's client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)

# -----------------------------------------------------
def lambda_handler(event, context):
    print("*** Entered lambda_handler")
    # -------------------------------------
    # Submit query request
    try:
        submit_result = client_redshift.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=list_public_tables_stmt)
        print(f"*** Mount command submitted")

    except Exception as e:
        raise Exception(e)

    submit_id = submit_result['Id']
    print(f"*** submit id = {submit_id}")
    for k, v in submit_result.items():
        print(f"***     {k} = {v}")

    # -------------------------------------
    # Loop til complete, then describe result
    elapsed_time = 0
    complete = False
    while not complete and elapsed_time < 300:
        try:
            describe_result = client_redshift.describe_statement(Id=submit_id)
            status = describe_result["Status"]
            print(f"*** Query Status - {status} after {elapsed_time} seconds")
            if status in ("ABORTED", "FAILED", "FINISHED"):
                complete = True
                desc_id = describe_result['Id']
                print(f"*** desc id = {desc_id}")
                for k, v in describe_result.items():
                    print(f"***    {k} = {v}")
            else:
                time.sleep(waittime)
                elapsed_time += waittime
        except Exception as e:
            print(f"Failed to describe_statement {e}")
            complete = True

    # -------------------------------------
    # IFF query, get statement output
    try:
        stmt_result = client_redshift.get_statement_result(Id=submit_id)
    except Exception as e:
        print(f"*** No get_statement_result {e}")
    else:
        print("*** get_statement_result records")
        try:
            records = stmt_result["Records"]
            for rec in records:
                print(f"***     {rec}")
        except Exception as e:
            print(f"Failed to return records ({e})")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Lambda result logged")
    }
