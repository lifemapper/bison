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
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
pub_schema = "public"
external_schema = "redshift_spectrum"
timeout = 900
waittime = 2
test_fname = 'bison_trigger_success.txt'
test_content = 'Success = True'

# Get previous data date
yr = datetime.now().year
mo = datetime.now().month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"

# Define the public bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
gbif_datestr = f"{yr}-{mo:02d}-01"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
# Define the bison bucket and table to create
bison_bucket = 'bison-321942852011-us-east-1'
subset_bison_name = f"{pub_schema}.bison_{bison_datestr}"

gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"
mounted_gbif_name = f"{external_schema}.occurrence_{bison_datestr}_parquet"

list_old_tables_stmt = (
    f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '%_{old_bison_datestr}';"
)

COMMAND = {
    "query_old": list_old_tables_stmt
}

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
    cmd = "query_old"
    stmt = list_old_tables_stmt
    print(f"*** Entered lambda_handler with {cmd} command")
    # -------------------------------------
    # Submit query request
    try:
        submit_result = client_redshift.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=stmt)
        print(f"*** {cmd.upper()} command submitted")
    except Exception as e:
        raise Exception(e)
    # success
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
            if status in ("ABORTED", "FAILED", "FINISHED"):
                print(f"*** Query Status - {status} after {elapsed_time} seconds")
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
    if cmd.startswith("query"):
        try:
            stmt_result = client_redshift.get_statement_result(Id=submit_id)
        except Exception as e:
            print(f"*** No get_statement_result {e}")
        else:
            print("*** get_statement_result records")
            try:
                tables = []
                records = stmt_result["Records"]
                for rec in records:
                    tbl = rec[2]["stringValue"]
                    if tbl.startswith("riisv2"):
                        print(f"***     Ignoring {tbl}")
                    else:
                        # records contain database, schema, table, ...
                        print(f"***     Adding {tbl}")
                        tables.append(tbl)
                print(f"*** {len(tables)} tables to be deleted: {tables}")
            except Exception as e:
                print(f"Failed to return records ({e})")

        for tbl in tables:
            drop_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tbl};"
            print(drop_stmt)
            try:
                drop_result = client_redshift.execute_statement(
                    WorkgroupName=workgroup, Database=database, Sql=drop_stmt)
                print(f"*** {drop_stmt} submitted")
            except Exception as e:
                raise Exception(e)
            # success
            drop_submit_id = drop_result['Id']
            # -------------------------------------
            # Loop til complete, then describe result
            elapsed_time = 0
            complete = False
            while not complete:
                try:
                    describe_result = client_redshift.describe_statement(Id=drop_submit_id)
                    status = describe_result["Status"]
                    if status in ("ABORTED", "FAILED", "FINISHED"):
                        print(f"*** Query Status - {status} after {elapsed_time} seconds")
                        complete = True
                        if status == "FAILED":
                            print(f"***    FAILED results of {stmt}")
                            for k, v in describe_result:
                                print(f"***    {k}: {v}")
                    else:
                        time.sleep(1)
                        elapsed_time += 1
                except Exception as e:
                    print(f"Failed to describe_statement {e}")
                    complete = True

    return {
        'statusCode': 200,
        'body': json.dumps(f"Lambda result logged")
    }
