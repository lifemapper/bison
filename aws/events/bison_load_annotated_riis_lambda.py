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
waittime = 1

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

riis_prefix = "USRIISv2_MasterList_annotated_"

# Define the bison bucket and table to create
bison_bucket = "bison-321942852011-us-east-1"
input_folder = "input"

drop_riis_stmt = f"DROP TABLE IF EXISTS {pub_schema}.riisv2_{old_bison_datestr};"
create_riis_stmt = f"""
    CREATE TABLE IF NOT EXISTS riisv2_{bison_datestr} (
	locality	VARCHAR(max),
	scientificname	VARCHAR(max),
	scientificnameauthorship	VARCHAR(max),
	vernacularname	VARCHAR(max),
	taxonrank	VARCHAR(max),
	establishmentmeans	VARCHAR(max),
	degreeofestablishment	VARCHAR(max),
	ishybrid	VARCHAR(max),
	pathway	VARCHAR(max),
	habitat	VARCHAR(max),
	biocontrol	VARCHAR(max),
	associatedtaxa	VARCHAR(max),
	eventremarks	VARCHAR(max),
	introdatenumber	VARCHAR(max),
	taxonremarks	VARCHAR(max),
	kingdom	VARCHAR(max),
	phylum	VARCHAR(max),
	class	VARCHAR(max),
	_order	VARCHAR(max),
    family	VARCHAR(max),
	taxonomicstatus	VARCHAR(max),
	itis_tsn	VARCHAR(max),
	gbif_taxonkey	VARCHAR(max),
	taxonid	VARCHAR(max),
	authority	VARCHAR(max),
	weblink	VARCHAR(max),
	associatedreferences	VARCHAR(max),
	eventdate	VARCHAR(max),
	modified	VARCHAR(max),
	update_remarks	VARCHAR(max),
	occurrenceremarks	VARCHAR(max),
	occurrenceid	VARCHAR(max),
	gbif_res_taxonkey	VARCHAR(max),
	gbif_res_scientificname	VARCHAR(max),
	lineno	VARCHAR(max)
);
"""

load_riis_stmt = f"""
    COPY riisv2_{bison_datestr}
    FROM 's3://{bison_bucket}/{input_folder}/{riis_prefix}{bison_datestr}.csv'
    FORMAT CSV
    IAM_role DEFAULT;
"""

COMMANDS = [
    ("drop", drop_riis_stmt),
    ("create", create_riis_stmt),
    ("load", load_riis_stmt)
]


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

def lambda_handler(event, context):
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)

        except Exception as e:
            raise Exception(e)

        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {stmt}")
        submit_id = submit_result['Id']
        for k, v in submit_result.items():
            print(f"***    {k}: {v}")

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
                    if status == "FAILED":
                        print("***    FAILED results")
                        for k, v in describe_result:
                            print(f"***    {k}: {v}")
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
