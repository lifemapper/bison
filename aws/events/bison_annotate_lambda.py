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

bison_tbl = f"{pub_schema}.bison_{bison_datestr}"
fields = [
	("aiannh_name", "VARCHAR(200)"), ("aiannh_geoid", "VARCHAR(200)"),
	("census_state", "VARCHAR(2)"), ("census_county", "VARCHAR(100)"),
	("riis_region", "VARCHAR(3)"), ("riis_occurrence_id", "VARCHAR(50)"),
	("riis_assessment", "VARCHAR(20)")]
COMMANDS = []
for fld, typ in fields:
	stmt = f"ALTER TABLE {bison_tbl} ADD COLUMN {fld} {typ} DEFAULT NULL;",
	COMMANDS.append((f"add_{fld}", stmt))

tmp_county_tbl = f"{pub_schema}.tmp_bison_x_county_{bison_datestr}"
intersect_county_tmp_stmt = f"""
	CREATE TABLE {tmp_county_tbl} AS
		SELECT bison.gbifid, county2023.stusps, county2023.namelsad
		FROM county2023, {bison_tbl} as bison
		WHERE ST_intersects(
			ST_SetSRID(bison.geom, 4326), ST_SetSRID(county2023.shape, 4326));
	"""
intersect_county_fill_stmt = f"""
	UPDATE {bison_tbl} AS bison
		SET census_state = tmp.stusps, census_county = tmp.namelsad
		FROM {tmp_county_tbl} AS tmp
		WHERE bison.gbifid = tmp.gbifid;
	"""
tmp_aiannh_tbl = f"{pub_schema}.tmp_bison_x_aiannh_{bison_datestr}"
intersect_aiannh_tmp_stmt = f"""
	CREATE TABLE {tmp_aiannh_tbl} AS
		SELECT bison.gbifid, aiannh2023.namelsad, aiannh2023.geoid
		FROM aiannh2023, {bison_tbl} as bison
		WHERE ST_intersects(
			ST_SetSRID(bison.geom, 4326), ST_SetSRID(aiannh2023.shape, 4326));
	"""
drop_tmp_county_stmt = f"DROP TABLE {tmp_county_tbl};"
drop_tmp_aiannh_stmt = f"DROP TABLE {tmp_aiannh_tbl};"

COMMANDS.extend(
	[("intersect_county", intersect_county_tmp_stmt),
	 ("fill_county", intersect_county_fill_stmt),
	 ("intersect_aiannh", intersect_aiannh_tmp_stmt),
	 ("fill_aiannh", intersect)]
)


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
