import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import pprint
import time

print("*** Loading lambda function")

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

COMMANDS = []
bison_tbl = f"{pub_schema}.bison_{bison_datestr}"
tmp_county_tbl = f"{pub_schema}.tmp_bison_x_county_{bison_datestr}"
tmp_aiannh_tbl = f"{pub_schema}.tmp_bison_x_aiannh_{bison_datestr}"

# Each fields tuple contains original fieldname, corresponding bison fieldname and type
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
        }
    },
    "riis": {
        "table": f"riisv2_{bison_datestr}",
        "filename": "{riis_prefix}{bison_datestr}.csv",
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

join_fld = "gbifid"
# Get table, field names
cty_data = ancillary_data["county"]
cty_tbl = cty_data["table"]
st_fld = cty_data["fields"]["state"][0]
cty_fld = cty_data["fields"]["county"][0]
b_st_fld = cty_data["fields"]["state"][1]
b_cty_fld = cty_data["fields"]["county"][1]
# Drop table if exists
drop_county_tmp_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tmp_county_tbl};"
# Intersect county polygons with BISON records
intersect_county_tmp_stmt = f"""
	CREATE TABLE {tmp_county_tbl} AS
		SELECT bison.{join_fld}, cty.{st_fld}, cty.{cty_fld}
		FROM {cty_tbl} as cty, {bison_tbl} as bison
		WHERE ST_intersects(
			ST_SetSRID(bison.geom, 4326), ST_SetSRID(cty.shape, 4326));
	"""
# Fill county, state BISON fields with intersection results
fill_county_stmt = f"""
	UPDATE {bison_tbl} AS bison
		SET {b_st_fld} = tmp.{st_fld}, {b_cty_fld} = tmp.{cty_fld}
		FROM {tmp_county_tbl} AS tmp
		WHERE bison.{join_fld} = tmp.{join_fld};
	"""
# Get table, field names
aiannh_data = ancillary_data["aiannh"]
aiannh_tbl = aiannh_data["table"]
nm_fld = aiannh_data["fields"]["name"][0]
gid_fld = aiannh_data["fields"]["geoid"][0]
b_nm_fld = aiannh_data["fields"]["name"][1]
b_gid_fld = aiannh_data["fields"]["geoid"][1]
# Drop table if exists
drop_aiannh_tmp_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tmp_aiannh_tbl};"
# Intersect aiannh polygons with BISON records
intersect_aiannh_tmp_stmt = f"""
	CREATE TABLE {tmp_aiannh_tbl} AS
		SELECT bison.{join_fld}, aiannh.{nm_fld}, aiannh.{gid_fld}
		FROM {aiannh_tbl} as aiannh, {bison_tbl} as bison
		WHERE ST_intersects(
			ST_SetSRID(bison.geom, 4326), ST_SetSRID(aiannh.shape, 4326));
	"""
# Fill aiannh BISON fields with intersection results
fill_aiannh_stmt = f"""
	UPDATE {bison_tbl} AS bison
		SET {b_nm_fld} = tmp.{nm_fld}, {b_gid_fld} = tmp.{gid_fld}
		FROM {tmp_aiannh_tbl} AS tmp
		WHERE bison.{join_fld} = tmp.{join_fld};
	"""
# Add field commands, < 30 sec total
COMMANDS.extend([
    #
    ("drop_tmp_county", drop_county_tmp_stmt),
    # 3 min
    ("intersect_county", intersect_county_tmp_stmt),
    # 7 min
	("fill_county", fill_county_stmt),
    #
    ("drop_tmp_aiannh", drop_aiannh_tmp_stmt),
    # 1 min
    ("intersect_aiannh", intersect_aiannh_tmp_stmt),
    # 1 min
    ("fill_aiannh", fill_aiannh_stmt),
    ]
)

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)

def lambda_handler(event, context):
    # Execute the commmands in order
    for (cmd, stmt) in COMMANDS:
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
        # Loop til complete, then describe result
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
        'body': json.dumps(f"Lambda result logged")
    }
