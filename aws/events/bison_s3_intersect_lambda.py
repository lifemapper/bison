import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import pprint
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

COMMANDS = []
bison_tbl = f"{pub_schema}.bison_{bison_datestr}"
tmp_county_tbl = f"{pub_schema}.tmp_bison_x_county_{bison_datestr}"
tmp_aiannh_tbl = f"{pub_schema}.tmp_bison_x_aiannh_{bison_datestr}"

# Each fields tuple contains original fieldname, corresponding bison fieldname and type
field_data = {
    "aiannh": {
        "table": "aiannh2023",
        "fields": {
            "name": ("namelsad", "aiannh_name", "VARCHAR(200)"),
            "geoid": ("geoid", "aiannh_geoid", "VARCHAR(200)")
        }
    },
    "county": {
        "table": "county2023",
        "fields": {
            "state": ("stusps", "census_state", "VARCHAR(200)"),
            "county": ("namelsad", "census_county", "VARCHAR(200)"),
        }
    },
    "riis": {
        "table": f"riisv2_{bison_datestr}",
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}
# Add fields to BISON records table
for tbl in field_data.keys():
    for (orig_fld, bison_fld, bison_typ) in tbl["fields"].values():
    	stmt = f"ALTER TABLE {bison_tbl} ADD COLUMN {bison_fld} {bison_typ} DEFAULT NULL;"
	COMMANDS.append((f"add_{bison_fld}", stmt))

join_fld = "gbifid"
# Get table, field names
tbl = field_data["county"]
tbl_name = tbl["table"]
st_fld = tbl["fields"]["state"][0]
cty_fld = tbl["fields"]["county"][0]
b_st_fld = tbl["fields"]["state"][1]
b_cty_fld = tbl["fields"]["county"][1]
# Intersect county polygons with BISON records
intersect_county_tmp_stmt = f"""
	CREATE TABLE {tmp_county_tbl} AS
		SELECT bison.{join_fld}, cty.{st_fld}, cty.{cty_fld}
		FROM {tbl_name} as cty, {bison_tbl} as bison
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
tbl = field_data["aiannh"]
tbl_name = tbl["table"]
nm_fld = tbl["fields"]["name"][0]
gid_fld = tbl["fields"]["geoid"][0]
b_nm_fld = tbl["fields"]["name"][1]
b_gid_fld = tbl["fields"]["geoid"][1]
# Intersect aiannh polygons with BISON records
intersect_aiannh_tmp_stmt = f"""
	CREATE TABLE {tmp_aiannh_tbl} AS
		SELECT bison.{join_fld}, aiannh.{nm_fld}, aiannh.{gid_fld}
		FROM {tbl_name} as aiannh, {bison_tbl} as bison
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
# Drop temporary tables
drop_tmp_county_stmt = f"DROP TABLE {tmp_county_tbl};"
drop_tmp_aiannh_stmt = f"DROP TABLE {tmp_aiannh_tbl};"

# Get table, field names
tbl = field_data["riis"]
tbl_name = tbl["table"]
loc_fld = tbl["fields"]["locality"][0]
occid_fld = tbl["fields"]["occid"][0]
ass_fld = tbl["fields"]["assess"][0]
b_loc_fld = tbl["fields"]["locality"][1]
b_occid_fld = tbl["fields"]["occid"][1]
b_ass_fld = tbl["fields"]["assess"][1]
# Compute riis_region (AK, HI, L48) values for dataset
update_akhi_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_loc_fld} = {b_st_fld}
        WHERE {b_st_fld} IN ('AK', 'HI');
"""
update_l48_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_loc_fld} = 'L48'
        WHERE {b_st_fld} IS NOT NULL AND {b_st_fld} NOT IN ('AK', 'HI');
"""
# Annotate records with matching RIIS region + GBIF taxonkey
update_assess_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_occid_fld} = riis.{occid_fld},
            {b_ass_fld} = riis.{ass_fld}
        FROM {tbl_name} as riis
        WHERE riis.{loc_fld} = {b_loc_fld}
          AND riis.gbif_res_taxonkey = taxonkey;
"""

# Annotate non-matching records to presumed native
update_presumed_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_ass_fld} = 'presumed_native'
        WHERE {b_loc_fld} IS NOT NULL AND {b_occid_fld} IS NULL;
"""
# Add field commands, 2-3 sec each
COMMANDS.extend([
    # # 2-3 min
    # ("intersect_county", intersect_county_tmp_stmt),
    # # 6-7 min
	# ("fill_county", fill_county_stmt),
    # # 1 sec
    # ("drop_tmp_county", drop_tmp_county_stmt),
    # # 27 sec
    # ("intersect_aiannh", intersect_aiannh_tmp_stmt),
    # # 19 sec
    # ("fill_aiannh", fill_aiannh_stmt),
    # # 1 sec
    # ("drop_tmp_aiannh", drop_tmp_aiannh_stmt),
    # 11 sec
    ("update_akhi", update_akhi_stmt),
    # 42 sec
    ("update_l48", update_l48_stmt),

    ("update_assess", update_assess_stmt),
    ("update_presumed", update_presumed_stmt)
    ]
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
    some_cmds = [
        ("drop_tmp_county", drop_tmp_county_stmt),
        ("intersect_aiannh", intersect_aiannh_tmp_stmt),
        ("fill_aiannh", fill_aiannh_stmt),
        ("drop_tmp_aiannh", drop_tmp_aiannh_stmt),
        ("update_akhi", update_akhi_stmt),
        ("update_l48", update_l48_stmt),
        ("update_assess", update_assess_stmt),
        ("update_presumed", update_presumed_stmt)
    ]

    # Execute the commmands in order
    for (cmd, stmt) in some_cmds:
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
        # Cannot describe results that affect many records
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
                    print("*** ......................")
                    print(f"*** Query Status - {status} after {elapsed_time} seconds")
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
