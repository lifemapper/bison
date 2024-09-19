"""Lambda function to aggregate counts and lists by region."""
# Set lambda timeout to 3 minutes.
import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
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
bison_tbl = f"bison_{bison_datestr}"

# Define the bison bucket and table to create
bison_bucket = "bison-321942852011-us-east-1"
input_folder = "input"

COMMANDS = []

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
        "filename": f"USRIISv2_MasterList_annotated_{bison_datestr}.csv",
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

# Relevant tables/fields
join_fld = "gbifid"
# County data
cty_data = ancillary_data["county"]
cty_tbl = cty_data["table"]
st_fld = cty_data["fields"]["state"][0]
cty_fld = cty_data["fields"]["county"][0]
b_st_fld = cty_data["fields"]["state"][1]
b_cty_fld = cty_data["fields"]["county"][1]

# RIIS data
riis_data = ancillary_data["riis"]
riis_tbl = riis_data["table"]
loc_fld = riis_data["fields"]["locality"][0]
occid_fld = riis_data["fields"]["occid"][0]
ass_fld = riis_data["fields"]["assess"][0]
b_loc_fld = riis_data["fields"]["locality"][1]
b_occid_fld = riis_data["fields"]["occid"][1]
b_ass_fld = riis_data["fields"]["assess"][1]

# Compute riis_region (AK, HI, L48) values for dataset
update_akhi_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_loc_fld} = {b_st_fld}
        WHERE {b_st_fld} IN ('AK', 'HI');
"""
update_l48_stmt = f"""
    UPDATE {pub_schema}.{bison_tbl}
        SET {b_loc_fld} = 'L48'
        WHERE {b_st_fld} IS NOT NULL AND {b_st_fld} NOT IN ('AK', 'HI');
"""
# Annotate records with matching RIIS region + GBIF taxonkey
update_assess_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_occid_fld} = riis.{occid_fld},
            {b_ass_fld} = riis.{ass_fld}
        FROM {riis_tbl} as riis
        WHERE riis.{loc_fld} = {b_loc_fld}
          AND riis.gbif_res_taxonkey = taxonkey;
"""
# Annotate non-matching records to presumed native
update_presumed_stmt = f"""
    UPDATE {bison_tbl}
        SET {b_ass_fld} = 'presumed_native'
        WHERE {b_loc_fld} IS NOT NULL AND {b_occid_fld} IS NULL;
"""

COMMANDS = [
    # 20 sec
    ("update_akhi", update_akhi_stmt),
    # 50 sec
    ("update_l48", update_l48_stmt),
    # 20 sec
    ("update_assess", update_assess_stmt),
    # 40 sec
    ("update_presumed", update_presumed_stmt)
]

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Annotate BISON records with RIIS regions and status of species in record region.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    # Execute the commmands in order
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)
        except Exception:
            raise

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
