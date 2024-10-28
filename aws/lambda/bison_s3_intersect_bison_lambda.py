"""Lambda function to intersect bison with region and annotate records."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import json
import time

print('Loading function')
PROJECT = "bison"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
bison_datestr = f"{yr}_{mo:02d}_01"
gbif_datestr = f"{yr}-{mo:02d}-01"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
AWS_METADATA_URL = "http://169.254.169.254/latest/"
WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"
WORKFLOW_ROLE_ARN = f"arn:aws:iam::{PROJECT}:role/service-role/{WORKFLOW_ROLE_NAME}"
WORKFLOW_USER = f"project.{PROJECT}"

# EC2 launch template/version
EC2_SPOT_TEMPLATE = "bison_spot_task_template"
TASK = "test_task"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"
RIIS_BASENAME = "USRIISv2_MasterList"
annotated_riis_key = f"{S3_IN_DIR}/{RIIS_BASENAME}_annotated_{bison_datestr}.csv"

# Redshift
# namespace, workgroup both = 'bison'
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5
# Name the Redshift mounted gbif data and bison table to create from it
bison_tbl = f"{pub_schema}.bison_{bison_datestr}"
mounted_gbif_name = f"{external_schema}.occurrence_{bison_datestr}_parquet"

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
rs_client = session.client("redshift", config=config)

# .............................................................................
# Ancillary data parameters
# .............................................................................
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
            "state_county": (None, "state_county", "VARCHAR(102)")
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

COMMANDS = []

join_fld = "gbifid"

# State, County data
cty_data = ancillary_data["county"]
cty_tbl = cty_data["table"]
st_fld = cty_data["fields"]["state"][0]
cty_fld = cty_data["fields"]["county"][0]
b_st_fld = cty_data["fields"]["state"][1]
b_cty_fld = cty_data["fields"]["county"][1]
b_stcty_fld = cty_data["fields"]["state_county"][1]

# Temporary intersection tables
tmp_county_tbl = f"{pub_schema}.tmp_bison_x_county_{bison_datestr}"
tmp_aiannh_tbl = f"{pub_schema}.tmp_bison_x_aiannh_{bison_datestr}"

# Drop temp tables if exist
drop_county_tmp_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tmp_county_tbl};"
drop_aiannh_tmp_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{tmp_aiannh_tbl};"

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
        SET {b_st_fld} = tmp.{st_fld},
            {b_cty_fld} = tmp.{cty_fld},
            {b_stcty_fld} = tmp.{st_fld} || ' ' || tmp.{cty_fld}
        FROM {tmp_county_tbl} AS tmp
        WHERE bison.{join_fld} = tmp.{join_fld};
    """

# AIANNH data
aiannh_data = ancillary_data["aiannh"]
aiannh_tbl = aiannh_data["table"]
nm_fld = aiannh_data["fields"]["name"][0]
gid_fld = aiannh_data["fields"]["geoid"][0]
b_nm_fld = aiannh_data["fields"]["name"][1]
b_gid_fld = aiannh_data["fields"]["geoid"][1]

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


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Intersect BISON points with ancillary data polygons, then annotate BISON records.

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
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=stmt)
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
                describe_result = rs_client.describe_statement(Id=submit_id)
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
        'body': json.dumps("Lambda result logged")
    }
