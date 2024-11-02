"""Lambda function to intersect bison with region and annotate records."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading function bison_s4_intersect_bison")
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
WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"

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
bison_tbl = f"bison_{bison_datestr}"
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
rs_client = session.client("redshift-data", config=config)

# .............................................................................
# Ancillary data parameters
# .............................................................................
RIIS_BASENAME = "USRIISv2_MasterList"
riis_fname = f"{RIIS_BASENAME}_annotated_{bison_datestr}.csv"
riis_tbl = f"riisv2_{bison_datestr}"

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
        "table": riis_tbl,
        "filename": riis_fname,
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

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
tmp_county_tbl = f"tmp_bison_x_county_{bison_datestr}"
tmp_aiannh_tbl = f"tmp_bison_x_aiannh_{bison_datestr}"

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

query_bison_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema} LIKE '{bison_tbl}';"
count_null_state_stmt = f"SELECT count(*) from {bison_tbl} WHERE {b_st_fld} IS NULL;"

# Add field commands, < 30 sec total
REDSHIFT_COMMANDS = [
    ("query_bison", query_bison_stmt),
    #
    ("drop_tmp_county", drop_county_tmp_stmt),
    # 3 min
    ("intersect_county", intersect_county_tmp_stmt),
    # 7 min
    ("fill_county", fill_county_stmt),
    ("query_count", count_null_state_stmt),
    #
    ("drop_tmp_aiannh", drop_aiannh_tmp_stmt),
    # 1 min
    ("intersect_aiannh", intersect_aiannh_tmp_stmt),
    # 1 min
    ("fill_aiannh", fill_aiannh_stmt),
    ]


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
    success = True
    # -------------------------------------
    # FIRST: Check that current BISON data is in Redshift
    # NEXT: Intersect BISON data with ancillary tables, annotate BISON, in Redshift
    # -------------------------------------
    for (cmd, stmt) in REDSHIFT_COMMANDS:
        # Stop after a failure
        if success is False:
            break
        # -------------------------------------
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=stmt)
        except Exception:
            raise

        print("*** ......................")
        print(f"*** {cmd.upper()} command submitted")
        print(stmt)
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete, then describe result
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = rs_client.describe_statement(Id=submit_id)
            except Exception as e:
                complete = True
                print(f"!!! Failed to describe_statement {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    complete = True
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    if status in ("ABORTED", "FAILED"):
                        success = False
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"!!!    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

        # -------------------------------------
        # Successful response to query required
        if cmd.startswith("query") and success is True:
            try:
                stmt_result = rs_client.get_statement_result(Id=submit_id)
            except Exception as e:
                print(f"!!! No get_statement_result {e}")
                raise
            else:
                try:
                    records = stmt_result["Records"]
                except Exception as e:
                    print(f"!!! Failed to return records ({e})")
                    raise
                else:
                    if cmd == "query_bison":
                        if len(records) == 0:
                            success = False
                            msg = f"!!! Missing Bison table {bison_tbl}"
                        else:
                            try:
                                name = records[0][2]["stringValue"]
                            except:
                                success = False
                                msg = f"!!! Unexpected query result {records}"
                            else:
                                msg = f"*** Bison table {name} found"
                    else:
                        count = records[0][0]['longValue']
                        msg = (f"***     Found {count} records where state could not "
                               f"be determined.")
                    print(msg)
    return {
        "statusCode": 200,
        "body": "Executed bison_s4_intersect_bison lambda"
    }
