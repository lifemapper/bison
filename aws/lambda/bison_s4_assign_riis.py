"""Lambda function to aggregate counts and lists by region."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading function bison_s3_intersect_bison")
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
# Report RIIS assessment counts
query_riis_counts_stmt = \
    (f"SELECT {b_ass_fld}, count(*) as total FROM {bison_tbl} "
     f"WHERE {b_st_fld} IS NOT NULL GROUP BY {b_ass_fld} ORDER BY total;")

REDSHIFT_COMMANDS = [
    # 20 sec
    ("update_akhi", update_akhi_stmt),
    # 50 sec
    ("update_l48", update_l48_stmt),
    # 20 sec
    ("update_assess", update_assess_stmt),
    # 40 sec
    ("update_presumed", update_presumed_stmt),
    ("query_riis", query_riis_counts_stmt)
]


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
    success = True
    # Execute the commmands in order
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

        print("*** ---------------------------------------")
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
                print(f"*** Failed to describe_statement in {elapsed_time} secs: {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    complete = True
                    if status in ("ABORTED", "FAILED"):
                        success = False
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
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
                    if cmd == "query_riis":
                        print(f"*** Assessment totals")
                        # Query returns assesment, count as 1st and 2nd items in record
                        for rec in records:
                            try:
                                print(f"***    {rec[0]['stringValue']}: {rec[1]['longValue']}")
                            except:
                                print(f"!!! Unexpected record format {rec}")
                                break

    return {
        "statusCode": 200,
        "body": "Executed bison_s4_assign_riis lambda"
    }
