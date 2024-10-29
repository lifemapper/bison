"""Lambda function to load ancillary data (geospatial and RIIS) into Redshift."""
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
riis_fname = f"{RIIS_BASENAME}_annotated_{bison_datestr}.csv"
# ancillary_data["riis"]["filename"]
annotated_riis_key = f"{S3_IN_DIR}/{riis_fname}"

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
            # Field constructed to ensure uniqueness
            "state_county": (None, "state_county", "VARCHAR(102)")
        }
    },
    "riis": {
        "table": f"riisv2_{bison_datestr}",
        "filename": riis_fname,
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

riis_tbl = ancillary_data["riis"]["table"]
aiannh_tbl = ancillary_data["aiannh"]["table"]
county_tbl = ancillary_data["county"]["table"]
tables_required = []
query_tables_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema};"

aiannh_fname = ancillary_data["aiannh"]["filename"]
create_aiannh_stmt = f"""
    CREATE TABLE {aiannh_tbl} (
       shape    GEOMETRY,
       AIANNHCE VARCHAR(4),
       AIANNHNS VARCHAR(8),
       GEOIDFQ  VARCHAR(13),
       GEOID    VARCHAR(4),
       NAME     VARCHAR(100),
       NAMELSAD VARCHAR(100),
       LSAD     VARCHAR(2),
       MTFCC    VARCHAR(5),
       ALAND    BIGINT,
       AWATER   BIGINT
    );
"""
fill_aiannh_stmt = f""""
    COPY {aiannh_tbl}
        FROM 's3://{S3_BUCKET}/{S3_IN_DIR}/{aiannh_fname}'
        FORMAT SHAPEFILE
        IAM_role DEFAULT;
"""

county_fname = ancillary_data["county"]["filename"]
create_county_stmt = f"""
    CREATE TABLE {county_tbl} (
       shape      GEOMETRY,
       STATEFP    VARCHAR(2),
       COUNTYFP   VARCHAR(3),
       COUNTYNS   VARCHAR(8),
       GEOIDFQ    VARCHAR(14),
       GEOID      VARCHAR(5),
       NAME       VARCHAR(100),
       NAMELSAD   VARCHAR(100),
       STUSPS     VARCHAR(2),
       STATE_NAME VARCHAR(100),
       LSAD       VARCHAR(2),
       ALAND      BIGINT,
       AWATER     BIGINT
    );
"""
fill_county_stmt = f"""
    COPY {county_tbl}
        FROM 's3://{S3_BUCKET}/{S3_IN_DIR}/{county_fname}'
        FORMAT SHAPEFILE
        IAM_role DEFAULT;
"""

create_riis_stmt = f"""
    CREATE TABLE IF NOT EXISTS {riis_tbl} (
    locality                  VARCHAR(max),
    scientificname            VARCHAR(max),
    scientificnameauthorship  VARCHAR(max),
    vernacularname            VARCHAR(max),
    taxonrank                 VARCHAR(max),
    establishmentmeans        VARCHAR(max),
    degreeofestablishment     VARCHAR(max),
    ishybrid                  VARCHAR(max),
    pathway                   VARCHAR(max),
    habitat                   VARCHAR(max),
    biocontrol                VARCHAR(max),
    associatedtaxa            VARCHAR(max),
    eventremarks              VARCHAR(max),
    introdatenumber           VARCHAR(max),
    taxonremarks              VARCHAR(max),
    kingdom                   VARCHAR(max),
    phylum                    VARCHAR(max),
    class                     VARCHAR(max),
    _order                    VARCHAR(max),
    family                    VARCHAR(max),
    taxonomicstatus           VARCHAR(max),
    itis_tsn                  VARCHAR(max),
    gbif_taxonkey             VARCHAR(max),
    taxonid                   VARCHAR(max),
    authority                 VARCHAR(max),
    weblink                   VARCHAR(max),
    associatedreferences      VARCHAR(max),
    eventdate                 VARCHAR(max),
    modified                  VARCHAR(max),
    update_remarks            VARCHAR(max),
    occurrenceremarks         VARCHAR(max),
    occurrenceid              VARCHAR(max),
    gbif_res_taxonkey         VARCHAR(max),
    gbif_res_scientificname   VARCHAR(max),
    lineno                    VARCHAR(max)
);
"""
fill_riis_stmt = f"""
    COPY {riis_tbl}
        FROM 's3://{S3_BUCKET}/{annotated_riis_key}'
        FORMAT CSV
        IAM_role DEFAULT;
"""

COMMANDS = [
    ("query_tables", query_tables_stmt, None),
    ("create_aiannh", create_aiannh_stmt, aiannh_tbl),
    ("fill_aiannh", fill_aiannh_stmt, aiannh_tbl),
    ("create_county", create_county_stmt, county_tbl),
    ("fill_county", fill_county_stmt, county_tbl),
    ("create_riis", create_riis_stmt, riis_tbl),
    ("fill_riis", fill_riis_stmt, riis_tbl)
]


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Load ancillary bison input data from S3 to Redshift.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    # -------------------------------------
    # FIRST: Check that current RIIS annotated data is on S3
    # -------------------------------------
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=annotated_riis_key, MaxKeys=10)
    except Exception as e:
        print(f"!!! Error querying for bucket/object {annotated_riis_key} ({e})")
        raise e
    try:
        _contents = tr_response["Contents"]
    except KeyError:
        raise Exception(
            f"!!! Missing annotated RIIS data: {annotated_riis_key}")
    else:
        print(f"*** Found annotated RIIS data: {annotated_riis_key}")

    # -------------------------------------
    # NEXT: Load tables from S3 to Redshift
    # -------------------------------------
    tables_present = []
    for (cmd, stmt, tblname) in COMMANDS:
        if tblname is None or tblname not in tables_present:
            # -------------------------------------
            try:
                submit_result = rs_client.execute_statement(
                    WorkgroupName=PROJECT, Database=database, Sql=stmt)
            except Exception as e:
                raise Exception(e)

            print(f"*** {cmd.upper()} command submitted")
            print(f"***    {stmt}")
            submit_id = submit_result['Id']

            # -------------------------------------
            # Loop til complete, then get result status
            elapsed_time = 0
            complete = False
            while not complete:
                try:
                    describe_result = rs_client.describe_statement(Id=submit_id)
                except Exception as e:
                    complete = True
                    print(f"*** Failed to describe_statement in {elapsed_time} secs: {e}")
                else:
                    status = describe_result["Status"]
                    if status in ("ABORTED", "FAILED", "FINISHED"):
                        complete = True
                        print("*** ......................")
                        print(f"*** Status - {status} after {elapsed_time} seconds")
                        if status == "FAILED":
                            try:
                                err = describe_result["Error"]
                            except Exception:
                                err = "Unknown Error"
                            print(f"!!!  FAILED: {err}")
                    else:
                        time.sleep(waittime)
                        elapsed_time += waittime

            # -------------------------------------
            # IFF query tables, create list of tables that are present
            if cmd == "query_tables":
                try:
                    stmt_result = rs_client.get_statement_result(Id=submit_id)
                except Exception as e:
                    print(f"*** No get_statement_result {e}")
                else:
                    try:
                        records = stmt_result["Records"]
                    except Exception as e:
                        print(f"!!! Failed to return records ({e})")
                    else:
                        # tablename is 2nd item in record
                        for rec in records:
                            tables_present.append(rec[2]['stringValue'])
                        print(f"***     Tables: {tables_present}")

    return {
        'statusCode': 200,
        'body': json.dumps("Lambda result logged")
    }
