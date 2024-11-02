"""Lambda function to load ancillary data (geospatial and RIIS) into Redshift."""
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading function bison_s2_load_ancillary")
PROJECT = "bison"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
gbif_datestr = f"{yr}-{mo:02d}-01"
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"

# Redshift
# namespace, workgroup both = 'bison'
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"
# Wait time for completion of Redshift command
waittime = 5

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
rs_client = session.client("redshift-data", config=config)

# .............................................................................
# Ancillary data parameters
# .............................................................................
RIIS_BASENAME = "USRIISv2_MasterList"
riis_fname = f"{RIIS_BASENAME}_annotated_{bison_datestr}.csv"
annotated_riis_key = f"{S3_IN_DIR}/{riis_fname}"
old_annotated_riis_key = f"{S3_IN_DIR}/{RIIS_BASENAME}_annotated_{old_bison_datestr}.csv"
riis_tbl = f"riisv2_{bison_datestr}"
old_riis_tbl = f"riisv2_{old_bison_datestr}"

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
        "table": riis_tbl,
        "filename": riis_fname,
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

aiannh_fname = ancillary_data["aiannh"]["filename"]
aiannh_tbl = ancillary_data["aiannh"]["table"]
county_fname = ancillary_data["county"]["filename"]
county_tbl = ancillary_data["county"]["table"]

# .............................................................................
# Commands
# .............................................................................
query_tables_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema};"

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
rm_old_riis_stmt = f"DROP TABLE IF EXISTS {pub_schema}.{old_riis_tbl};"

# Before Redshift commands, check that current RIIS data exists on S3
REDSHIFT_COMMANDS = [
    ("query_tables", query_tables_stmt, None),
    ("create_aiannh", create_aiannh_stmt, aiannh_tbl),
    ("fill_aiannh", fill_aiannh_stmt, aiannh_tbl),
    ("create_county", create_county_stmt, county_tbl),
    ("fill_county", fill_county_stmt, county_tbl),
    ("create_riis", create_riis_stmt, riis_tbl),
    ("fill_riis", fill_riis_stmt, riis_tbl),
    ("query_tables", query_tables_stmt, None),
]
# After successful load, remove old RIIS data from S3


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
    # FIRST: Check that current RIIS annotated data exists on S3
    # -------------------------------------
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=annotated_riis_key, MaxKeys=10)
    except Exception as e:
        print(f"!!! Error querying for object {annotated_riis_key} ({e})")
        raise e
    try:
        _contents = tr_response["Contents"]
    except KeyError:
        raise Exception(
            f"!!! Missing annotated RIIS data: {annotated_riis_key}")
    else:
        print(f"*** Found annotated RIIS data: {annotated_riis_key}")

    # -------------------------------------
    # NEXT: Delete obsolete table,
    #       list existing tables,
    #       load current tables from S3 to Redshift
    # -------------------------------------
    tables_present = []
    for (cmd, stmt, tblname) in REDSHIFT_COMMANDS:
        if tblname is None or tblname not in tables_present:
            # -------------------------------------
            try:
                submit_result = rs_client.execute_statement(
                    WorkgroupName=PROJECT, Database=database, Sql=stmt)
            except Exception as e:
                raise Exception(e)

            submit_id = submit_result['Id']
            print(f"*** {cmd.upper()} command submitted with Id {submit_id}")

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
            # Return list of tables now present
            if cmd == "query_tables":
                try:
                    stmt_result = rs_client.get_statement_result(Id=submit_id)
                except Exception as e:
                    print(f"!!! No get_statement_result {e}")
                else:
                    try:
                        records = stmt_result["Records"]
                    except Exception as e:
                        print(f"!!! Failed to return records ({e})")
                    else:
                        # tablename is 2nd item in record
                        for rec in records:
                            tables_present.append(rec[2]['stringValue'])
                        print(f"***     Tables in Redshift: {tables_present}")

    return {
        "statusCode": 200,
        "body": "Executed bison_s2 _load_ancillary lambda"
    }
