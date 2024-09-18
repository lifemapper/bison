import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print('Loading function')

# AWS region for all services
region = "us-east-1"
# Redshift settings
workgroup = "bison"
database = "dev"
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
pub_schema = "public"
external_schema = "redshift_spectrum"
# S3 settings
bison_bucket = "bison-321942852011-us-east-1"
input_folder = "input"
# Timeout/check results times
timeout = 900
waittime = 1

# Dataload filename postfixes
yr = datetime.now().year
mo = datetime.now().month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"

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

query_tables_stmt = f"SHOW TABLES FROM SCHEMA {database}.{pub_schema};"
aiannh_tbl = ancillary_data["aiannh"]["table"]
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
        FROM 's3://{bison_bucket}/{input_folder}/{aiannh_fname}'
        FORMAT SHAPEFILE
        IAM_role DEFAULT;
"""

county_tbl = ancillary_data["county"]["table"]
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
        FROM 's3://{bison_bucket}/{input_folder}/{county_fname}'
        FORMAT SHAPEFILE
        IAM_role DEFAULT;
"""

riis_tbl = ancillary_data["riis"]["table"]
riis_fname = ancillary_data["riis"]["filename"]
create_riis_stmt = f"""
    CREATE TABLE IF NOT EXISTS {riis_tbl} (
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
fill_riis_stmt = f"""
    COPY riisv2_{bison_datestr}
        FROM 's3://{bison_bucket}/{input_folder}/{riis_fname}'
        FORMAT CSV
        IAM_role DEFAULT;
"""

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
COMMANDS = [
    ("query_tables", query_tables_stmt, None),
    ("create_aiannh", create_aiannh_stmt, aiannh_tbl),
    ("fill_aiannh", fill_aiannh_stmt, aiannh_tbl),
    ("create_county", create_county_stmt, county_tbl),
    ("fill_county", fill_county_stmt, county_tbl),
    ("create_riis", create_riis_stmt, riis_tbl),
    ("fill_riis", fill_riis_stmt, riis_tbl)
]

# -----------------------------------------------------
def lambda_handler(event, context):
    tables_present = []
    for (cmd, stmt, tblname) in COMMANDS:
        if tblname is None or tblname not in tables_present:
            # -------------------------------------
            try:
                submit_result = client_redshift.execute_statement(
                    WorkgroupName=workgroup, Database=database, Sql=stmt)
            except Exception as e:
                raise Exception(e)

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
                            print(f"***    FAILED: {err}")
                    else:
                        time.sleep(waittime)
                        elapsed_time += waittime

            # -------------------------------------
            # IFF query tables, create list of tables that are present
            if cmd == "query_tables":
                try:
                    stmt_result = client_redshift.get_statement_result(Id=submit_id)
                except Exception as e:
                    print(f"*** No get_statement_result {e}")
                else:
                    try:
                        records = stmt_result["Records"]
                    except Exception as e:
                        print(f"*** Failed to return records ({e})")
                    else:
                        # tablename is 2nd item in record
                        for rec in records:
                            tables_present.append(rec[2]['stringValue'])
                        print(f"***     Tables: {tables_present}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Lambda result logged")
    }
