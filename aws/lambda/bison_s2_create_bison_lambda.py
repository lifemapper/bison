"""Lambda function to create bison records by subsetting public GBIF data."""
"""Lambda function to delete temporary and previous month's tables."""
# Set lambda timeout to 5 minutes.
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
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
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"
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

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"
# S3 GBIF bucket and file to query
gbif_bucket = f"gbif-open-data-{REGION}"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"

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

COMMANDS = []

create_schema_stmt = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS {external_schema}
        FROM data catalog
        DATABASE '{database}'
        IAM_ROLE DEFAULT
        CREATE external database IF NOT EXISTS;
"""

mount_stmt = f"""
    CREATE EXTERNAL TABLE {mounted_gbif_name} (
        gbifid                              VARCHAR(max),
        datasetkey                          VARCHAR(max),
        occurrenceid                        VARCHAR(max),
        kingdom                             VARCHAR(max),
        phylum                              VARCHAR(max),
        class                               VARCHAR(max),
        _order                              VARCHAR(max),
        family                              VARCHAR(max),
        genus                               VARCHAR(max),
        species                             VARCHAR(max),
        infraspecificepithet                VARCHAR(max),
        taxonrank                           VARCHAR(max),
        scientificname                      VARCHAR(max),
        verbatimscientificname              VARCHAR(max),
        verbatimscientificnameauthorship    VARCHAR(max),
        countrycode                         VARCHAR(max),
        locality                            VARCHAR(max),
        stateprovince                       VARCHAR(max),
        occurrencestatus                    VARCHAR(max),
        individualcount                     INT,
        publishingorgkey                    VARCHAR(max),
        decimallatitude                     DOUBLE PRECISION,
        decimallongitude                    DOUBLE PRECISION,
        coordinateuncertaintyinmeters       DOUBLE PRECISION,
        coordinateprecision                 DOUBLE PRECISION,
        elevation                           DOUBLE PRECISION,
        elevationaccuracy                   DOUBLE PRECISION,
        depth                               DOUBLE PRECISION,
        depthaccuracy                       DOUBLE PRECISION,
        eventdate                           TIMESTAMP,
        day                                 INT,
        month                               INT,
        year                                INT,
        taxonkey                            INT,
        specieskey                          INT,
        basisofrecord                       VARCHAR(max),
        institutioncode                     VARCHAR(max),
        collectioncode                      VARCHAR(max),
        catalognumber                       VARCHAR(max),
        recordnumber                        VARCHAR(max),
        identifiedby                        SUPER,
        dateidentified                      TIMESTAMP,
        license                             VARCHAR(max),
        rightsholder                        VARCHAR(max),
        recordedby                          SUPER,
        typestatus                          SUPER,
        establishmentmeans                  VARCHAR(max),
        lastinterpreted                     TIMESTAMP,
        mediatype                           SUPER,
        issue                               SUPER
    )
    STORED AS PARQUET
    LOCATION '{gbif_odr_data}';
"""

subset_stmt = f"""
    CREATE TABLE {bison_tbl} AS
        SELECT
            gbifid, datasetkey, species, taxonrank, scientificname, countrycode, stateprovince,
            occurrencestatus, publishingorgkey, day, month, year, taxonkey, specieskey,
            basisofrecord, decimallongitude, decimallatitude,
            (taxonkey || ' ' || species) as taxonkey_species
            ST_Makepoint(decimallongitude, decimallatitude) as geom
        FROM redshift_spectrum.occurrence_{bison_datestr}_parquet
        WHERE decimallatitude IS NOT NULL
          AND decimallongitude IS NOT NULL
          AND countrycode = 'US'
          AND occurrencestatus = 'PRESENT'
          AND taxonrank IN
            ('SPECIES', 'SUBSPECIES', 'VARIETY', 'FORM', 'INFRASPECIFIC_NAME', 'INFRASUBSPECIFIC_NAME')
          AND basisofrecord IN
            ('HUMAN_OBSERVATION', 'OBSERVATION', 'OCCURRENCE', 'PRESERVED_SPECIMEN');
"""

count_gbif_stmt = f"SELECT COUNT(*) from {mounted_gbif_name};"
count_bison_stmt = f"SELECT COUNT(*) FROM {bison_tbl};"
unmount_stmt = f"DROP TABLE {mounted_gbif_name};"

COMMANDS = [
    ("schema", create_schema_stmt),
    # 2 secs
    ("mount", mount_stmt),
    # 5 secs
    ("query_mount", count_gbif_stmt),
    # 1 min
    ("subset", subset_stmt),
    # 2 secs
    ("query_subset", count_bison_stmt),
    # 1 secs
    ("unmount", unmount_stmt)
]
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
            "state_county": (None, "state_county", "VARCHAR(102)")
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
# Add commands to
#    add fields for annotations from ancillary tables to BISON table
for _ttyp, tbl in ancillary_data.items():
    for (_orig_fld, bison_fld, bison_typ) in tbl["fields"].values():
        # 1-2 secs
        stmt = f"ALTER TABLE {bison_tbl} ADD COLUMN {bison_fld} {bison_typ} DEFAULT NULL;"
        COMMANDS.append((f"add_{bison_fld}", stmt))


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Subset GBIF data to a Redshift table, then add fields to it.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = rs_client.execute_statement(
                WorkgroupName=PROJECT, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)

        print("*** ......................")
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
                print(f"Failed to describe_statement {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    complete = True
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
        # IF query for count, get statement output
        if cmd.startswith("query"):
            try:
                stmt_result = rs_client.get_statement_result(Id=submit_id)
            except Exception as e:
                print(f"*** No get_statement_result {e}")
            else:
                try:
                    records = stmt_result["Records"]
                except Exception as e:
                    print(f"Failed to return records ({e})")
                else:
                    print(f"***     COUNT = {records[0][0]['longValue']}")

    return {
        'statusCode': 200,
        'body': json.dumps("Lambda result logged")
    }
