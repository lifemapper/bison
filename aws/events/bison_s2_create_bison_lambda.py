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

# S3 GBIF bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
gbif_datestr = f"{yr}-{mo:02d}-01"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
# Define the bison bucket and table to create
bison_tbl = f"{pub_schema}.bison_{bison_datestr}"

gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"
mounted_gbif_name = f"{external_schema}.occurrence_{bison_datestr}_parquet"

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
        gbifid	VARCHAR(max),
        datasetkey	VARCHAR(max),
        occurrenceid	VARCHAR(max),
        kingdom	VARCHAR(max),
        phylum	VARCHAR(max),
        class	VARCHAR(max),
        _order	VARCHAR(max),
        family	VARCHAR(max),
        genus	VARCHAR(max),
        species	VARCHAR(max),
        infraspecificepithet	VARCHAR(max),
        taxonrank	VARCHAR(max),
        scientificname	VARCHAR(max),
        verbatimscientificname	VARCHAR(max),
        verbatimscientificnameauthorship	VARCHAR(max),
        countrycode	VARCHAR(max),
        locality	VARCHAR(max),
        stateprovince	VARCHAR(max),
        occurrencestatus	VARCHAR(max),
        individualcount     INT,
        publishingorgkey	VARCHAR(max),
        decimallatitude	DOUBLE PRECISION,
        decimallongitude	DOUBLE PRECISION,
        coordinateuncertaintyinmeters	DOUBLE PRECISION,
        coordinateprecision	DOUBLE PRECISION,
        elevation	DOUBLE PRECISION,
        elevationaccuracy	DOUBLE PRECISION,
        depth	DOUBLE PRECISION,
        depthaccuracy	DOUBLE PRECISION,
        eventdate	TIMESTAMP,
        day	INT,
        month	INT,
        year	INT,
        taxonkey	INT,
        specieskey	INT,
        basisofrecord	VARCHAR(max),
        institutioncode	VARCHAR(max),
        collectioncode	VARCHAR(max),
        catalognumber	VARCHAR(max),
        recordnumber	VARCHAR(max),
        identifiedby	SUPER,
        dateidentified	TIMESTAMP,
        license	VARCHAR(max),
        rightsholder	VARCHAR(max),
        recordedby	SUPER,
        typestatus	SUPER,
        establishmentmeans	VARCHAR(max),
        lastinterpreted	TIMESTAMP,
        mediatype	SUPER,
        issue    SUPER
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
# Add fields for annotations to BISON table
for ttyp, tbl in ancillary_data.items():
    for (orig_fld, bison_fld, bison_typ) in tbl["fields"].values():
        # 1-2 secs
        stmt = f"ALTER TABLE {bison_tbl} ADD COLUMN {bison_fld} {bison_typ} DEFAULT NULL;"
        COMMANDS.append((f"add_{bison_fld}", stmt))

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

# -----------------------------------------------------
def lambda_handler(event, context):
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)

        print("*** ......................")
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
                stmt_result = client_redshift.get_statement_result(Id=submit_id)
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
        'body': json.dumps(f"Lambda result logged")
    }
