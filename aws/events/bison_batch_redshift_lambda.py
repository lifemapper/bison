import os
import json
import boto3
import botocore
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print('Loading function')

region = "us-east-1"
workgroup = "bison"
database = "dev"
dbuser = "IAM:aimee.stewart"
dbuser = "arn:aws:iam::321942852011:role/service-role/bison_subset_gbif_lambda-role-9i5qvpux"
# Lambda limit = 15 minutes = 900 seconds
timeout = 900
waittime = 10

# Define the public bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
gbif_datestr = f"{datetime.now().year}-{datetime.now().month:02d}-01"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
bison_datestr = gbif_datestr.replace("-", "_")

pub_schema = "public"
external_schema = "redshift_spectrum"
gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"
mounted_gbif_name = f"{external_schema}.occurrence_{bison_datestr}_parquet"
subset_bison_name = f"{pub_schema}.bison_{bison_datestr}"

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
    CREATE TABLE {subset_bison_name} AS
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

list_external_tables_stmt = f"""
    SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
    FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
    WHERE cls.relnamespace = ns.oid
      AND schemaname = '{external_schema}';
"""

list_public_tables_stmt = f"""
    SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
    FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
    WHERE cls.relnamespace = ns.oid
      AND schemaname = '{pub_schema}';
"""

count_gbif_stmt = f"SELECT COUNT(*) from {mounted_gbif_name};"
count_bison_stmt = f"SELECT COUNT(*) FROM {subset_bison_name};"
unmount_stmt = f"DROP TABLE {mounted_gbif_name};"
bison_bucket = 'bison-321942852011-us-east-1'
test_fname = 'bison_trigger_success.txt'
test_content = 'Success = True'

session = boto3.session.Session()
region = "us-east-1"

# Initializing Botocore client
bc_session = bc.get_session()
session = boto3.Session(
    botocore_session=bc_session,
    region_name=region
)

# Initializing Redshift's client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)


def lambda_handler(event, context):
    print("*** Entered lambda_handler")
    # -------------------------------------
    # Submit SQL statements
    try:
        submit_result = client_redshift.batch_execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            Sqls=[
                list_public_tables_stmt,
            ],
            StatementName='bison_batch_test',
            WithEvent=True)
        print("*** Batch execute returned")

    except botocore.exceptions.ConnectionError as e:
        client_redshift_1 = session.client("redshift-data", config=config)
        submit_result = client_redshift_1.batch_execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            Sqls=[
                list_public_tables_stmt,
            ],
            StatementName='bison_batch_test',
            WithEvent=True)
        print("*** Batch execute returned after reestablishing the connection")

    except Exception as e:
        raise Exception(e)

    for k, v in submit_result.items():
        print(f"***     {k} = {v}")
    curr_id = submit_result['Id']
    print(f"*** id = {curr_id}")

    # -------------------------------------
    # Loop til complete
    elapsed_time = 0
    while True and elapsed_time < timeout:
        describe_result = client_redshift.describe_statement(Id=curr_id)
        status = describe_result["Status"]
        print(f"*** Query Status - {status} after {elapsed_time} seconds")
        if (status == "FINISHED"):
            break;
        else:
            time.sleep(waittime)
            elapsed_time += waittime

    # -------------------------------------
    # Get statement output
    # stmt_result = client_redshift.get_statement_result(Id=curr_id)
    # for k, v in stmt_result.items():
    #     print(f"***     {k} = {v}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Lambda result: {str(submit_result)}")
    }
