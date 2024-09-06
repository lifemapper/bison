import os
import json
import boto3
import botocore
import botocore.session as bc
from botocore.client import Config
from datetime import datetime

print('Loading function')

region = "us-east-1"
workgroup = "bison"
database = "dev"
timeout = 900

# Define the public bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
gbif_datestr = f"{datetime.now().year}-{datetime.now().month:02d}-01"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"
bison_datestr = gbif_datestr.replace("-", "_")

gbif_odr_data = f"s3://{gbif_bucket}/{parquet_key}/"
mounted_gbif_name = f"redshift_spectrum.occurrence_{bison_datestr}_parquet"
subset_bison_name = f"public.bison_{bison_datestr}"

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

count_gbif_stmt = f"SELECT COUNT(*) from {mounted_gbif_name};"
count_bison_stmt = f"SELECT COUNT(*) FROM {subset_bison_name};"
unmount_stmt = f"DROP TABLE {mounted_gbif_name};"

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
    # Mount GBIF data
    try:
        mount_result = client_redshift.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=mount_stmt)
        print(f"*** {mounted_gbif_name} mount successfully executed")

    except botocore.exceptions.ConnectionError as e:
        client_redshift_1 = session.client("redshift-data", config=config)
        mount_result = client_redshift_1.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=mount_stmt)
        print(f"*** {mounted_gbif_name} mount after reestablishing the connection")

    except Exception as e:
        raise Exception(e)

    print(str(mount_result))
    # -------------------------------------
    # Wait for success

    # -------------------------------------
    # Count GBIF records
    try:
        count_gbif_result = client_redshift.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=count_gbif_stmt)
        print("*** GBIF count successfully executed")

    except botocore.exceptions.ConnectionError as e:
        client_redshift_1 = session.client("redshift-data", config=config)
        count_gbif_result = client_redshift_1.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=mount_stmt)
        print("*** GBIF count after reestablishing the connection")

    except Exception as e:
        raise Exception(e)

    print(str(count_gbif_result))
    # -------------------------------------
    # Subset GBIF data for BISON
    try:
        subset_result = client_redshift.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=subset_stmt)
        print(f"*** Subset to {subset_bison_name} successfully executed")

    except botocore.exceptions.ConnectionError as e:
        client_redshift_1 = session.client("redshift-data", config=config)
        subset_result = client_redshift_1.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=subset_stmt)
        print(f"*** Subset to {subset_bison_name} after reestablishing the connection")

    except Exception as e:
        raise Exception(e)

    print(str(subset_result))
    # -------------------------------------
    # Count BISON records
    try:
        count_bison_result = client_redshift.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=count_bison_stmt)
        print("*** BISON count successfully executed")

    except botocore.exceptions.ConnectionError as e:
        client_redshift_1 = session.client("redshift-data", config=config)
        count_bison_result = client_redshift_1.execute_statement(
            WorkgroupName=workgroup, Database=database, Sql=count_bison_stmt)
        print("*** BISON count after reestablishing the connection")

    except Exception as e:
        raise Exception(e)

    print(str(count_bison_result))
    return {
        'statusCode': 200,
        'body': json.dumps(f"Lambda result: {str(mount_result)}")
    }
