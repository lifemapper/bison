"""Script to populate existing RDS with S3 data, using EC2."""
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import geopandas
import os
import pandas
import psycopg2
import re
from sqlalchemy import create_engine, URL
import subprocess

# AWS credentials
SECRET_DB_ACCESS_KEY = "aws/secretsmanager"
SECRET_NAME = "admin_bison-db-test"
REGION = "us-east-1"

# PostgreSQL connection
DB_INSTANCE = "bison-db-test"
DB_SCHEMA = "lmb"
DB_NAME = "bison_input"

# S3 paths
BUCKET = "bison-321942852011-us-east-1"

# Data tables, s3 input files
BISON_INPUTS = [
    {
        "table": "county",
        "relative_path": "input_data/region/",
        "pattern": "cb_2021_us_county_500k.zip",
        "is_geo": True
    },
    {
        "table": "aiannh",
        "relative_path": "input_data/region/",
        "filepattern": "cb_2021_us_aiannh_500k.zip",
        "is_geo": True
    },
    {
        "table": "pad",
        "relative_path": "input_data/pad/",
        # "pattern": r"PADUS3_0Designation_State[A-Z]{1,2}_4326\.zip",
        "pattern": r"PADUS3_0Designation_State[A-Z]{1,2}\.zip",
        "is_geo": True
    },
    {
        "table": "riis",
        "relative_path": "input_data/riis/",
        "pattern": "US-RIIS_MasterList_2021_annotated.csv",
        "is_geo": False
    },
]


# ----------------------------------------------------
def get_secret(secret_name, region):
    """Get a secret from the Secrets Manager for connection authentication.

    Args:
        secret_name: name of the secret to retrieve.
        region: AWS region containint the secret.

    Returns:
        a dictionary containing the secret data.

    Raises:
        ClientError:  an AWS error in communication.
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region)
    try:
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise(e)
    # Decrypts secret using the associated KMS key.
    secret_str = secret_value_response["SecretString"]
    return eval(secret_str)


# ----------------------------------------------------
# Function to create a pgpass file for database connection
def create_pgpass(db_name, secret):
    """Create a file of PostgreSQL database credentials to be read from the environment.

    Args:
        db_name: Database for which to write credential info.
        secret: Dictionary containing AWS RDS database credentials.

    Returns:
        boolean flag indicating if the file write was successful.

    Raises:
        Exception: on failure to open or write to file.
    """
    filename = "~/.pgpass"
    line = f"{secret['host']}:{secret['port']}:{db_name}:{secret['username']}:{secret['password']}"
    try:
        f = open(filename, "w")
        f.write(line)
        print(f"Created {filename}")
    except IOError as e:
        raise Exception(f"Error creating {filename}: {e}")
    finally:
        f.close()
    if os.path.exists(filename):
        return True
    return False


# ----------------------------------------------------
def get_db_connection(db_name, secret):
    """Create a psycopg2 connection to a PostgreSQL database.

    Args:
        db_name: Database to connect to.
        secret: Dictionary containing AWS RDS database credentials.

    Returns:
        conn: psycopg2.connection to the database.

    Raises:
        Exception: on failure to connect to database.
    """
    try:
        conn = psycopg2.connect(
            host=secret["host"], database=db_name, user=secret["username"],
            password=secret["password"]
        )
    except Exception as e:
        raise Exception("Error connecting to PostgreSQL database:", e)
    return conn


# ----------------------------------------------------
def get_db_engine(db_name, secret):
    """Create a sqlalchemy engine to connect to a PostgreSQL database.

    Args:
        db_name: Database to connect to.
        secret: Dictionary containing AWS RDS database credentials.

    Returns:
        engine: sqlalchemy.engine to the database.
    """
    drivername = "postgresql+psycopg2"
    url_object = URL.create(
        drivername,
        username=secret["username"],
        password=secret["password"],  # plain (unescaped) text
        host=secret["host"],
        database=db_name,
    )
    engine = create_engine(url_object)
    # user = secret["username"]
    # password = urllib.parse.quote_plus(secret["password"])
    # endpoint = secret["host"]
    # engine = create_engine(
    #     f"{drivername}://{user}:{password}@{endpoint}:5432/{db_name}")
    return engine


# ----------------------------------------------------
# List files in an S3 Bucket matching
# region, bucket, rel_path, filepattern = REGION, BUCKET, meta["relative_path"], meta["pattern"]
def list_files(region, bucket, rel_path, filepattern):
    """List files matching a pattern from AWS S3.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing files.
        rel_path: string of S3 enclosing "folders" in bucket
        filepattern: string for matching files in the relative path.

    Returns:
        relative_filenames: relative path to the S3 files.

    Raises:
        Exception: on failure to match any files.
    """
    relative_filenames = []
    session = boto3.session.Session()
    s3_client = session.client(service_name="s3", region_name=region)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=rel_path)
    try:
        contents = response["Contents"]
    except KeyError:
        raise Exception(f"No files found matching: {rel_path}{filepattern}")
    pattern = re.compile(filepattern)
    for item in contents:
        rel_fname = item["Key"]
        basename = os.path.basename(rel_fname)
        result = pattern.match(basename)
        if result is not None:
            relative_filenames.append(rel_fname)
    return relative_filenames


# ----------------------------------------------------
# Download a file from an S3 Bucket into a GeoPandas dataframe
def download_file(region, bucket, rel_filename):
    """Download a file from AWS S3.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing shapefile.
        rel_filename: Shapefilename including S3 enclosing "folders" in bucket

    Returns:
        local_filename: full path to the local file.

    Raises:
        Exception: on failure during file download.
        Exception: on object failed to be downloaded.
    """
    session = boto3.session.Session()
    s3_client = session.client(service_name="s3", region_name=region)
    local_filename = os.path.join("/tmp", os.path.split(rel_filename)[1])
    try:
        obj = s3_client.download_file(bucket, rel_filename, local_filename)
    except Exception as e:
        raise Exception(f"Failed to download {bucket}/{rel_filename}: {e}")
    if obj is None:
        raise Exception(f"No object downloaded from {bucket}/{rel_filename}")
    return local_filename


# ----------------------------------------------------
# Read file from an S3 Bucket into a GeoPandas dataframe
def read_s3file_into_geodataframe(region, bucket, rel_filename):
    """Read a shapefile from AWS S3 into a geopandas DataFrame.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing shapefile.
        rel_filename: Shapefilename including S3 enclosing "folders" in bucket

    Returns:
        geo_dataframe (geopandas.GeoDataFrame): dataframe containing the geospatial
            data.

    Raises:
        Exception: on failure during file download.
        Exception: on object failed to be downloaded.
    """
    session = boto3.session.Session()
    s3_client = session.client(service_name="s3", region_name=region)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=rel_filename)
    except Exception as e:
        raise Exception(f"Failed to retrieve {bucket}/{rel_filename}: {e}")
    if obj is None:
        raise Exception(f"No object retrieved from {bucket}/{rel_filename}")
    filestream = BytesIO(obj["Body"].read())
    geo_dataframe = geopandas.read_file(filestream)
    return geo_dataframe


# ----------------------------------------------------
# Insert a GeoPandas dataframe into a database table
def insert_geofile_to_database(
        region, bucket, rel_filename, engine, schema, table, do_replace=True):
    """Read a shapefile from AWS S3, then upload it to a table in AWS RDS.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing shapefile.
        rel_filename: Shapefilename including S3 enclosing "folders" in bucket
        engine: SQLAlchemy connection to the database.
        schema: Organizing structure for the database definitions.
        table: Table for data insertion
        do_replace: Flag indicating whether to replace the table if it exists,
            or append to it.
    """
    # New or existing
    exist_behavior = "replace"
    if do_replace is False:
        exist_behavior = "append"
    # Read from S3 bucket into geo-df
    geo_dataframe = read_s3file_into_geodataframe(region, bucket, rel_filename)
    print(f"Read {rfname} into geo dataframe")
    # Create or add to table
    geo_dataframe.to_postgis(
        table, engine, schema=schema, if_exists=exist_behavior, chunksize=100,
        index=False)
    print(f"Inserted geo dataframe into {table}")


# ----------------------------------------------------
# Insert a GeoPandas dataframe into a database table
def insert_padfile_to_database(
        region, bucket, rel_filename, db_name, secret, schema, table, do_replace=True):
    """Download a shapefile from AWS S3, then upload it to a table in AWS RDS.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing shapefile.
        rel_filename: Shapefilename including S3 enclosing "folders" in bucket
        db_name: Database in which to insert the data.
        secret: Dictionary containing AWS RDS database credentials.
        schema: Organizing structure for the database definitions.
        table: Table for data insertion
        do_replace: Flag indicating whether to replace the table if it exists,
            or append to it.
    """
    # success = create_pgpass(db_name, secret)
    # if success:
    # Download from S3 bucket
    local_filename = download_file(region, bucket, rel_filename)
    print(f"Downloaded {local_filename}")
    # Create or append
    behavior = "-c"
    if do_replace is False:
        behavior = "-a"
    cmd = (f"shp2pgsql  -s 4326 {behavior} -W 'UTF-8' {local_filename} ${schema}.${table}"
           f" | psql --host={secret['host']}  -d {db_name} -U {secret['username']};")
    info, err = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    if err:
        print(f"Failed to execute shp2pgsql on {table}")


# ----------------------------------------------------
# Insert a GeoPandas dataframe into a database table
def insert_csvfile_to_database(
        region, bucket, rel_filename, engine, schema, table, do_replace=True):
    """Read a CSV from AWS S3, then upload it to a table in AWS RDS.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing shapefile.
        rel_filename: Shapefilename including S3 enclosing "folders" in bucket
        engine: SQLAlchemy connection to the database.
        schema: Organizing structure for the database definitions.
        table: Table for data insertion
        do_replace: Flag indicating whether to replace the table if it exists,
            or append to it.
    """
    # New or existing
    exist_behavior = "replace"
    if do_replace is False:
        exist_behavior = "append"
    # Read from S3 bucket into df
    dataframe = read_s3file_into_dataframe(region, bucket, rel_filename)
    print(f"Read {rfname} into dataframe")
    # Create or add to table
    dataframe.to_sql(
        table, engine, schema=schema, if_exists=exist_behavior, index=False)
    print(f"Inserted dataframe into {table}")


# ----------------------------------------------------
# Read file from an S3 Bucket into a GeoPandas dataframe
def read_s3file_into_dataframe(region, bucket, rel_filename):
    """Read a CSV from AWS S3 into a pandas DataFrame.

    Args:
        region: AWS region for operations
        bucket: AWS S3 bucket containing shapefile.
        rel_filename: Shapefilename including S3 enclosing "folders" in bucket

    Returns:
        dataframe (pandas.DataFrame): dataframe containing the structured data.

    Raises:
        Exception: on failure during file download.
        Exception: on object failed to be downloaded.
    """
    session = boto3.session.Session()
    s3_client = session.client(service_name="s3", region_name=region)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=rel_filename)
    except Exception as e:
        raise Exception(f"Failed to retrieve {bucket}/{rel_filename}: {e}")
    if obj is None:
        raise Exception(f"No object retrieved from {bucket}/{rel_filename}")
    filestream = BytesIO(obj["Body"].read())
    dataframe = pandas.read_csv(filestream)
    return dataframe


# --------------------------------------------------------------------------------------
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """Main script to execute all elements of the summarize-GBIF BISON workflow."""
    # Retrieve credentials
    secret = get_secret(SECRET_NAME, REGION)
    engine = get_db_engine(DB_NAME, secret)

    for meta in BISON_INPUTS:
        table = meta["table"]
        rel_fnames = list_files(REGION, BUCKET, meta["relative_path"], meta["pattern"])
        for rfname in rel_fnames:
            do_replace = True
            # if series and not first, append
            if len(rel_fnames) > 1 and rfname != rel_fnames[0]:
                do_replace = False
            if meta["is_geo"] is True:
                if meta["table"] == "pad":
                    insert_geofile_to_database(
                        REGION, BUCKET, rfname, engine, DB_SCHEMA, table,
                        do_replace=do_replace)
                else:
                    # TODO: why is PAD data insertion crashing python with "Killed" message?
                    insert_padfile_to_database(
                        REGION, BUCKET, rfname, DB_NAME, secret, DB_SCHEMA, table,
                        do_replace=do_replace)
            else:
                insert_csvfile_to_database(
                    REGION, BUCKET, rfname, engine, DB_SCHEMA, table, do_replace=do_replace)


"""
region = REGION
bucket = BUCKET
schema = DB_SCHEMA

# Test PAD state data
meta = BISON_INPUTS[2]
table = meta["table"]
table, rel_path, filepattern = meta["table"], meta["relative_path"], meta["pattern"]
rel_fnames = list_files(REGION, BUCKET, meta["relative_path"], meta["pattern"])

# Test
rfname = rel_fnames[0]
rel_filename = rfname
append = True
if rfname == rel_fnames[0]:
    append = False


insert_geofile_to_database(
    REGION, BUCKET, rfname, engine, DB_SCHEMA, table, append=append)

# insert_csvfile_to_database(
#     REGION, BUCKET, rfname, engine, DB_SCHEMA, table, append=append)
"""

"""
sudo su -
apt-get update -y
apt-get upgrade -y
apt-get install -y awscli
apt-get install -y python3-pip

# Set up configuration in local file ~/.aws/config
aws configure set default.region us-east-1 && \
aws configure set default.output json

Setup credentials in local file  ~/.aws/credentials
[default]
aws_access_key_id = <>
aws_secret_access_key = <>

pip3 install boto3 pandas geopandas psycopg2-binary sqlalchemy
"""
