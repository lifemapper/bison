"""Glue script to read S3 point data, RDS polygon data, append enclosing value to points."""
# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from botocore.exceptions import ClientError
from datetime import datetime as DT
# import psycopg2
from pyspark.context import SparkContext
from sqlalchemy import create_engine, URL
import sys

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


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
        raise (e)
    # Decrypts secret using the associated KMS key.
    secret_str = secret_value_response["SecretString"]
    return eval(secret_str)


# # ----------------------------------------------------
# def get_db_connection(db_name, secret):
#     """Create a psycopg2 connection to a PostgreSQL database.
#
#     Args:
#         db_name: Database to connect to.
#         secret: Dictionary containing AWS RDS database credentials.
#
#     Returns:
#         conn: psycopg2.connection to the database.
#
#     Raises:
#         Exception: on failure to connect to database.
#     """
#     try:
#         conn = psycopg2.connect(
#             host=secret["host"], database=db_name, user=secret["username"],
#             password=secret["password"]
#         )
#     except Exception as e:
#         raise Exception("Error connecting to PostgreSQL database:", e)
#     return conn


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
    return engine


# ----------------------------------------------------
def get_s3_dynframe(s3_bucket, s3_path):
    """Get a dynamic_frame from S3 data.

    Args:
        s3_bucket (str): base S3 bucket
        s3_path (str): folder and object name of the S3 dataset.

    Returns:
        bison_points_dynf: dynamic frame containing occurrence records
    """
    full_path = f"{s3_bucket}/{s3_path}"
    bison_points_dynf = glueContext.create_dynamic_frame.from_options(
        format_options={},
        format="parquet",
        connection_type="s3",
        connection_options={
            "paths": [full_path],
            "recurse": True
        }
    )
    print(f"Read {full_path} with {bison_points_dynf.count()} records into dynamic frame.")
    bison_points_dynf.printSchema()
    return bison_points_dynf


# ----------------------------------------------------
def get_rds_table(secret, db_name, schema, tbl_name):
    """Get a table from Amazon RDS database.

    Args:
        secret (dict): database connection parameters.
        db_name (str): name of the database instance.
        schema (str): schema for database.
        tbl_name (str): table for connection.

    Returns:
        polygon_table: connection to a data table.
    """
    rds_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{db_name}"
    # .option("driver", "org.posgresql.Driver") \
    polygon_table = spark.read.format("jdbc") \
        .option("url", rds_url) \
        .option("dbtable", f"{schema}.{tbl_name}") \
        .option("user", secret["username"]) \
        .option("password", secret["password"]) \
        .load()
    return polygon_table


# ----------------------------------------------------
# ----------------------------------------------------
# rds_db_name = "bison_db_test"
rds_instance = "bison-db-test"
rds_db_schema = "lmb"
county_table = "county"
state_fieldname = "STUSPS"

# ----------------------------------------------------
sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

n = DT.now()
datastr = f"{n.year}-{n.month}-01"
# # Full data
# input_s3_path = f"raw_data/gbif_{datastr}.parquet/"
# Subset 5k data
input_s3_path = f"raw_data/gbif_5k_{datastr}.parquet/"
output_s3_path = f"out_data/bison_{datastr}.parquet/"

REGION = "us-east-1"
BISON_BUCKET = "s3://bison-321942852011-us-east-1/"
SECRET_NAME = "admin_bison-db-test"

secret = get_secret(SECRET_NAME, REGION)

# Get dynamic frame of point records
gbif_pt_recs = get_s3_dynframe(BISON_BUCKET, input_s3_path)

# Get spatial table
polygon_table = get_rds_table(secret, rds_instance, rds_db_schema, county_table)

intersected_points = gbif_pt_recs.crossJoin(polygon_table).filter(
    "ST_INTERSECTS("
    "ST_POINT(point_records.decimallongitude, point_records.decimallatitude), "
    "polygon_table.geometry")

# Intersect points with polygons
intersected_points = intersected_points.withColumn("georef_st", polygon_table.STUSPS)
job.commit()
