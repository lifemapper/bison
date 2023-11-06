"""Glue script to pull data subset from AWS GBIF Open Data Registry and write to S3."""
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime as DT
from pyspark import SparkConf
from pyspark.context import SparkContext
import sys

region = "us-east-1"
bison_path = f"s3://bison-321942852011-{region}"
gbif_path = f"s3://gbif-open-data-{region}/occurrence"

n = DT.datetime.now()
datastr = f"{n.year}-{n.month}-01"

gbif_s3_fullname = f"{gbif_path}/{datastr}/occurrence.parquet/"
bison_s3_fullname = f"{bison_path}/raw_data/gbif_{datastr}.parquet/"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

conf = SparkConf()
conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

sc = SparkContext.getOrCreate(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Input from GBIF S3 bucket
gbif_full_data = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [gbif_s3_fullname],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node_gbif",
)

# Create filtered DynamicFrame with custom lambda to filter records
gbif_subset = Filter.apply(
    frame=gbif_full_data,
    f=lambda x: x["countrycode"] == "US" and x["occurrencestatus"] == "PRESENT" and x["taxonrank"] in ["species", "subspecies", "variety", "form", "infraspecific_name", "infrasubspecific_name"] and "COORDINATE_INVALID" not in x["issue"] and "COORDINATE_OUT_OF_RANGE" not in x["issue"] and "COORDINATE_REPROJECTION_FAILED" not in x["issue"] and "COORDINATE_REPROJECTION_SUSPICIOUS" not in x["issue"])

# Output to BISON S3 bucket
bison_full_data = glueContext.write_dynamic_frame.from_options(
    frame=gbif_subset,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": bison_s3_fullname,
        "compression": "snappy",
        "partitionKeys": []
    },
    transformation_ctx="S3bucket_node_bison",
)

job.commit()
