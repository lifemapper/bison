import datetime as DT
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

bison_path = "s3://bison-321942852011-us-east-1/raw_data/"
gbif_path = "s3://gbif-open-data-us-east-1/occurrence"
n = DT.datetime.now()
cur_folder = f"{n.year}-{n.month}-01"
gbif_s3_path = f"{gbif_path}/{cur_folder}/occurrence.parquet/"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
gbif_full_data = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    datetimeRebaseMode="CORRECTED",
    connection_options={
        "paths": [ gbif_s3_path ],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node_gbif",
)

# Create filtered DynamicFrame with custom lambda to filter records
# gbif_subset = gbif_full_data.filter(
#     f=lambda x: x["countrycode"] EQUALS "US" and x["occurrencestatus"] EQUALS "PRESENT" and x["taxonrank"] in [ "species", "subspecies", "variety", "form", "infraspecific_name", "infrasubspecific_name"] and "COORDINATE_INVALID" not in x["issue"] and "COORDINATE_OUT_OF_RANGE" not in x["issue"] and "COORDINATE_REPROJECTION_FAILED" not in x["issue"] and "COORDINATE_REPROJECTION_SUSPICIOUS" not in x["issue"])
gbif_subset = Filter.apply(
    frame = gbif_full_data,
    f = lambda x: x["countrycode"] == "US" and x["occurrencestatus"] == "PRESENT" and x["taxonrank"] in ["species", "subspecies", "variety", "form", "infraspecific_name", "infrasubspecific_name"] and "COORDINATE_INVALID" not in x["issue"] and "COORDINATE_OUT_OF_RANGE" not in x["issue"] and "COORDINATE_REPROJECTION_FAILED" not in x["issue"] and "COORDINATE_REPROJECTION_SUSPICIOUS" not in x["issue"])

# Convert the dynamic frame to a Spark DataFrame for further processing
subset_df = gbif_subset.toDF()

# Write the filtered data to a Parquet file in S3
subset_df.write.parquet(bison_path, mode="overwrite")

# # Script generated for node S3 bucket
# S3bucket_node_bison = glueContext.write_dynamic_frame.from_options(
#     frame=gbif_subset,
#     connection_type="s3",
#     format="glueparquet",
#     connection_options={
#         "path": "s3://bison-321942852011-us-east-1/raw_data/",
#         "compression": "snappy", "partitionKeys": []},
#     transformation_ctx="S3bucket_node_bison",
# )

job.commit()
