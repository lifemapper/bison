"""Glue Spark script to read GBIF Open Data Registry data, subset it, and save it to S3."""
import datetime as DT
import sys
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

bison_bucket = "s3://bison-321942852011-us-east-1/"
gbif_bucket = "s3://gbif-open-data-us-east-1/"

n = DT.datetime.now()
datastr = f"{n.year}-{n.month}-01"

bison_s3_fullname = f"{bison_bucket}/raw_data/gbif_{datastr}.parquet/"
gbif_s3_fullname = f"{gbif_bucket}/occurrence/{datastr}/occurrence.parquet/"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

conf = SparkConf()
conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

sc = SparkContext.getOrCreate(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# # Small test 5000 record dataset
# bison_s3_fullname = f"{bison_bucket}/raw_data/gbif_5k_{datastr}.parquet/"
# gbif_dynf = glueContext.create_sample_dynamic_frame_from_options(
#     num=5000,
#     format="parquet",
#     connection_type="s3",
#     connection_options={
#         "paths": [ gbif_s3_fullname ],
#         "recurse": True
#     },
#     # transformation_ctx="S3bucket_node_gbif"
# )
# print(f"Created 5k dynamic frame of GBIF {gbif_s3_fullname}.")

# Full dataset
gbif_dynf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    format="parquet",
    connection_type="s3",
    connection_options={
        "paths": [gbif_s3_fullname],
        "recurse": True
    }
)
print(f"Read GBIF {gbif_s3_fullname} with {gbif_dynf.count()} records into dynamic frame.")

gbif_filtered_dynf = gbif_dynf.filter(
    f=lambda x: x["countrycode"] == "US" and x["occurrencestatus"] == "PRESENT" and x["taxonrank"] in ["SPECIES", "SUBSPECIES", "VARIETY", "FORM", "INFRASPECIFIC_NAME", "INFRASUBSPECIFIC_NAME"])
print(f"Filtered GBIF to dynamic frame with {gbif_filtered_dynf.count()} records .")

glueContext.write_dynamic_frame.from_options(
    frame=gbif_filtered_dynf,
    connection_type="s3",
    connection_options={"path": bison_s3_fullname},
    format="parquet",
    format_options={
        "useGlueParquetWriter": True,
        "compression": "snappy"
    }
)
print(f"Wrote dynamic frame to {bison_s3_fullname}.")

job.commit()
