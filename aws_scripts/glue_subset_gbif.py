"""Glue Spark script to read GBIF Open Data Registry data, subset it, and save it to S3."""
"""
This script runs on AWS Glue in approximately XXX minutes.  If it runs longer, check 
the input and output S3 locations are correct.  

Name bison_subset_gbif in AWS Glue jobs.
"""
import datetime as DT
import sys
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Query GBIF data in our working region
region = "us-east-1"
bison_bucket = f"s3://bison-321942852011-{region}/"
gbif_bucket = f"s3://gbif-open-data-{region}/"

# Date to identify dataset
n = DT.datetime.now()
datastr = f"{n.year}-{n.month:02d}-01"

bison_s3_fullname = f"{bison_bucket}/input_data/gbif_{datastr}.parquet/"
gbif_s3_fullname = f"{gbif_bucket}/occurrence/{datastr}/occurrence.parquet/"

# Spark Job configuration
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
conf = SparkConf()
# Handle different versions of parquet
conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
sc = SparkContext.getOrCreate(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

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

# Subset to BISON specifications
gbif_filtered_dynf = gbif_dynf.filter(
    f=lambda x: x["countrycode"] == "US" and x["occurrencestatus"] == "PRESENT" and x["taxonrank"] in ["SPECIES", "SUBSPECIES", "VARIETY", "FORM", "INFRASPECIFIC_NAME", "INFRASUBSPECIFIC_NAME"])
print(f"Filtered GBIF to dynamic frame with {gbif_filtered_dynf.count()} records .")

# Write subset to BISON bucket
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
