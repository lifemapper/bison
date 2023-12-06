"""Glue Spark script to read S3 occurrence data and report the number of records."""
import sys
import datetime as DT
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf
from pyspark.context import SparkContext

bison_bucket = "s3://bison-321942852011-us-east-1/"

n = DT.datetime.now()
datastr = f"{n.year}-{n.month}-01"

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

# Read county species list with occurrence counts
site_species_s3_fullname = f"{bison_bucket}/out_data/county_lists_000"
# BISON raw dataset
site_species_dynf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [site_species_s3_fullname],
        "recurse": True,
    },
)
print(f"Read BISON county x species data in  {site_species_s3_fullname} with {site_species_dynf.count()} records.")


job.commit()
