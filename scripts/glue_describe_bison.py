import datetime as DT
import sys
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

bison_bucket = "s3://bison-321942852011-us-east-1/"
gbif_bucket = "s3://gbif-open-data-us-east-1/"

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

# Small test 5000 record dataset
bison_subset_s3_fullname = f"{bison_bucket}/raw_data/gbif_5k_{datastr}.parquet/"
# BISON raw dataset
bison_subset_dynf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [bison_subset_s3_fullname],
        "recurse": True,
    },
)
print(f"Read GBIF {bison_subset_s3_fullname} with {bison_subset_dynf.count()} records.")


# BISON raw dataset
bison_s3_bad_fullname = f"{bison_bucket}/gbif_{datastr}.geoparquet/"
bison_dynf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [bison_s3_bad_fullname],
        "recurse": True,
    },
)
print(f"Read GBIF {bison_s3_bad_fullname} with {bison_dynf.count()} records.")


job.commit()
