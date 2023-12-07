"""Glue Spark script to read S3 occurrence data and report the number of records."""
import sys
import datetime as DT
import pandas

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
county_species_list_s3_fullname = f"{bison_bucket}/out_data/county_lists_000"
# BISON raw dataset
county_species_list_dynf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [county_species_list_s3_fullname],
        "recurse": True,
    },
)
print(f"Read BISON county x species data in  {county_species_list_s3_fullname} with {county_species_list_dynf.count()} records.")

agg_df = spark.read.load(
    county_species_list_s3_fullname, format="csv", delimiter="\t", header=True)
print(f"Read BISON county x species data in  {county_species_list_s3_fullname} "
      f"with {agg_df.count()} county records.")

# Track columns {species name: index}, rows {state_county: index}
columns = {}
rows = {}

# Find index of field of interest
name_idx = agg_df.columns.index("scientificname")
count_idx = agg_df.columns.index("occ_count")

# Iterate over rows/counties
row_cnt = agg_df.size[0]
# agg_df.foreach()
for i in range(row_cnt):
    # Rows = counties
    county = f"{agg_df[i]['census_state']}_{agg_df[i]['census_county']}"
    rows[county] = i
    # Columns = species
    species = agg_df[i]["scientificname"]
    count = agg_df[i]["occ_count"]
    # Choose existing species column
    if species in columns.keys():
        col_idx = columns[species]
    # or create another
    else:
        col_idx = len(columns)
        columns[species] = col_idx
    # Count each species for county
    agg_df.loc[i][col_idx] = count
job.commit()
