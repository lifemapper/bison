"""Glue Spark script to read S3 occurrence data and report the number of records."""
import sys
import datetime as DT
import pandas

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

bison_bucket = "s3://bison-321942852011-us-east-1/"
data_catalog = "bison-metadata"
county_dataname = "county_lists_000"
output_dataname = "heatmatrix"
n = DT.datetime.now()
datastr = f"{n.year}-{n.month}-01"

# .............................................................................
def create_county_dict(input_df):
    """Create a dictionary of counties with species and counts.

    Args:
        input_df: dataframe from S3 table of counties with species counts

    Returns:
        counties: Dict of dictionaries {county: {species: count, ...}, ...}.
    """
    county_dict = {}
    species = set()
    # First county
    cnty = f"{input_df[0]['census_state']}_{input_df[0]['census_county']}"
    # Create a dictionary of dictionaries {county: {species: count, ...}, ...}
    for idx, in_row in input_df.iterrows():
        curr_cnty = f"{in_row['census_state']}_{in_row['census_county']}"
        sp_name = in_row["scientificname"]
        sp_count = in_row["occ_count"]
        species.add(sp_name)
        if curr_cnty != cnty:
            cnty = curr_cnty
            county_dict[cnty] = {sp_name: sp_count}
        else:
            county_dict[cnty][sp_name] = sp_count
    return county_dict, species

# .............................................................................
# Main
# .............................................................................
# Read county species list with occurrence counts
# county_species_list_s3_fullname = f"{bison_bucket}/out_data/{county_dataname}"
# input_dynf = glueContext.create_dynamic_frame.from_options(
#     format_options={},
#     connection_type="s3",
#     format="csv",
#     delimiter="\t",
#     connection_options={
#         "paths": [county_species_list_s3_fullname],
#         "recurse": True,
#     },
# )

input_dynf = glueContext.create_dynamic_frame.from_catalog(
    database=data_catalog, table_name=county_dataname)

print(f"Read BISON county x species data in  {county_dataname} with "
      f"{input_dynf.count()} records.")

# input_df = spark.read.load(
#     county_species_list_s3_fullname, format="csv", delimiter="\t", header=True)
# # Make sure indexes pair with number of rows
# input_df = input_df.reset_index()
# print(f"Read BISON county x species data in  {county_species_list_s3_fullname} "
#       f"with {input_df.count()} county records.")

# county_dict = create_county_dict(input_dynf)
county_dict = {}
species = set()
# Create a dictionary of dictionaries {county: {species: count, ...}, ...}
cnty = f"{input_dynf[0]['census_state']}_{input_dynf[0]['census_county']}"
for idx, in_row in input_dynf.iterrows():
    curr_cnty = f"{in_row['census_state']}_{in_row['census_county']}"
    sp_name = in_row["scientificname"]
    sp_count = in_row["occ_count"]
    species.add(sp_name)
    if curr_cnty != cnty:
        cnty = curr_cnty
        county_dict[cnty] = {sp_name: sp_count}
    else:
        county_dict[cnty][sp_name] = sp_count

county_names = list(county_dict.keys())
county_names.sort()
print(f"Read {len(county_names)} counties and {len(species)} species")

# Create an empty pandas dataframe
site_species_df = pandas.DataFrame(columns=list(species), index=county_names)
# Fill dataframe
for cnty_name in county_names:
    sp_dict = county_dict[cnty_name]
    for sp, cnt in sp_dict.items():
        site_species_df[cnty_name][sp] = cnt
# Convert to a pyspark dataframe
heatmatrix_df = spark.createDataFrame(site_species_df)

heatmatrix_s3_fullname = f"{bison_bucket}/out_data/{output_dataname}.csv"
heatmatrix_df.write.option("header", "true").csv(heatmatrix_s3_fullname)
print(f"Wrote dataframe to {heatmatrix_s3_fullname}.")

# glueContext.write_dynamic_frame.from_options(
#     frame=gbif_filtered_dynf,
#     connection_type="s3",
#     connection_options={"path": bison_s3_fullname},
#     format="parquet",
#     format_options={
#         "useGlueParquetWriter": True,
#         "compression": "snappy"
#     }
# )
# print(f"Wrote dynamic frame to {bison_s3_fullname}.")


job.commit()
