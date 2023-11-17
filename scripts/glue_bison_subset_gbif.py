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
# bison_s3_fullname = f"{bison_path}/raw_data/gbif_5k_{datastr}.parquet/"
# gbif_full_data = glueContext.create_sample_dynamic_frame_from_options(
#     connection_type="s3",
#     connection_options={
#         "paths": [ gbif_s3_fullname ],
#         "recurse": True
#     },
#     num=5000,
#     format="parquet",
#     transformation_ctx="S3bucket_node_gbif"
# )

# Full dataset
gbif_dynf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [gbif_s3_fullname],
        "recurse": True,
        # 'groupFiles': 'inPartition',
        # 'groupSize': '1073741824'
    },
    # transformation_ctx="S3bucket_node_gbif",
)
print(f"Read GBIF {gbif_s3_fullname} with {gbif_dynf.count()} records.")

gbif_filtered_dynf = gbif_dynf.filter(
    f=lambda x: x["countrycode"] == "US" and x["occurrencestatus"] == "PRESENT" and x["taxonrank"] in ["SPECIES", "SUBSPECIES", "VARIETY", "FORM", "INFRASPECIFIC_NAME", "INFRASUBSPECIFIC_NAME"])
print(f"Filtered GBIF to dynamic frame with {gbif_filtered_dynf.count()} records.")

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

# # Create filtered DynamicFrame with custom lambda to filter records
# gbif_subset = Filter.apply(
#     frame = gbif_full_data,
#     f=lambda row: (
#         bool(bool(re.match("US", row["countrycode"]))
#             and bool(re.match("PRESENT", row["occurrencestatus"]))
#             and bool(
#                 re.match("SPECIES", row["taxonrank"])
#                 or bool(re.match("SUBSPECIES", row["taxonrank"]))
#                 or bool(re.match("VARIETY", row["taxonrank"]))
#                 or bool(re.match("FORM", row["taxonrank"]))
#                 or bool(re.match("INFRASPECIFIC_NAME", row["taxonrank"]))
#                 or bool(re.match("INFRASUBSPECIFIC_NAME", row["taxonrank"]))
#                 )
#             )
#     ))


# # Output to BISON S3 bucket
# bison_full_data = glueContext.write_dynamic_frame.from_options(
#     frame=gbif_subset,
#     connection_type="s3",
#     connection_options={
#         "path": bison_s3_fullname,
#     },
#     format="parquet",
#     format_options={
#         "useGlueParquetWriter": True,
#         "compression": "snappy"
#     },
#     transformation_ctx="S3bucket_node_bison",
# )

job.commit()
