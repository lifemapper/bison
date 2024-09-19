"""Lambda function to delete temporary and previous month's tables."""
# Set lambda timeout to 5 minutes.
import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime

print('Loading function')

# AWS region for all services
region = "us-east-1"
# Redshift settings
workgroup = "bison"
database = "dev"
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
pub_schema = "public"
external_schema = "redshift_spectrum"
# S3 settings
bison_bucket = "bison-321942852011-us-east-1"
input_folder = "input"
# Timeout for connect/read
timeout = 60
# Wait time between result checks
waittime = 1

# Dataload filename postfixes
dt = datetime.now()
yr = dt.year
mo = dt.month
gbif_datestr = f"{yr}-{mo:02d}-01"
# S3 GBIF bucket and file to query
gbif_bucket = f"gbif-open-data-{region}"
parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)
s3_client = session.client("s3", config=config)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """List the first 10 objects in the current public GBIF data on S3.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute S3 query command.
    """
    print("Received trigger: " + json.dumps(event, indent=2))

    # Check for latest GBIF content
    try:
        odr_response = s3_client.list_objects_v2(
            Bucket=gbif_bucket, Prefix=parquet_key, MaxKeys=10)
    except Exception as e:
        print(f"Error getting bucket/object {gbif_bucket}/{parquet_key} ({e})")
        raise e
    try:
        contents = odr_response["Contents"]
    except KeyError:
        msg = f"Found NO items in {gbif_bucket} in {parquet_key}"
    else:
        for item in contents:
            print(f"GBIF item: {item['Key']}")
        msg = f"Found {len(contents)}+ items in {gbif_bucket} in {parquet_key}"

    return {
        'statusCode': 200,
        'body': json.dumps(msg)
    }
