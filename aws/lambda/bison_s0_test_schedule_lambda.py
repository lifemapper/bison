"""Lambda function to delete temporary and previous month's tables."""
# Set lambda timeout to 5 minutes.
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import json
import time

print('Loading function')

PROJECT = "bison"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
AWS_METADATA_URL = "http://169.254.169.254/latest/"

WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"
WORKFLOW_ROLE_ARN = f"arn:aws:iam::{PROJECT}:role/service-role/{WORKFLOW_ROLE_NAME}"
WORKFLOW_USER = f"project.{PROJECT}"
WORKFLOW_SECRET_NAME = f"{PROJECT}_workflow_user"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

EC2_SPOT_TEMPLATE = "bison_spot_task_template"
EC2_USERDATA_TEST_TASK = "test_task.userdata.sh"
EC2_USERDATA_ANNOTATE_RIIS = "annotate_riis.userdata.sh"
EC2_USERDATA_BUILD_SUMMARIES = "build_summaries.userdata.sh"
EC2_USERDATA_BUILD_HEATMAP = "build_heatmap.userdata.sh"

S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"

RIIS_BASENAME = "USRIISv2_MasterList"
db_user = f"IAMR:{WORKFLOW_ROLE_NAME}"
database = "dev"
pub_schema = "public"
external_schema = "redshift_spectrum"

# Timeout for ec2 startup
timeout = 300
# Wait time between result checks
waittime = 30

# Dataload filename postfixes
dt = datetime.now()
yr = dt.year
mo = dt.month
bison_datestr = f"{yr}_{mo:02d}_01"
gbif_datestr = f"{yr}-{mo:02d}-01"

gbif_parquet_key = f"occurrence/{gbif_datestr}/{GBIF_ODR_FNAME}"
annotated_riis_key = f"input/{RIIS_BASENAME}_annotated_{bison_datestr}.csv"

query_stmt = "riis_output_s3"

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=REGION)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
s3_client = session.client("s3", config=config, region_name=REGION)
ec2_client = session.client("ec2", config=config)
ssm_client = session.client("ssm", config=config, region_name=REGION)

template = EC2_SPOT_TEMPLATE
task_userdata_fname = EC2_USERDATA_TEST_TASK
task = task_userdata_fname.rstrip('.userdata.sh')

# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Check for an S3 object, delete if exists, export from Redshift to S3.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute S3 query command.
        Exception: on failure to execute S3 delete command.
        Exception: on failure to execute Redshift export command.
    """
    print("*** ---------------------------------------")
    print("*** Received trigger: " + json.dumps(event))
    print("*** ---------------------------------------")

    # -------------------------------------
    # Look for existing S3 annotated data
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=annotated_riis_key, MaxKeys=10)
    except Exception as e:
        print(f"*** Error querying for bucket/object {annotated_riis_key} ({e})")
        raise e
    try:
        contents = tr_response["Contents"]
    except KeyError:
        print(f"*** Object {annotated_riis_key} is not present")
    else:
        raise Exception(
            f"*** Annotated RIIS data is already present: {annotated_riis_key}")

    # -------------------------------------
    # Start instance to run task
    version_num = None
    print("*** ---------------------------------------")
    print("*** Find template version")
    response = ec2_client.describe_launch_template_versions(
        LaunchTemplateName=template
    )
    versions = response["LaunchTemplateVersions"]
    for ver in versions:
        if ver["VersionDescription"] == task_userdata_fname:
            version_num = ver["VersionNumber"]
            break
    if version_num is None:
        raise Exception(
            f"Template {template} version {task} "
            "does not exist")

    print("*** ---------------------------------------")
    print("*** Launch EC2 instance with task template version")
    instance_name = f"bison_{task}"

    try:
        response = ec2_client.run_instances(
            MinCount=1, MaxCount=1,
            LaunchTemplate={
                "LaunchTemplateName": template,
                "Version": f"{version_num}"
            },
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": instance_name},
                        {"Key": "TemplateName", "Value": template}
                    ]
                }
            ]
        )
    except NoCredentialsError:
        print("Failed to authenticate for run_instances")
        raise
    except ClientError:
        print(
            f"Failed to run instance for template {template}, "
            f"version {version_num}/{task}")
        raise

    try:
        instance = response["Instances"][0]
    except KeyError:
        raise Exception(f"*** No instances returned in {response}")

    instance_id = instance["InstanceId"]
    print(f"*** Started instance {instance_id}. ")

    return {
        "statusCode": 200,
        "body": f"Executed bison_s0_test_schedule_lambda starting EC2 {instance_id}"
    }
