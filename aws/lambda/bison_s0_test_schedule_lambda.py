"""Lambda function to test EC2 task execution."""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import json

print('Loading function')
PROJECT = "bison"

# .............................................................................
# Dataload filename postfixes
# .............................................................................
dt = datetime.now()
yr = dt.year
mo = dt.month
bison_datestr = f"{yr}_{mo:02d}_01"
gbif_datestr = f"{yr}-{mo:02d}-01"

# .............................................................................
# AWS constants
# .............................................................................
REGION = "us-east-1"
AWS_ACCOUNT = "321942852011"
AWS_METADATA_URL = "http://169.254.169.254/latest/"
WORKFLOW_ROLE_NAME = f"{PROJECT}_redshift_lambda_role"
WORKFLOW_ROLE_ARN = f"arn:aws:iam::{PROJECT}:role/service-role/{WORKFLOW_ROLE_NAME}"
WORKFLOW_USER = f"project.{PROJECT}"

# EC2 launch template/version
EC2_SPOT_TEMPLATE = "bison_spot_task_template"
TASK = "test_task"

# S3 locations
S3_BUCKET = f"{PROJECT}-{AWS_ACCOUNT}-{REGION}"
S3_IN_DIR = "input"
S3_OUT_DIR = "output"
S3_LOG_DIR = "log"
S3_SUMMARY_DIR = "summary"
RIIS_BASENAME = "USRIISv2_MasterList"
annotated_riis_key = f"{S3_IN_DIR}/{RIIS_BASENAME}_annotated_{bison_datestr}.csv"

# .............................................................................
# Initialize Botocore session and clients
# .............................................................................
timeout = 300
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=REGION)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
s3_client = session.client("s3", config=config, region_name=REGION)
ec2_client = session.client("ec2", config=config)

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
    ver_num = None
    print("*** ---------------------------------------")
    print("*** Find template version")
    response = ec2_client.describe_launch_template_versions(
        LaunchTemplateName=EC2_SPOT_TEMPLATE
    )
    versions = response["LaunchTemplateVersions"]
    for ver in versions:
        if ver["VersionDescription"] == TASK:
            ver_num = ver["VersionNumber"]
            break
    if ver_num is None:
        raise Exception(
            f"Template {EC2_SPOT_TEMPLATE} version {TASK} "
            "does not exist")
    print(f"*** Found template {EC2_SPOT_TEMPLATE} version {ver_num} for {TASK}.")

    print("*** ---------------------------------------")
    print("*** Launch EC2 instance with task template version")
    instance_name = f"bison_{TASK}"

    try:
        response = ec2_client.run_instances(
            MinCount=1, MaxCount=1,
            LaunchTemplate={
                "LaunchTemplateName": EC2_SPOT_TEMPLATE, "Version": f"{ver_num}"
            },
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": instance_name},
                        {"Key": "TemplateName", "Value": EC2_SPOT_TEMPLATE}
                    ]
                }
            ]
        )
    except NoCredentialsError:
        print("Failed to authenticate for run_instances")
        raise
    except ClientError:
        print(
            f"Failed to run instance for template {EC2_SPOT_TEMPLATE}, "
            f"version {ver_num}/{TASK}")
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
