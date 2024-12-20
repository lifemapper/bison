"""Lambda to start an EC2 instance to create/analyze a PAM from specimen records."""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import botocore.session as bc
from botocore.client import Config
from datetime import datetime

PROJECT = "bison"
TASK = "calc_stats"
print(f"*** Loading function {PROJECT}_s8_{TASK}")

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
EC2_SPOT_TEMPLATE = f"{PROJECT}_spot_task_template"
EC2_INSTANCE_NAME = f"{PROJECT}_{TASK}"

# .............................................................................
# Initialize Botocore session
# .............................................................................
timeout = 300

session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=REGION)
# Initialize EC2 client
config = Config(connect_timeout=timeout, read_timeout=timeout)
ec2_client = session.client("ec2", config=config)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Start an EC2 instance to resolve RIIS records, then write the results to S3.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        instance_id (number): ID of the EC2 instance started.

    Raises:
        Exception: on requested template does not exist.
        NoCredentialsError: on failure to get credentials to run EC2 task.
        ClientError: on failure to run EC2 task.
        Exception: on no instances started.
        Exception: on unknown error.
    """
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
            f"!!! Template {EC2_SPOT_TEMPLATE} version {TASK} does not exist")
    print(f"*** Found template {EC2_SPOT_TEMPLATE} version {ver_num} for {TASK}.")

    print("*** ---------------------------------------")
    print("*** Launch EC2 instance with task template version")

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
                        {"Key": "Name", "Value": EC2_INSTANCE_NAME},
                        {"Key": "TemplateName", "Value": EC2_SPOT_TEMPLATE}
                    ]
                }
            ]
        )
    except NoCredentialsError:
        print("!!! Failed to authenticate for run_instances")
        raise
    except ClientError:
        print(
            f"!!! Failed to run instance for template {EC2_SPOT_TEMPLATE}, "
            f"version {ver_num}/{TASK}")
        raise

    try:
        instance = response["Instances"][0]
    except KeyError:
        raise Exception(f"!!! No instances returned in {response}")

    instance_id = instance["InstanceId"]
    print(f"*** Started instance {instance_id}. ")

    return {
        "statusCode": 200,
        "body": f"Executed bison_s1_annotate_riis lambda starting EC2 {instance_id}"
    }
