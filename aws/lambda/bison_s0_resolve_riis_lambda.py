"""Lambda function to aggregate counts and lists by region."""
# Set lambda timeout to 3 minutes.
import boto3
import botocore.session as bc
from botocore.client import Config

print("*** Loading lambda function")

region = "us-east-1"
workgroup = "bison"
database = "dev"
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
pub_schema = "public"
external_schema = "redshift_spectrum"
timeout = 900
waittime = 1
EC2_TASK_INSTANCE_ID = "i-0bc5a64e9385902a6"
# EC2_TASK_INSTANCE_ARN = "arn:aws:ec2:us-east-1:321942852011:instance/{EC2_TASK_INSTANCE_ID}"

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize EC2 and SSM clients
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_ec2 = session.client("ec2", config=config)
client_ssm = session.client("ssm", config=config)

# Bison command
bison_script = "venv/bin/python -m bison.task.test_task"


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Start an EC2 instance to resolve RIIS records, then write the results to S3.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        instance_id (number): ID of the EC2 instance started.

    Raises:
        Exception: on unexpected response.
        Exception: on no instances started.
        Exception: on unknown error.
    """
    response = client_ec2.start_instances(
        InstanceIds=[EC2_TASK_INSTANCE_ID],
        AdditionalInfo="Task initiated by Lambda",
        DryRun=False
    )
    try:
        instance_meta = response["StartingInstances"][0]
    except KeyError:
        raise Exception(f"Invalid response returned {response}")
    except ValueError:
        raise Exception(f"No instances returned in {response}")
    except Exception:
        raise

    instance_id = instance_meta['InstanceId']
    prev_state = instance_meta['PreviousState']['Name']
    curr_state = instance_meta['CurrentState']['Name']
    print(f"Started instance {instance_meta['InstanceId']}. ")
    print(f"Moved from {prev_state} to {curr_state}")

    # sudo docker compose -f compose.annotate_riis.yml up
    response = client_ssm.send_command(
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': [bison_script]},
        InstanceIds=[EC2_TASK_INSTANCE_ID]
    )
    return instance_id
