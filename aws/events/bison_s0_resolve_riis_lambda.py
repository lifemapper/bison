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
bison_task_instance_id = "i-0595bfd381e64d2c9"
# bison_task_ec2 = "arn:aws:ec2:us-east-1:321942852011:instance/i-0595bfd381e64d2c9"

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize EC2 and SSM clients
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_ec2 = session.client("ec2", config=config)
client_ssm = session.client("ssm", config=config)

# Bison command
bison_script = "~/bison/bison/tools/annotate_riis.py"

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
        InstanceIds=[bison_task_instance_id],
        AdditionalInfo="Task initiated by Lambda",
        DryRun=False
    )
    try:
        instance_meta = response["StartingInstances"][0]
    except KeyError:
        raise Exception(f"Invalid response returned {instance_meta}")
    except ValueError:
        raise Exception(f"No instances returned in {instance_meta}")
    except Exception:
        raise

    instance_id = instance_meta['InstanceId']
    prev_state = instance_meta['PreviousState']['Name']
    curr_state = instance_meta['CurrentState']['Name']
    print(f"Started instance {instance_meta['InstanceId']}. ")
    print(f"Moved from {curr_state} to {prev_state}")

    response = client_ssm.send_command(
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': [script]},
        InstanceIds=exec_list
    )
    return instance_id
