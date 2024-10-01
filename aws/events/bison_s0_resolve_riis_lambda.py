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

# AMI = "ami-096ea6a12ea24a797"
# INSTANCE_TYPE = "t4g.micro"
# KEY_NAME = "aimee-aws-key"

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_ec2 = session.client("ec2", config=config)
# ec2 = boto3.client('ec2', region_name=region)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Start an EC2 instance to resolve RIIS records, then write the results to S3.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        instance_id (number): ID of the EC2 instance started.

    Raises:
        Exception: on failure to start the EC2 instance
    """
    instance = client_ec2.run_instances(
        # ImageId=AMI,
        # InstanceType=INSTANCE_TYPE,
        # KeyName=KEY_NAME,
        MaxCount=1,
        MinCount=1
    )

    print("New instance created:")
    instance_id = instance['Instances'][0]['InstanceId']
    print(instance_id)

    return instance_id
