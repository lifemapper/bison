"""Lambda function to aggregate counts and lists by region."""
# Set lambda timeout to 3 minutes.
import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

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

# Get previous data date
yr = datetime.now().year
mo = datetime.now().month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
bison_datestr = f"{yr}_{mo:02d}_01"
bison_tbl = f"bison_{bison_datestr}"

# Define the bison bucket and table to create
bison_bucket = "bison-321942852011-us-east-1"
input_folder = "input"

# List of tuples, (command for logfile, corresponding AWS command)
COMMANDS = [
    # Userdata script will pull
    #   bison repo (or bison docker image)
    #   run docker compose -f docker-compose.development.yml -f docker-compose.yml  up
    ("launch_ec2_annotate_riis"),
    (""),
    (),
    ()]

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_ec2 = session.client("ec2", config=config)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Annotate BISON records with RIIS regions and status of species in record region.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    # Execute the commmands in order
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_ec2
        except Exception:
            raise

        print("*** ---------------------------------------")
        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {stmt}")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete, then describe result
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = client_ec2.describe_statement(Id=submit_id)
            except Exception as e:
                print(f"*** Failed to describe_statement in {elapsed_time} secs: {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    complete = True
                    if status == "FAILED":
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

    return {
        'statusCode': 200,
        'body': json.dumps("Lambda result logged")
    }



ec2 = boto3.client('ec2', region_name=REGION)


def lambda_handler(event, context):

    instance = ec2.run_instances(
        ImageId=AMI,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        MaxCount=1,
        MinCount=1
    )

    print ("New instance created:")
    instance_id = instance['Instances'][0]['InstanceId']
    print (instance_id)

    return instance_id