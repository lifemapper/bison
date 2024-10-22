"""Lambda function to delete temporary and previous month's tables."""
# Set lambda timeout to 5 minutes.
import boto3
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

EC2_TASK_INSTANCE_ID = "i-0bc5a64e9385902a6"
EC2_ROLE_NAME = f"{PROJECT}_ec2_s3_role"
# Instance types: https://aws.amazon.com/ec2/spot/pricing/
EC2_INSTANCE_TYPE = "t4g.micro"

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
    do_annotate = False
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
        do_annotate = True
    else:
        print(f"*** Found object: {contents[0]['Key']}")

    # -------------------------------------
    # Start instance to run task
    if do_annotate is True:
        print("*** ---------------------------------------")
        print("*** Initiate EC2 start_instances task")
        try:
            response = ec2_client.start_instances(
                InstanceIds=[EC2_TASK_INSTANCE_ID],
                AdditionalInfo="Task initiated by Lambda",
                DryRun=False
            )
        except Exception:
            print("*** Failed to start EC2 instance")
            raise

        try:
            instance_meta = response["StartingInstances"][0]
        except KeyError:
            raise Exception(f"*** Invalid response returned {response}")
        except ValueError:
            raise Exception(f"*** No instances returned in {response}")
        except Exception:
            raise

        instance_id = instance_meta['InstanceId']
        print(f"*** Started instance {instance_meta['InstanceId']}. ")

        # -------------------------------------
        # Loop til running
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = ec2_client.describe_instances(
                    InstanceIds=[instance_id])
            except Exception as e:
                print(f"*** Failed to describe_instances in {elapsed_time} secs: {e}")
                raise

            else:
                try:
                    instance_meta = describe_result["Reservations"][0]["Instances"][0]
                except Exception:
                    print(f"*** Failed to return metadata for instance {instance_id}.")
                    raise

                state = instance_meta["State"]["Name"].lower()
                print(f"*** Instance {state} after {elapsed_time} seconds")
                if state != "running":
                    time.sleep(waittime)
                    elapsed_time += waittime
                else:
                    complete = True

            if elapsed_time >= timeout and not complete:
                complete = True
                print(f"*** Failed with {state} state after {elapsed_time} seconds")

    return {
        "statusCode": 200,
        "body": "Executed bison_s0_test_schedule_lambda"
    }
