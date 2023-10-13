"""Script to initiate AWS (EC2 Spot to S3) download of GBIF data."""
import base64
import boto3
from botocore.exceptions import ClientError
import csv
import datetime
import io
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas

from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------------------------------------------
# Constants for GBIF data, local dev machine, EC2, S3
# --------------------------------------------------------------------------------------
PROJ_NAME = "bison"

GBIF_BUCKET = f"gbif-open-data-us-east-1/occurrence"

MY_BUCKET = f"{PROJ_NAME}-321942852011-us-east-1"
KEY_NAME = "aimee-aws-key"
REGION = "us-east-1"
# Allows KU Dyche hall
SECURITY_GROUP_ID = "sg-0b379fdb3e37389d1"

GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

# S3
ORIG_DATA_PATH = "orig_data"
TRIGGER_FILENAME = "go.txt"

# EC2 Spot Instance
SPOT_TEMPLATE_NAME = f"{PROJ_NAME}_launch_template"
# List of instance types at https://aws.amazon.com/ec2/spot/pricing/
INSTANCE_TYPE = "a1.medium"
# INSTANCE_TYPE = "a1.large"
# TODO: Define the GBIF download file as an environment variable in template
#  instead of user_data script
USER_DATA_FILENAME = "scripts/user_data_for_ec2spot.sh"

# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5


# ----------------------------------------------------
def get_logger(log_directory, log_name, log_level=logging.INFO):
    filename = f"{log_name}.log"
    if log_directory is not None:
        filename = os.path.join(log_directory, f"{filename}")
        os.makedirs(log_directory, exist_ok=True)
    # create file handler
    handler = RotatingFileHandler(
        filename, mode="w", maxBytes=LOGFILE_MAX_BYTES, backupCount=10,
        encoding="utf-8"
    )
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    # Get logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    # Add handler to logger
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# --------------------------------------------------------------------------------------
# Tools for experimentation
# --------------------------------------------------------------------------------------
# ----------------------------------------------------
def get_launch_template_from_instance(instance_id):
    ec2_client = boto3.client("ec2")
    launch_template_data = ec2_client.get_launch_template_data(InstanceId=instance_id)
    return launch_template_data


# ----------------------------------------------------
def delete_launch_template(template_name):
    response = None
    ec2_client = boto3.client("ec2")
    lnch_tmpl = _get_launch_template(template_name)
    if lnch_tmpl is not None:
        response = ec2_client.delete_launch_template(LaunchTemplateName=template_name)
    return response


# ----------------------------------------------------
def delete_instance(instance_id):
    ec2_client = boto3.client("ec2")
    response = ec2_client.delete_instance(InstanceId=instance_id)
    return response


# ----------------------------------------------------
def create_dataframe_from_gbifcsv_s3_bucket(bucket, csv_path):
    # Read CSV file from S3 into a pandas DataFrame
    s3_client = boto3.client("s3")
    s3_obj = s3_client.get_object(Bucket=bucket, Key=csv_path)
    df = pandas.read_csv(
        s3_obj["Body"], delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    return df


# ----------------------------------------------------
def get_client(profile, service):
    session = boto3.Session(profile_name=profile)
    client = session.client(service)
    return client


# ----------------------------------------------------
def find_instances(key_name, launch_template_name):
    # Describe all instances with key_name or launch_template_id
    ec2_client = boto3.client("ec2")
    filters = []
    if launch_template_name is not None:
        filters.append({"Name": "tag:TemplateName", "Values": [launch_template_name]})
    if key_name is not None:
        filters.append({"Name": "key-name", "Values": [key_name]})
    response = ec2_client.describe_instances(
        Filters=filters,
        DryRun=False,
        MaxResults=123,
        # NextToken="string"
    )
    instances = []
    try:
        ress = response["Reservations"]
    except:
        pass
    else:
        for res in ress:
            _print_inst_info(res)
            instances.extend(res["Instances"])
    return instances


# ----------------------------------------------------
def write_dataframe_to_s3_parquet(df, bucket, parquet_path):
    # Write DataFrame to Parquet format and upload to S3
    s3_client = boto3.client("s3")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)
    s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)


# ----------------------------------------------------
def upload_to_s3(local_path, filename, bucket, s3_path):
    s3_client = boto3.client("s3")
    local_filename = os.path.join(local_path, filename)
    s3_client.upload_file(local_filename, bucket, s3_path)
    print(f"Successfully uploaded {filename} to s3://{bucket}/{s3_path}")


# ----------------------------------------------------
def get_instance(instance_id):
    # Describe instance
    ec2_client = boto3.client("ec2")
    response = ec2_client.describe_instances(
        InstanceIds=[instance_id],
        DryRun=False,
    )
    try:
        instance = response["Reservations"][0]["Instances"][0]
    except:
        instance = None
    return instance



# --------------------------------------------------------------------------------------
# Hidden helper functions
# --------------------------------------------------------------------------------------
# ----------------------------------------------------
def _create_token(type):
    token = f"{type}_{datetime.datetime.now().timestamp()}"
    return token


# ----------------------------------------------------
def get_date_str():
    n = datetime.datetime.now()
    date_str = f"{n.year}-{n.month}-{n.day}"
    return date_str


# ----------------------------------------------------
def get_current_gbif_subdir():
    n = datetime.datetime.now()
    date_str = f"{n.year}-{n.month}-01"
    return date_str


# ----------------------------------------------------
def get_prev_gbif_subdir():
    n = datetime.datetime.now()
    yr = n.year
    mo = n.month
    if n.month == 1:
        mo = 12
        yr -= 1
    date_str = f"{yr}-{mo}-01"
    return date_str


# ----------------------------------------------------
def _get_user_data(script_filename):
    try:
        with open(script_filename, "r") as infile:
            script_text = infile.read()
    except:
        return None
    else:
        text_bytes = script_text.encode("ascii")
        text_base64_bytes = base64.b64encode(text_bytes)
        base64_script_text = text_base64_bytes.decode("ascii")
        return base64_script_text


# ----------------------------------------------------upload_to_s3
def _print_inst_info(reservation):
    resid = reservation["ReservationId"]
    inst = reservation["Instances"][0]
    print(f"ReservationId: {resid}")
    name = temp_id = None
    try:
        tags = inst["Tags"]
    except:
        pass
    else:
        for t in tags:
            if t["Key"] == "Name":
                name = t["Value"]
            if t["Key"] == "aws:ec2launchtemplate:id":
                temp_id = t["Value"]
    ip = inst["PublicIpAddress"]
    state = inst["State"]["Name"]
    print(f"Instance name: {name}, template: {temp_id}, IP: {ip}, state: {state}")


# ----------------------------------------------------
def _define_spot_launch_template_data(
        template_name, instance_type, security_group_id, script_filename, key_name):
    user_data_64 = _get_user_data(script_filename)
    launch_template_data = {
        "EbsOptimized": True,
        "IamInstanceProfile":
            {
                # "Arn":
                #     "arn:aws:iam::321942852011:instance-profile/AmazonEMR-InstanceProfile-20230404T163626",
                 "Name": "AmazonEMR-InstanceProfile-20230404T163626"
            },
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "Encrypted": False,
                    "DeleteOnTermination": True,
                    # "SnapshotId": "snap-0a6ff81ccbe3194d1",
                    "VolumeSize": 50, "VolumeType": "gp2"
                }
            }
        ],
        "NetworkInterfaces": [
            {
                "AssociatePublicIpAddress": True,
                "DeleteOnTermination": True,
                "Description": "",
                "DeviceIndex": 0,
                "Groups": [security_group_id],
                "InterfaceType": "interface",
                "Ipv6Addresses": [],
                # "PrivateIpAddresses": [
                #     {"Primary": True, "PrivateIpAddress": "172.31.16.201"}
                # ],
                # "SubnetId": "subnet-0beb8b03a44442eef",
                # "NetworkCardIndex": 0
            }
        ],
        "ImageId": "ami-0a0c8eebcdd6dcbd0",
        "InstanceType": instance_type,
        "KeyName": key_name,
        "Monitoring": {"Enabled": False},
        "Placement": {
            "AvailabilityZone": "us-east-1c", "GroupName": "", "Tenancy": "default"
        },
        "DisableApiTermination": False,
        "InstanceInitiatedShutdownBehavior": "terminate",
        "UserData": user_data_64,
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "TemplateName", "Value": template_name}]
            }
        ],
        "InstanceMarketOptions": {
            "MarketType": "spot",
            "SpotOptions": {
                "MaxPrice": "0.033600",
                "SpotInstanceType": "one-time",
                "InstanceInterruptionBehavior": "terminate"
            }
        },
        "CreditSpecification": {"CpuCredits": "unlimited"},
        "CpuOptions": {"CoreCount": 2, "ThreadsPerCore": 1},
        "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
        "HibernationOptions": {"Configured": False},
        "MetadataOptions": {
            "HttpTokens": "optional",
            "HttpPutResponseHopLimit": 1,
            "HttpEndpoint": "enabled",
            "HttpProtocolIpv6": "disabled",
            "InstanceMetadataTags": "disabled"
        },
        "EnclaveOptions": {"Enabled": False},
        "PrivateDnsNameOptions": {
            "HostnameType": "ip-name",
            "EnableResourceNameDnsARecord": True,
            "EnableResourceNameDnsAAAARecord": False},
        "MaintenanceOptions": {"AutoRecovery": "default"},
        "DisableApiStop": False
    }
    return launch_template_data


# --------------------------------------------------------------------------------------
# On local machine: Describe the launch_template with the template_name
def _get_launch_template(template_name):
    ec2_client = boto3.client("ec2", region_name=REGION)
    lnch_temp = None
    try:
        response = ec2_client.describe_launch_templates(
            LaunchTemplateNames=[template_name],
        )
    except:
        pass
    else:
        # LaunchTemplateName is unique
        try:
            lnch_temp = response["LaunchTemplates"][0]
        except:
            pass
    return lnch_temp


# ----------------------------------------------------
def create_spot_launch_template(
        ec2_client, template_name, instance_type, security_group_id, script_filename, key_name):
    success = False
    template = _get_launch_template(template_name)
    if template is not None:
        success = True
    else:
        spot_template_data = _define_spot_launch_template_data(
            template_name, instance_type, security_group_id, script_filename,
            key_name)
        template_token = _create_token("template")
        try:
            response = ec2_client.create_launch_template(
                DryRun = False,
                ClientToken = template_token,
                LaunchTemplateName = template_name,
                VersionDescription = "Spot for GBIF/BISON process",
                LaunchTemplateData = spot_template_data
            )
        except ClientError as e:
            print(f"Failed to create launch template {template_name}, ({e})")
        else:
            success = (response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    return success


# ----------------------------------------------------
def run_instance_spot(ec2_client, proj_name, template_name):
    instance_id = None
    spot_token = _create_token("spot")
    instance_name = _create_token(proj_name)
    try:
        response = ec2_client.run_instances(
            # KeyName=key_name,
            ClientToken=spot_token,
            MinCount=1, MaxCount=1,
            LaunchTemplate = {"LaunchTemplateName": template_name, "Version": "1"},
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": instance_name},
                    {"Key": "TemplateName", "Value": template_name}
                ]
            }]
        )
    except ClientError as e:
        print(f"Failed to instantiate Spot instance {spot_token}, ({e})")
    else:
        try:
            instance = response["Instances"][0]
        except KeyError:
            print("No instance returned")
        else:
            instance_id = instance["InstanceId"]
    return instance_id


# ----------------------------------------------------
def upload_trigger_to_s3(filename, s3_bucket, s3_bucket_path):
    with open(filename, "r") as f:
        f.write("go!")
    s3_client = boto3.client("s3")
    obj_name = f"{s3_bucket_path}/{filename}"
    try:
        s3_client.upload_file(filename, s3_bucket, obj_name)
    except ClientError as e:
        print(f"Failed to upload {obj_name} to {s3_bucket}, ({e})")
    else:
        s3_filename = f"s3://{s3_bucket}/{obj_name}"
        print(f"Successfully uploaded {filename} to {s3_filename}")
    return s3_filename



# --------------------------------------------------------------------------------------
# On EC2: Create a trimmed dataframe from CSV and save to S3 in parquet format
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # -------  Create a logger -------
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = get_logger(None, script_name)

    # ------- Get EC2 client -------
    ec2_client = boto3.client("ec2", region_name=REGION)

    # -------  Find or create template -------
    # Adds the script to the spot template
    success = create_spot_launch_template(
        ec2_client, SPOT_TEMPLATE_NAME, INSTANCE_TYPE, SECURITY_GROUP_ID,
        USER_DATA_FILENAME, KEY_NAME)

    # -------  Run instance from template -------
    # Runs the script on instantiation
    response = run_instance_spot(ec2_client, PROJ_NAME, SPOT_TEMPLATE_NAME)

    # Create and upload a file triggering an event that converts the CSV to parquet
    fname = "go.txt"
    with open(fname, "r") as f:
        f.write("go!")
    trigger = upload_to_s3(TRIGGER_FILENAME, MY_BUCKET, ORIG_DATA_PATH)
