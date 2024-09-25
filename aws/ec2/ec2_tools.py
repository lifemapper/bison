"""Test workflow."""
import base64
import boto3
import csv
import datetime
import io
import json
import os
import pandas
from pprint import pp

# --------------------------------------------------------------------------------------
# Constants for GBIF data, local dev machine, EC2, S3
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# On EC2: Create a trimmed dataframe from CSV and save to S3 in parquet format
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
key_name = "aimee-aws-key"
aws_zone = "us-east-1"
# Allows KU Dyche hall
security_group_id = "sg-0b379fdb3e37389d1"

# Local machine data
local_ec2_key = f"/home/astewart/.ssh/{key_name}.pem"
local_path = "/mnt/sata8/tank/testgbif/occ_2023_06_23"
test_basename = "occurrence_100k"
csv_fname = f"{test_basename}.csv"

# DWCA data for download from GBIF
gbif_basename = "0042658-230530130749713"

# S3
dev_bucket = "specify-network-dev"
bucket_path = "gbif_test/gbif_dwc_extract"
# s3_csv_path = f"{bucket_path}/{test_basename}.csv"
# s3_parquet_path = f"{bucket_path}/{test_basename}.parquet"

# EC2 persistent
ec2_user = "ubuntu"
ec2_dev_ip = "54.156.84.82"

# EC2 Spot Instance
bison_ec2_template_name = "bison_launch_template"
instance_basename = "bison_dev"

script_filename = "scripts/user_data_for_ec2spot.sh"

target_capactity_spec = {
    "TotalTargetCapacity": 1,
    "SpotTargetCapacity": 1,
    "DefaultTargetCapacityType": "spot"
}
spot_opts = {
    "AllocationStrategy": "price-capacity-optimized",
    "InstanceInterruptionBehavior": "terminate"
}
launch_template_config = {
    "LaunchTemplateSpecification": {
        "LaunchTemplateName": bison_ec2_template_name,
        "Version": "1"}
}



# --------------------------------------------------------------------------------------
# Tools for experimentation
# --------------------------------------------------------------------------------------
# ----------------------------------------------------
def get_launch_template_from_instance(instance_id):
    """Get or create an EC2 launch template from an EC2 instance identifier.

    Args:
        instance_id: identifier for an EC2 instance to use as a template.

    Returns:
        launch_template_data: metadata to be used as an EC2 launch template.
    """
    ec2_client = boto3.client("ec2")
    launch_template_data = ec2_client.get_launch_template_data(InstanceId=instance_id)
    return launch_template_data


# ----------------------------------------------------
def delete_instance(instance_id):
    """Delete an EC2 instance.

    Args:
        instance_id: identifier for an EC2 instance to delete.

    Returns:
        response: response from the server.
    """
    ec2_client = boto3.client("ec2")
    response = ec2_client.delete_instance(InstanceId=instance_id)
    return response


# ----------------------------------------------------
def create_dataframe_from_gbifcsv_s3_bucket(bucket, csv_path):
    """Get or create an EC2 launch template from an EC2 instance identifier.

    Args:
        bucket: name for an S3 bucket.
        csv_path: bucket path, including object name, of CSV data of interest.

    Returns:
        df: pandas dataframe containing the tabular CSV data.
    """
    # Read CSV file from S3 into a pandas DataFrame
    s3_client = boto3.client("s3")
    s3_obj = s3_client.get_object(Bucket=bucket, Key=csv_path)
    df = pandas.read_csv(
        s3_obj["Body"], delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    return df


# --------------------------------------------------------------------------------------
# On local machine:
def find_instances(key_name, launch_template_name):
    """Describe all instances with given key_name and/or launch_template_id.

    Args:
        key_name: optional key_name assigned to an instance.
        launch_template_name: name assigned to the template which created the instance

    Returns:
        instances: list of metadata for instances
    """
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
    except Exception:
        pass
    else:
        for res in ress:
            _print_inst_info(res)
            instances.extend(res["Instances"])
    return instances


# --------------------------------------------------------------------------------------
# Hidden helper functions
# --------------------------------------------------------------------------------------
# ----------------------------------------------------
def _create_token(type):
    token = f"{type}_{datetime.datetime.now().timestamp()}"
    return token


# ----------------------------------------------------
def _get_user_data(script_filename):
    try:
        with open(script_filename, "r") as infile:
            script_text = infile.read()
    except Exception:
        return None
    else:
        text_bytes = script_text.encode("ascii")
        text_base64_bytes = base64.b64encode(text_bytes)
        base64_script_text = text_base64_bytes.decode("ascii")
        return base64_script_text


# ----------------------------------------------------
def _print_inst_info(reservation):
    resid = reservation["ReservationId"]
    inst = reservation["Instances"][0]
    print(f"ReservationId: {resid}")
    name = temp_id = None
    try:
        tags = inst["Tags"]
    except Exception:
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
        spot_template_name, security_group_id, script_filename, key_name):
    user_data_64 = _get_user_data(script_filename)
    launch_template_data = {
        "EbsOptimized": True,
        "IamInstanceProfile": {"Name": "AmazonEMR-InstanceProfile-20230404T163626"},
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "Encrypted": False,
                    "DeleteOnTermination": True,
                    "VolumeSize": 50, "VolumeType": "gp2"}
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
            }
        ],
        "ImageId": "ami-0a0c8eebcdd6dcbd0",
        "InstanceType": "t4g.medium",
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
                "Tags": [{"Key": "TemplateName", "Value": spot_template_name}]
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
    ec2_client = boto3.client("ec2")
    lnch_temp = None
    try:
        response = ec2_client.describe_launch_templates(
            LaunchTemplateNames=[template_name])
    except Exception:
        pass
    else:
        # LaunchTemplateName is unique
        try:
            lnch_temp = response["LaunchTemplates"][0]
        except Exception:
            pass
    return lnch_temp


# ----------------------------------------------------
def create_spot_launch_template(
        spot_template_name, security_group_id, script_filename, key_name):
    """Create a launch template for an EC2 Spot instance.

    Args:
        spot_template_name: Name to assign to this template.
        security_group_id: Identifier for security group to assign to the
            EC2 instance created from this template.
        script_filename: filename for a script to include as userdata on an
            EC2 instance created from this template.
        key_name: key name assigned to a launch template.

    Returns:
        success: boolean flag indicating success of the operation.
    """
    template = _get_launch_template(spot_template_name)
    if template is not None:
        success = True
    else:
        spot_template_data = _define_spot_launch_template_data(
            spot_template_name, security_group_id, script_filename, key_name)
        template_token = _create_token("template")
        ec2_client = boto3.client("ec2")
        response = ec2_client.create_launch_template(
            DryRun=False,
            ClientToken=template_token,
            LaunchTemplateName=spot_template_name,
            VersionDescription="Spot for GBIF process",
            LaunchTemplateData=spot_template_data
        )
        success = (response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    return success


# ----------------------------------------------------
def run_instance_spot(instance_basename, spot_template_name):
    """Create a launch template for an EC2 Spot instance.

    Args:
        instance_basename: Basename to assign to this EC2 Spot instance.
        spot_template_name: Name assigned to a spot launch template to be used
            creating this instance.

    Returns:
        string: instance id of the successfully launched EC2 Spot instance.
    """
    ec2_client = boto3.client("ec2")
    spot_token = _create_token("spot")
    instance_name = _create_token(instance_basename)
    response = ec2_client.run_instances(
        # KeyName=key_name,
        ClientToken=spot_token,
        MinCount=1, MaxCount=1,
        LaunchTemplate={"LaunchTemplateName": spot_template_name, "Version": "1"},
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": instance_name},
                {"Key": "TemplateName", "Value": spot_template_name}
            ]
        }]
    )
    try:
        instance = response["Instances"][0]
    except KeyError:
        instance = None
        print("No instance created")
    return instance["InstanceId"]


# ----------------------------------------------------
def write_dataframe_to_s3_parquet(df, bucket, parquet_path):
    """Write DataFrame to Parquet format and upload to S3.

    Args:
        df (pandas.DataFrame): tabular data to write on S3.
        bucket (str): Bucket identifier on S3.
        parquet_path (str): Destination path to the S3 parquet data.
    """
    s3_client = boto3.client("s3")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow")
    parquet_buffer.seek(0)
    s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)


# --------------------------------------------------------------------------------------
# Run on EC2 or local: Upload file to S3
# --------------------------------------------------------------------------------------
def upload_to_s3(local_path, filename, bucket, s3_path):
    """Upload a local file to S3.

    Args:
        local_path: path to data to upload.
        filename: filename of the data to upload.
        bucket (str): Bucket identifier on S3.
        s3_path (str): Destination path to the S3 data.

    TODO: Use aws_tools method here
    """
    s3_client = boto3.client("s3")
    local_filename = os.path.join(local_path, filename)
    s3_client.upload_file(local_filename, bucket, s3_path)
    print(f"Successfully uploaded {filename} to s3://{bucket}/{s3_path}")


# # --------------------------------------------------------------------------------------
# # On local machine: Trim CSV file to named columns and save in parquet format
# # --------------------------------------------------------------------------------------
# def trim_gbifcsv_to_parquet(local_filename, parquet_filename):
#     """Trim a GBIF CSV file to a subset of fields, then write to S3 in parquet format.
#
#     Args:
#         local_filename: path and filename of data to subset and upload.
#         parquet_filename: filename for the destination parquet file on S3.
#     """
#     gbif_dataframe = pandas.read_csv(
#         local_filename, delimiter="\t", encoding="utf-8", low_memory=False,
#         quoting=csv.QUOTE_NONE)
#     # Trim the DataFrame to the subset of fieldnames
#     trimmed_gbif_dataframe = gbif_dataframe[FIELD_SUBSET]
#     # Write the trimmed DataFrame to Parquet file format
#     trimmed_gbif_dataframe.to_parquet(parquet_filename)


# --------------------------------------------------------------------------------------
# On local machine: Describe the instance with the instance_id
# --------------------------------------------------------------------------------------
def get_instance(instance_id):
    """Get an EC2 instance.

    Args:
        instance_id: identifier for an EC2 instance to retrieve.

    Returns:
        instance: metadata response from the server.
    """
    ec2_client = boto3.client("ec2")
    response = ec2_client.describe_instances(
        InstanceIds=[instance_id],
        DryRun=False,
    )
    try:
        instance = response["Reservations"][0]["Instances"][0]
    except Exception:
        instance = None
    return instance


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == '__main__':

# # -------  Find or create template -------
# # Adds the script to the spot template
# success = create_spot_launch_template(
#         spot_template_name, security_group_id, script_filename, key_name)
#
# # -------  Run instance from template -------
# # Runs the script on instantiation
# instance_id = run_instance_spot(instance_basename, spot_template_name)
#
# instance = get_instance(instance_id)
# ip = instance["PublicIpAddress"]

    # Get an existing launch template
    launch_template_file = "/tmp/bison_launch_template.json"
    bison_dev_instance_id = "i-0cc438734534bb505"
    launch_template = get_launch_template_from_instance(bison_dev_instance_id)
    # Make easier to read
    json_str = json.dumps(launch_template, indent=4)
    with open(launch_template_file, "w") as outf:
        outf.write(json_str)

    # Edit this launch template

    # Upload new launch template

    create_spot_launch_template(
        spot_template_name, security_group_id, script_filename, key_name)