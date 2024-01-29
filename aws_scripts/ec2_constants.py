# Constants for BISON project: GBIF data, local dev machine, EC2, S3.
PROJ_NAME = "bison"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

PROJ_BUCKET = f"{PROJ_NAME}-321942852011-us-east-1"
SPOT_TEMPLATE_BASENAME = f"launch_template"

KEY_NAME = "aimee-aws-key"
REGION = "us-east-1"
# Allows KU Dyche hall
SECURITY_GROUP_ID = "sg-0b379fdb3e37389d1"

# S3
TRIGGER_PATH = "trigger"
TRIGGER_FILENAME = "go.txt"

# EC2 Spot Instance
# List of instance types at https://aws.amazon.com/ec2/spot/pricing/
INSTANCE_TYPE = "t2.micro"
# INSTANCE_TYPE = "a1.large"

# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = "%d %b %Y %H:%M"
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"

# --------------------------------------------------------------------------------------
# Methods for constructing and instantiating EC2 instances
# --------------------------------------------------------------------------------------

#
#
# # ----------------------------------------------------
# def define_spot_launch_template_data(template_name, script_filename):
#     user_data_64 = get_user_data(script_filename)
#     launch_template_data = {
#         "EbsOptimized": True,
#         "IamInstanceProfile":
#             {"Name": "AmazonEMR-InstanceProfile-20230404T163626"},
#             #  "Arn":
#             #      "arn:aws:iam::321942852011:instance-profile/AmazonEMR-InstanceProfile-20230404T163626",
#             # },
#         "BlockDeviceMappings": [
#             {
#                 "DeviceName": "/dev/sda1",
#                 "Ebs": {
#                     "Encrypted": False,
#                     "DeleteOnTermination": True,
#                     # "SnapshotId": "snap-0a6ff81ccbe3194d1",
#                     "VolumeSize": 50, "VolumeType": "gp2"
#                 }
#             }],
#         "NetworkInterfaces": [
#             {
#                 "AssociatePublicIpAddress": True,
#                 "DeleteOnTermination": True,
#                 "Description": "",
#                 "DeviceIndex": 0,
#                 "Groups": [SECURITY_GROUP_ID],
#                 "InterfaceType": "interface",
#                 "Ipv6Addresses": [],
#                 # "PrivateIpAddresses": [
#                 #     {"Primary": True, "PrivateIpAddress": "172.31.16.201"}
#                 # ],
#                 # "SubnetId": "subnet-0beb8b03a44442eef",
#                 # "NetworkCardIndex": 0
#             }],
#         "ImageId": "ami-0a0c8eebcdd6dcbd0",
#         "InstanceType": INSTANCE_TYPE,
#         "KeyName": KEY_NAME,
#         "Monitoring": {"Enabled": False},
#         "Placement": {
#             "AvailabilityZone": "us-east-1c", "GroupName": "", "Tenancy": "default"},
#         "DisableApiTermination": False,
#         "InstanceInitiatedShutdownBehavior": "terminate",
#         "UserData": user_data_64,
#         "TagSpecifications": [
#             {
#                 "ResourceType": "instance",
#                 "Tags": [{"Key": "TemplateName", "Value": template_name}]
#             }],
#         "InstanceMarketOptions": {
#             "MarketType": "spot",
#             "SpotOptions": {
#                 "MaxPrice": "0.033600",
#                 "SpotInstanceType": "one-time",
#                 "InstanceInterruptionBehavior": "terminate"
#             }},
#         "CreditSpecification": {"CpuCredits": "unlimited"},
#         "CpuOptions": {"CoreCount": 2, "ThreadsPerCore": 1},
#         "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
#         "HibernationOptions": {"Configured": False},
#         "MetadataOptions": {
#             "HttpTokens": "optional",
#             "HttpPutResponseHopLimit": 1,
#             "HttpEndpoint": "enabled",
#             "HttpProtocolIpv6": "disabled",
#             "InstanceMetadataTags": "disabled"},
#         "EnclaveOptions": {"Enabled": False},
#         "PrivateDnsNameOptions": {
#             "HostnameType": "ip-name",
#             "EnableResourceNameDnsARecord": True,
#             "EnableResourceNameDnsAAAARecord": False},
#         "MaintenanceOptions": {"AutoRecovery": "default"},
#         "DisableApiStop": False
#     }
#     return launch_template_data
#
# # ----------------------------------------------------
# def get_user_data(script_filename):
#     try:
#         with open(script_filename, "r") as infile:
#             script_text = infile.read()
#     except Exception:
#         return None
#     else:
#         text_bytes = script_text.encode("ascii")
#         text_base64_bytes = base64.b64encode(text_bytes)
#         base64_script_text = text_base64_bytes.decode("ascii")
#         return base64_script_text
#
# # ----------------------------------------------------
# def create_token(type):
#     token = f"{type}_{datetime.datetime.now().timestamp()}"
#     return token
#
#
# # ----------------------------------------------------
# def get_date_str():
#     n = datetime.datetime.now()
#     date_str = f"{n.year}-{n.month}-{n.day}"
#     return date_str
#
#
# # ----------------------------------------------------
# def get_current_date_str():
#     n = datetime.datetime.now()
#     date_str = f"{n.year}-{n.month}-01"
#     return date_str
#
#
# # ----------------------------------------------------
# def get_previous_date_str():
#     n = datetime.datetime.now()
#     yr = n.year
#     mo = n.month
#     if n.month == 1:
#         mo = 12
#         yr -= 1
#     date_str = f"{yr}-{mo}-01"
#     return date_str
#
#
# # ----------------------------------------------------
# def create_spot_launch_template(
#         ec2_client, template_name, instance_type, security_group_id, script_filename,
#         key_name, overwrite=False):
#     """Create an EC2 Spot Instance Launch template on AWS.
#
#     Args:
#         ec2_client: an object for communicating with EC2.
#         template_name: name for the launch template
#         instance_type: AWS-defined type of instance.
#         security_group_id: identifier of an existing security group.
#         script_filename: script to be installed and run on EC2 instantiation.
#         key_name: name of authentication key with permission to execute this process.
#
#     Returns:
#         success: boolean flag indicating the success of creating launch template.
#     """
#     success = False
#     template = get_launch_template(template_name)
#     if template is not None:
#         success = True
#     else:
#         spot_template_data = define_spot_launch_template_data(
#             template_name, instance_type, security_group_id, script_filename,
#             key_name)
#         template_token = create_token("template")
#         try:
#             response = ec2_client.create_launch_template(
#                 DryRun=False,
#                 ClientToken=template_token,
#                 LaunchTemplateName=template_name,
#                 VersionDescription="Spot for GBIF/BISON process",
#                 LaunchTemplateData=spot_template_data
#             )
#         except ClientError as e:
#             print(f"Failed to create launch template {template_name}, ({e})")
#         else:
#             success = (response["ResponseMetadata"]["HTTPStatusCode"] == 200)
#     return success
#
#
# # ----------------------------------------------------
# def upload_trigger_to_s3(trigger_name, s3_bucket, s3_bucket_path):
#     """Upload a file to S3 which will trigger a workflow.
#
#     Args:
#         trigger_name: Name of workflow to trigger.
#         s3_bucket: name of the S3 bucket destination.
#         s3_bucket_path: the data destination inside the S3 bucket (without filename).
#
#     Returns:
#         s3_filename: the URI to the file in the S3 bucket.
#     """
#     filename = f"{trigger_name}.txt"
#     with open(filename, "r") as f:
#         f.write("go!")
#     s3_client = boto3.client("s3")
#     obj_name = f"{s3_bucket_path}/{filename}"
#     try:
#         s3_client.upload_file(filename, s3_bucket, obj_name)
#     except ClientError as e:
#         print(f"Failed to upload {obj_name} to {s3_bucket}, ({e})")
#     else:
#         s3_filename = f"s3://{s3_bucket}/{obj_name}"
#         print(f"Successfully uploaded {filename} to {s3_filename}")
#     return s3_filename
#
#
# # ----------------------------------------------------
# def write_dataframe_to_s3_parquet(df, bucket, parquet_path):
#     """Convert DataFrame to Parquet format and upload to S3.
#
#     Args:
#         df: pandas DataFrame containing data.
#         bucket: name of the S3 bucket destination.
#         parquet_path: the data destination inside the S3 bucket
#     """
#     s3_client = boto3.client("s3")
#     parquet_buffer = io.BytesIO()
#     df.to_parquet(parquet_buffer, engine="pyarrow")
#     parquet_buffer.seek(0)
#     s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)
#
#
# # ----------------------------------------------------
# def upload_to_s3(local_filename, bucket, s3_path):
#     """Upload a file to S3.
#
#     Args:
#         local_filename: Full path to local file for upload.
#         bucket: name of the S3 bucket destination.
#         s3_path: the data destination inside the S3 bucket (without filename).
#     """
#     s3_client = boto3.client("s3")
#     filename = os.path.split(local_filename)[1]
#     s3_client.upload_file(local_filename, bucket, s3_path)
#     print(f"Successfully uploaded {filename} to s3://{bucket}/{s3_path}")
#
#
# # ----------------------------------------------------
# def get_instance(instance_id):
#     """Describe an EC2 instance with instance_id.
#
#     Args:
#         instance_id: EC2 instance identifier.
#
#     Returns:
#         instance: metadata for the EC2 instance
#     """
#     ec2_client = boto3.client("ec2")
#     response = ec2_client.describe_instances(
#         InstanceIds=[instance_id],
#         DryRun=False,
#     )
#     try:
#         instance = response["Reservations"][0]["Instances"][0]
#     except Exception:
#         instance = None
#     return instance
#
#
# # ----------------------------------------------------
# def run_instance_spot(ec2_client, proj_name, template_name):
#     """Run an EC2 Spot Instance on AWS.
#
#     Args:
#         ec2_client: an object for communicating with EC2.
#         proj_name: project name used for creating a token for this instance.
#         template_name: name for the launch template to be used for instantiation.
#
#     Returns:
#         instance_id: unique identifier of the new Spot instance.
#     """
#     instance_id = None
#     spot_token = create_token("spot")
#     instance_name = create_token(proj_name)
#     try:
#         response = ec2_client.run_instances(
#             # KeyName=key_name,
#             ClientToken=spot_token,
#             MinCount=1, MaxCount=1,
#             LaunchTemplate={"LaunchTemplateName": template_name, "Version": "1"},
#             TagSpecifications=[
#                 {
#                     "ResourceType": "instance",
#                     "Tags": [
#                         {"Key": "Name", "Value": instance_name},
#                         {"Key": "TemplateName", "Value": template_name}
#                     ]
#                 }
#             ]
#         )
#     except ClientError as e:
#         print(f"Failed to instantiate Spot instance {spot_token}, ({e})")
#     else:
#         try:
#             instance = response["Instances"][0]
#         except KeyError:
#             print("No instance returned")
#         else:
#             instance_id = instance["InstanceId"]
#     return instance_id
#
#
#
# # --------------------------------------------------------------------------------------
# # Tools for experimentation
# # --------------------------------------------------------------------------------------
#
# # ----------------------------------------------------upload_to_s3
# def _print_inst_info(reservation):
#     resid = reservation["ReservationId"]
#     inst = reservation["Instances"][0]
#     print(f"ReservationId: {resid}")
#     name = temp_id = None
#     try:
#         tags = inst["Tags"]
#     except Exception:
#         pass
#     else:
#         for t in tags:
#             if t["Key"] == "Name":
#                 name = t["Value"]
#             if t["Key"] == "aws:ec2launchtemplate:id":
#                 temp_id = t["Value"]
#     ip = inst["PublicIpAddress"]
#     state = inst["State"]["Name"]
#     print(f"Instance name: {name}, template: {temp_id}, IP: {ip}, state: {state}")
#
#
# # ----------------------------------------------------
# def find_instances(key_name, launch_template_name):
#     """Describe all EC2 instances with name or launch_template_id.
#
#     Args:
#         key_name: EC2 instance name
#         launch_template_name: EC2 launch template name
#
#     Returns:
#         instances: list of metadata for EC2 instances
#     """
#     ec2_client = boto3.client("ec2")
#     filters = []
#     if launch_template_name is not None:
#         filters.append({"Name": "tag:TemplateName", "Values": [launch_template_name]})
#     if key_name is not None:
#         filters.append({"Name": "key-name", "Values": [key_name]})
#     response = ec2_client.describe_instances(
#         Filters=filters,
#         DryRun=False,
#         MaxResults=123,
#         # NextToken="string"
#     )
#     instances = []
#     try:
#         ress = response["Reservations"]
#     except Exception:
#         pass
#     else:
#         for res in ress:
#             _print_inst_info(res)
#             instances.extend(res["Instances"])
#     return instances
#
#
# # ----------------------------------------------------
# def get_launch_template_from_instance(instance_id, region_name=REGION):
#     """Return a JSON formatted template from an existing EC2 instance.
#
#     Args:
#         instance_id: unique identifier for the selected EC2 instance.
#
#     Returns:
#         launch_template_data: a JSON formatted launch template.
#     """
#     ec2_client = boto3.client("ec2")
#     launch_template_data = ec2_client.get_launch_template_data(InstanceId=instance_id)
#     return launch_template_data
#
#
# # --------------------------------------------------------------------------------------
# # On local machine: Describe the launch_template with the template_name
# def get_launch_template(template_name):
#     ec2_client = boto3.client("ec2", region_name=REGION)
#     lnch_temp = None
#     # Find pre-existing template
#     try:
#         response = ec2_client.describe_launch_templates(
#             LaunchTemplateNames=[template_name],
#         )
#     except Exception:
#         pass
#     else:
#         # LaunchTemplateName is unique
#         try:
#             lnch_temp = response["LaunchTemplates"][0]
#         except Exception:
#             pass
#     return lnch_temp
#
#
# # ----------------------------------------------------
# def delete_launch_template(template_name):
#     """Delete an EC2 launch template AWS.
#
#     Args:
#         template_name: name of the selected EC2 launch template.
#
#     Returns:
#         response: a JSON formatted AWS response.
#     """
#     response = None
#     ec2_client = boto3.client("ec2")
#     lnch_tmpl = get_launch_template(template_name)
#     if lnch_tmpl is not None:
#         response = ec2_client.delete_launch_template(LaunchTemplateName=template_name)
#     return response
#
#
# # ----------------------------------------------------
# def delete_instance(instance_id):
#     """Delete an EC2 instance.
#
#     Args:
#         instance_id: unique identifier for the selected EC2 instance.
#
#     Returns:
#         response: a JSON formatted AWS response.
#     """
#     ec2_client = boto3.client("ec2")
#     response = ec2_client.delete_instance(InstanceId=instance_id)
#     return response
#
# # ----------------------------------------------------
# def get_logger(log_directory, log_name, log_level=logging.INFO):
#     """Get a logger for writing logging messages to file and console.
#
#     Args:
#         log_directory: absolute path for the logfile.
#         log_name: Name for the log object and output log file.
#         log_level: logging constant error level (logging.INFO, logging.DEBUG,
#                 logging.WARNING, logging.ERROR)
#
#     Returns:
#         logger: logging.Logger object
#     """
#     filename = f"{log_name}.log"
#     if log_directory is not None:
#         filename = os.path.join(log_directory, f"{filename}")
#         os.makedirs(log_directory, exist_ok=True)
#     # create file handler
#     handler = RotatingFileHandler(
#         filename, mode="w", maxBytes=LOGFILE_MAX_BYTES, backupCount=10,
#         encoding="utf-8"
#     )
#     formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
#     handler.setLevel(log_level)
#     handler.setFormatter(formatter)
#     # Get logger
#     logger = logging.getLogger(log_name)
#     logger.setLevel(logging.DEBUG)
#     # Add handler to logger
#     logger.addHandler(handler)
#     logger.propagate = False
#     return logger
#
# # ----------------------------------------------------
# def create_dataframe_from_gbifcsv_s3_bucket(bucket, csv_path):
#     """Read CSV data from S3 into a pandas DataFrame.
#
#     Args:
#         bucket: name of the bucket containing the CSV data.
#         csv_path: the CSV object name with enclosing S3 bucket folders.
#
#     Returns:
#         df: pandas DataFrame containing the CSV data.
#     """
#     s3_client = boto3.client("s3")
#     s3_obj = s3_client.get_object(Bucket=bucket, Key=csv_path)
#     df = pandas.read_csv(
#         s3_obj["Body"], delimiter="\t", encoding="utf-8", low_memory=False,
#         quoting=csv.QUOTE_NONE)
#     return df
#
