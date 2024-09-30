"""Tools to use either locally or on EC2 to initiate BISON AWS EC2 Instances."""
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
import base64
import boto3
from botocore.exceptions import ClientError
import datetime as DT
import os

from bison.common.constants import (
    INSTANCE_TYPE, KEY_NAME, REGION, SECURITY_GROUP_ID, USER_DATA_TOKEN, WORKFLOW_ROLE
)


# .............................................................................
class aws:
    """Class for working with AWS tools."""
    # ----------------------------------------------------
    @classmethod
    def get_secret(cls, secret_name, region):
        """Get a secret from the Secrets Manager for connection authentication.

        Args:
            secret_name: name of the secret to retrieve.
            region: AWS region containint the secret.

        Returns:
            a dictionary containing the secret data.

        Raises:
            ClientError:  an AWS error in communication.
        """
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region)
        try:
            secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise (e)
        # Decrypts secret using the associated KMS key.
        secret_str = secret_value_response["SecretString"]
        return eval(secret_str)

    # ----------------------------------------------------
    @classmethod
    def get_authenticated_session(cls, region):
        session = boto3.Session()
        sts = session.client("sts", region_name=region)
        response = sts.assume_role(
            RoleArn=WORKFLOW_ROLE, RoleSessionName="authenticated-bison-session")
        creds = response['Credentials']
        new_session = boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken'])
        return new_session


# --------------------------------------------------------------------------------------
# Methods for constructing and instantiating EC2 instances
# --------------------------------------------------------------------------------------
# .............................................................................
class ec2:
    """Class for creating and manipulating ec2 instances."""
    TEMPLATE_BASENAME = "launch_template"
    # ----------------------------------------------------
    def __init__(self, region=REGION):
        self.client = boto3.client("ec2", region=region)

    # ----------------------------------------------------
    def create_spot_launch_template_name(self, proj_prefix, desc_str=None):
        """Create a name identifier for a Spot Launch Template.

        Args:
            desc_str (str): optional descriptor to include in the name.

        Returns:
            template_name (str): name for identifying this Spot Launch Template.
        """
        if desc_str is None:
            template_name = f"{proj_prefix}_{self.TEMPLATE_BASENAME}"
        else:
            template_name = f"{proj_prefix}_{desc_str}_{self.TEMPLATE_BASENAME}"
        return template_name

    # ----------------------------------------------------
    def define_spot_launch_template_data(
            self, template_name, user_data_filename, script_filename,
            token_to_replace=USER_DATA_TOKEN):
        """Create the configuration data for a Spot Launch Template.

        Args:
            template_name: unique name for this Spot Launch Template.
            user_data_filename: full filename for script to be included in the
                template and executed on Spot instantiation.
            script_filename: full filename for script to be inserted into user_data file.
            token_to_replace: string within the user_data_filename which will be replaced
                by the text in the script filename.

        Returns:
            launch_template_data (dict): Dictionary of configuration data for the template.
        """
        user_data_64 = self.get_user_data(
            user_data_filename, script_filename, token_to_replace=token_to_replace)
        launch_template_data = {
            "EbsOptimized": True,
            "IamInstanceProfile":
                {"Name": "AmazonEMR-InstanceProfile-20230404T163626"},
                #  "Arn":
                #      "arn:aws:iam::321942852011:instance-profile/AmazonEMR-InstanceProfile-20230404T163626",
                # },
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "Encrypted": False,
                        "DeleteOnTermination": True,
                        # "SnapshotId": "snap-0a6ff81ccbe3194d1",
                        "VolumeSize": 50, "VolumeType": "gp2"
                    }
                }],
            "NetworkInterfaces": [
                {
                    "AssociatePublicIpAddress": True,
                    "DeleteOnTermination": True,
                    "Description": "",
                    "DeviceIndex": 0,
                    "Groups": [SECURITY_GROUP_ID],
                    "InterfaceType": "interface",
                    "Ipv6Addresses": [],
                    # "PrivateIpAddresses": [
                    #     {"Primary": True, "PrivateIpAddress": "172.31.16.201"}
                    # ],
                    # "SubnetId": "subnet-0beb8b03a44442eef",
                    # "NetworkCardIndex": 0
                }],
            "ImageId": "ami-0a0c8eebcdd6dcbd0",
            "InstanceType": INSTANCE_TYPE,
            "KeyName": KEY_NAME,
            "Monitoring": {"Enabled": False},
            "Placement": {
                "AvailabilityZone": "us-east-1c", "GroupName": "", "Tenancy": "default"},
            "DisableApiTermination": False,
            "InstanceInitiatedShutdownBehavior": "terminate",
            "UserData": user_data_64,
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "TemplateName", "Value": template_name}]
                }],
            "InstanceMarketOptions": {
                "MarketType": "spot",
                "SpotOptions": {
                    "MaxPrice": "0.033600",
                    "SpotInstanceType": "one-time",
                    "InstanceInterruptionBehavior": "terminate"
                }},
            "CreditSpecification": {"CpuCredits": "unlimited"},
            "CpuOptions": {"CoreCount": 2, "ThreadsPerCore": 1},
            "CapacityReservationSpecification": {"CapacityReservationPreference": "open"},
            "HibernationOptions": {"Configured": False},
            "MetadataOptions": {
                "HttpTokens": "optional",
                "HttpPutResponseHopLimit": 1,
                "HttpEndpoint": "enabled",
                "HttpProtocolIpv6": "disabled",
                "InstanceMetadataTags": "disabled"},
            "EnclaveOptions": {"Enabled": False},
            "PrivateDnsNameOptions": {
                "HostnameType": "ip-name",
                "EnableResourceNameDnsARecord": True,
                "EnableResourceNameDnsAAAARecord": False},
            "MaintenanceOptions": {"AutoRecovery": "default"},
            "DisableApiStop": False
        }
        return launch_template_data

    # # ----------------------------------------------------
    # @classmethod
    # def get_user_data(
    #         self, user_data_filename, script_filename=None,
    #         token_to_replace=USER_DATA_TOKEN):
    #     """Return the EC2 user_data script as a Base64 encoded string.
    #
    #     Args:
    #         user_data_filename: Filename containing the user-data script to be executed on
    #             EC2 instantiation.
    #         script_filename: Filename containing a python script to be written to a file on
    #             the EC2 instantiation.
    #         token_to_replace: string within the user_data_filename which will be replaced
    #             by the text in the script filename.
    #
    #     Returns:
    #         A Base64-encoded string of the user_data file to create on an EC2 instance.
    #     """
    #     # Insert an external script if provided
    #     if script_filename is not None:
    #         self.fill_user_data_script(
    #             user_data_filename, script_filename, token_to_replace)
    #     try:
    #         with open(user_data_filename, "r") as infile:
    #             script_text = infile.read()
    #     except Exception:
    #         return None
    #     else:
    #         text_bytes = script_text.encode("ascii")
    #         text_base64_bytes = base64.b64encode(text_bytes)
    #         base64_script_text = text_base64_bytes.decode("ascii")
    #         return base64_script_text

    # ----------------------------------------------------
    @classmethod
    def fill_user_data_script(
            self, user_data_filename, script_filename, token_to_replace):
        """Fill the EC2 user_data script with a python script in another file.

        Args:
            user_data_filename: Filename containing the user-data script to be executed on
                EC2 instantiation.
            script_filename: Filename containing a python script to be written to a file on
                the EC2 instantiation.
            token_to_replace: string within the user_data_filename which will be replaced
                by the text in the script filename.

        Postcondition:
            The user_data file contains the text of the script file.
        """
        # Safely read the input filename using 'with'
        with open(user_data_filename) as f:
            s = f.read()
            if token_to_replace not in s:
                print(f"{token_to_replace} not found in {user_data_filename}.")
                return

        with open(script_filename) as sf:
            script = sf.read()

        # Safely write the changed content, if found in the file
        with open(user_data_filename, "w") as uf:
            print(
                f"Changing {token_to_replace} in {user_data_filename} to contents in "
                f"{script_filename}")
            s = s.replace(token_to_replace, script)
            uf.write(s)

    # ----------------------------------------------------
    @classmethod
    def create_token(self, type=None):
        """Create a token to name and identify an AWS resource.

        Args:
            type (str): optional descriptor to include in the token string.

        Returns:
            token(str): token for AWS resource identification.
        """
        if type is None:
            type = "token"
        token = f"{type}_{DT.datetime.now().timestamp()}"
        return token

    # ----------------------------------------------------
    @classmethod
    def create_spot_launch_template(
        self, ec2_client, template_name, user_data_filename, description=None,
            insert_script_filename=None, overwrite=False):
        """Create an EC2 Spot Instance Launch template on AWS.

        Args:
            ec2_client: an object for communicating with EC2.
            template_name: name for the launch template0
            user_data_filename: script to be installed and run on EC2 instantiation.
            description: user-supplied name defining the template.
            insert_script_filename: optional script to be inserted into user_data_filename.
            overwrite: flag indicating whether to use an existing template with this name,
                or create a new

        Returns:
            success: boolean flag indicating the success of creating launch template.
        """
        success = False
        if overwrite is True:
            self.delete_launch_template(template_name)
        template = self.get_launch_template(template_name)
        if template is not None:
            success = True
        else:
            spot_template_data = self.define_spot_launch_template_data(
                template_name, user_data_filename, insert_script_filename)
            template_token = self.create_token("template")
            try:
                response = ec2_client.create_launch_template(
                    DryRun=False,
                    ClientToken=template_token,
                    LaunchTemplateName=template_name,
                    VersionDescription=description,
                    LaunchTemplateData=spot_template_data
                )
            except ClientError as e:
                print(f"Failed to create launch template {template_name}, ({e})")
            else:
                success = (response["ResponseMetadata"]["HTTPStatusCode"] == 200)
        return success

    # ----------------------------------------------------
    @classmethod
    def get_instance(self, instance_id, region=REGION):
        """Describe an EC2 instance with instance_id.

        Args:
            instance_id: EC2 instance identifier.
            region: AWS region to query.

        Returns:
            instance: metadata for the EC2 instance
        """
        response = self.client.describe_instances(
            InstanceIds=[instance_id],
            DryRun=False,
        )
        try:
            instance = response["Reservations"][0]["Instances"][0]
        except Exception:
            instance = None
        return instance

    # ----------------------------------------------------
    @classmethod
    def run_instance_spot(self, ec2_client, template_name):
        """Run an EC2 Spot Instance on AWS.

        Args:
            ec2_client: an object for communicating with EC2.
            template_name: name for the launch template to be used for instantiation.

        Returns:
            instance_id: unique identifier of the new Spot instance.
        """
        instance_id = None
        token = self.create_token()
        instance_name = self.create_token(type="ec2")
        try:
            response = ec2_client.run_instances(
                # KeyName=key_name,
                ClientToken=token,
                MinCount=1, MaxCount=1,
                LaunchTemplate={"LaunchTemplateName": template_name, "Version": "1"},
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": instance_name},
                            {"Key": "TemplateName", "Value": template_name}
                        ]
                    }
                ]
            )
        except ClientError as e:
            print(f"Failed to instantiate Spot instance {instance_name}, ({e})")
        else:
            try:
                instance = response["Instances"][0]
            except KeyError:
                print("No instance returned")
            else:
                instance_id = instance["InstanceId"]
        return instance_id

    # ----------------------------------------------------
    @classmethod
    def get_launch_template_from_instance(self, instance_id):
        """Get or create an EC2 launch template from an EC2 instance identifier.

        Args:
            instance_id: identifier for an EC2 instance to use as a template.

        Returns:
            launch_template_data: metadata to be used as an EC2 launch template.
        """
        launch_template_data = self.client.get_launch_template_data(InstanceId=instance_id)
        return launch_template_data

    # ----------------------------------------------------
    @classmethod
    def get_launch_template(self, template_name):
        lnch_temp = None
        try:
            response = self.client.describe_launch_templates(
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
    def delete_instance(self, instance_id):
        """Delete an EC2 instance.

        Args:
            instance_id: identifier for an EC2 instance to delete.

        Returns:
            response: response from the server.
        """
        response = self.client.delete_instance(InstanceId=instance_id)
        return response


    # --------------------------------------------------------------------------------------
    def find_instances(self, key_name, launch_template_name):
        """Describe all instances with given key_name and/or launch_template_id.

        Args:
            key_name: optional key_name assigned to an instance.
            launch_template_name: name assigned to the template which created the instance

        Returns:
            instances: list of metadata for instances
        """
        filters = []
        if launch_template_name is not None:
            filters.append({"Name": "tag:TemplateName", "Values": [launch_template_name]})
        if key_name is not None:
            filters.append({"Name": "key-name", "Values": [key_name]})
        response = self.client.describe_instances(
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
                self.print_inst_info(res)
                instances.extend(res["Instances"])
        return instances

# --------------------------------------------------------------------------------------
# Methods for moving data to and from S3 buckets
# --------------------------------------------------------------------------------------
# .............................................................................
class s3:
    """Class for interacting with S3."""
    # ----------------------------------------------------
    def __init__(self, region=REGION):
        self.client = boto3.client("s3", region=region)

    # ----------------------------------------------------
    def upload_trigger_to_s3(
            self, trigger_name, s3_bucket, s3_bucket_path, region=REGION):
        """Upload a file to S3 which will trigger a workflow.

        Args:
            trigger_name: Name of workflow to trigger.
            s3_bucket: name of the S3 bucket destination.
            s3_bucket_path: the data destination inside the S3 bucket (without filename).
            region: AWS region to query.

        Returns:
            s3_filename: the URI to the file in the S3 bucket.
        """
        filename = f"{trigger_name}.txt"
        with open(filename, "r") as f:
            f.write("go!")
        s3_client = boto3.client("s3", region_name=region)
        obj_name = f"{s3_bucket_path}/{filename}"
        try:
            s3_client.upload_file(filename, s3_bucket, obj_name)
        except ClientError as e:
            print(f"Failed to upload {obj_name} to {s3_bucket}, ({e})")
        else:
            s3_filename = f"s3://{s3_bucket}/{obj_name}"
            print(f"Successfully uploaded {filename} to {s3_filename}")
        return s3_filename

    # ----------------------------------------------------
    def write_dataframe_to_s3_parquet(self, df, bucket, parquet_path, region=REGION):
        """Convert DataFrame to Parquet format and upload to S3.

        Args:
            df: pandas DataFrame containing data.
            bucket: name of the S3 bucket destination.
            parquet_path: the data destination inside the S3 bucket
            region: AWS region to query.
        """
        s3_client = boto3.client("s3", region_name=region)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")
        parquet_buffer.seek(0)
        s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)

    # ----------------------------------------------------
    def upload_to_s3(self, local_filename, bucket, s3_path, region=REGION):
        """Upload a file to S3.

        Args:
            local_filename: Full path to local file for upload.
            bucket: name of the S3 bucket destination.
            s3_path: the data destination inside the S3 bucket (without filename).
            region: AWS region to query.
        """
        s3_client = boto3.client("ec2", region_name=region)
        filename = os.path.split(local_filename)[1]
        s3_client.upload_file(local_filename, bucket, s3_path)
        print(f"Successfully uploaded {filename} to s3://{bucket}/{s3_path}")

    # ----------------------------------------------------
    def download_from_s3(
            self, bucket, bucket_path, filename, local_path, region=REGION, logger=None,
            overwrite=True):
        """Download a file from S3 to a local file.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Folder path to the S3 parquet data.
            filename (str): Filename of data to read from S3.
            local_path (str): local path for download.
            region (str): AWS region to query.
            logger (object): logger for saving relevant processing messages
            overwrite (boolean):  flag indicating whether to overwrite an existing file.

        Returns:
            local_filename (str): full path to local filename containing downloaded data.

        Raises:
            Exception: on failure with SSL error to download from S3
            Exception: on failure with AWS error to download from S3
            Exception: on failure to save file locally
        """
        local_filename = os.path.join(local_path, filename)
        obj_name = f"{bucket_path}/{filename}"
        # Delete if needed
        if os.path.exists(local_filename):
            if overwrite is True:
                os.remove(local_filename)
            else:
                logit(logger, f"{local_filename} already exists")
        # Download current
        if not os.path.exists(local_filename):
            s3_client = boto3.client("s3", region_name=region)
            try:
                s3_client.download_file(bucket, obj_name, local_filename)
            except SSLError:
                raise Exception(
                    f"Failed with SSLError to download s3://{bucket}/{obj_name}")
            except ClientError as e:
                raise Exception(
                    f"Failed with ClientError to download s3://{bucket}/{obj_name}, "
                    f"({e})")
            except Exception as e:
                raise Exception(
                    f"Failed with unknown Exception to download s3://{bucket}/{obj_name}, "
                    f"({e})")
            else:
                # Do not return until download to complete, allow max 5 min
                count = 0
                while not os.path.exists(local_filename) and count < 10:
                    sleep(seconds=30)
                if not os.path.exists(local_filename):
                    raise Exception(f"Failed to download from S3 to {local_filename}")
                else:
                    logit(logger, f"Downloaded from S3 to {local_filename}")

        return local_filename
