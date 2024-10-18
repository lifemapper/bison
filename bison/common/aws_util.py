"""Tools to use either locally or on EC2 to initiate BISON AWS EC2 Instances."""
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
import base64
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, SSLError
from csv import QUOTE_NONE
from datetime import datetime, timezone
from io import BytesIO
import os
import pandas
import subprocess as sp
from time import sleep

from bison.common.constants import AWS_METADATA_URL, REGION

six_hours = 21600


# .............................................................................
class _AWS:
    """Class for working with AWS tools."""

    # ----------------------------------------------------
    @classmethod
    def _get_instance_metadata_token(cls):
        token_prefix = "api/token"
        ttl_key = "X-aws-ec2-metadata-token-ttl-seconds"
        token_cmd = \
            f'curl -X PUT "{AWS_METADATA_URL}{token_prefix}" -H "{ttl_key}: {six_hours}"'
        token, err = sp.Popen(
            token_cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
        return token

    # ----------------------------------------------------
    @classmethod
    def _get_creds(cls, token, ec2_role):
        cred_prefix = "meta-data/iam/security-credentials/"
        token_key = "X-aws-ec2-metadata-token"
        cred_cmd = \
            f'curl -H "{token_key}": {token}" {AWS_METADATA_URL}{cred_prefix}{ec2_role}'
        creds, err = sp.Popen(
            cred_cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE).communicate()
        return creds

    # # ----------------------------------------------------
    # def _set_credentials(self):
    #     self._token = _AWS._get_instance_metadata_token()
    #     creds = _AWS._get_creds(self.token, self._role)
    #     self._expiration = self._creds["Expiration"]
    #     self._access_key_id = creds["AccessKeyId"]
    #     self._secret_access_key = creds["SecretAccessKey"]
    #     self._access_token = creds["Token"]
    #
    # # ----------------------------------------------------
    # def _check_expiration(self):
    #     n = datetime.now(timezone.utc)
    #     remaining_seconds = self._expiration - n
    #     if remaining_seconds < 300:
    #         # Reinitialize the client
    #         self._set_credentials()
    #
    # # ----------------------------------------------------
    # @classmethod
    # def _get_secret(cls, secret_name, region=REGION):
    #     """Get a secret from the Secrets Manager for connection authentication.
    #
    #     Args:
    #         secret_name: name of the secret to retrieve.
    #         region: AWS region for the secret.
    #
    #     Returns:
    #         a dictionary containing the secret data.
    #
    #     Raises:
    #         ClientError:  an AWS error in communication.
    #
    #     Note: For a list of exceptions thrown, see
    #     https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    #     """
    #     # Create a Secrets Manager client
    #     tmp_session = boto3.session.Session()
    #     client = tmp_session.client(service_name="secretsmanager", region_name=region)
    #     try:
    #         secret_value_response = client.get_secret_value(SecretId=secret_name)
    #     except ClientError:
    #         raise
    #     # Decrypt secret using the associated KMS key.
    #     secret_str = secret_value_response["SecretString"]
    #     return eval(secret_str)
    #
    # # ----------------------------------------------------
    # @classmethod
    # def _assume_role(cls, profile, role, region):
    #     session = boto3.session.Session(profile_name=profile)
    #     sts = session.client("sts", region_name=region)
    #     try:
    #         response = sts.assume_role(
    #             RoleArn=role, RoleSessionName="authenticated-bison-session")
    #     except NoCredentialsError:
    #         secret = cls._get_secret(WORKFLOW_SECRET_NAME, region=region)
    #         sts = session.client(
    #             "sts",
    #             aws_access_key_id=secret["aws_access_key_id"],
    #             aws_secret_access_key=secret["aws_secret_access_key"],
    #             region_name=region
    #         )
    #         try:
    #             response = sts.assume_role(
    #                 RoleArn=role, RoleSessionName="authenticated-bison-session")
    #         except Exception:
    #             raise
    #      return response
    #
    # # ----------------------------------------------------
    # @classmethod
    # def get_authenticated_session(cls, profile, role, region=REGION):
    #     """Get an authenticated session for AWS clients.
    #
    #     Args:
    #         profile: local user profile with assume_role privilege on role
    #         role: role with permissions for AWS operations.
    #         region: AWS region for the session.
    #
    #     Returns:
    #         auth_session (boto3.session.Session): an authenticated session.
    #         expiration (datetime.datetime): expiration time
    #     """
    #     response = cls._assume_role(profile, role, region)
    #     creds = response["Credentials"]
    #     expiration = creds["Expiration"]
    #
    #     # Initiate new authenticated session with credentials
    #     auth_session = boto3.session.Session(
    #         aws_access_key_id=creds["AccessKeyId"],
    #         aws_secret_access_key=creds["SecretAccessKey"],
    #         aws_session_token=creds["SessionToken"],
    #         profile_name=profile,
    #         region_name=region
    #     )
    #     return auth_session, expiration


# --------------------------------------------------------------------------------------
# Methods for constructing and instantiating EC2 instances
# --------------------------------------------------------------------------------------
# .............................................................................
class EC2:
    """Class for creating and manipulating ec2 instances."""
    TEMPLATE_BASENAME = "launch_template"

    # ----------------------------------------------------
    def __init__(self, region=REGION):
        """Constructor for common ec2 operations.

        Args:
            region: AWS region for the session.
        """
        self._region = region
        self._auth_session = boto3.session.Session(region_name=region)
        self._client = self._auth_session.client("s3")

    # ----------------------------------------------------
    def create_spot_launch_template_name(self, proj_prefix, desc_str=None):
        """Create a name identifier for a Spot Launch Template.

        Args:
            proj_prefix (str): prefix for template name.
            desc_str (str): optional descriptor to include in the name.

        Returns:
            template_name (str): name for identifying this Spot Launch Template.
        """
        if desc_str is None:
            template_name = f"{proj_prefix}_{self.TEMPLATE_BASENAME}"
        else:
            template_name = f"{proj_prefix}_{desc_str}_{self.TEMPLATE_BASENAME}"
        return template_name

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
            cls, user_data_filename, script_filename, token_to_replace):
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

        # with open(script_filename) as sf:
        #     script = sf.read()
        base64_script_text = None
        try:
            with open(script_filename, "r") as infile:
                script_text = infile.read()
        except Exception:
            pass
        else:
            text_bytes = script_text.encode("ascii")
            text_base64_bytes = base64.b64encode(text_bytes)
            base64_script_text = text_base64_bytes.decode("ascii")

        # Safely write the changed content, if found in the file
        with open(user_data_filename, "w") as uf:
            print(
                f"Changing {token_to_replace} in {user_data_filename} to contents in "
                f"{script_filename}")
            s = s.replace(token_to_replace, base64_script_text)
            uf.write(s)

    # ----------------------------------------------------
    def create_token(self, ec2_type=None):
        """Create a token to name and identify an AWS resource.

        Args:
            ec2_type (str): optional descriptor to include in the token string.

        Returns:
            token(str): token for AWS resource identification.
        """
        if ec2_type is None:
            ec2_type = "token"
        token = f"{ec2_type}_{datetime.now(timezone.utc).timestamp()}"
        return token

    # ----------------------------------------------------
    def get_instance(self, instance_id):
        """Describe an EC2 instance with instance_id.

        Args:
            instance_id: EC2 instance identifier.

        Returns:
            instance: metadata for the EC2 instance
        """
        response = self._client.describe_instances(
            InstanceIds=[instance_id],
            DryRun=False,
        )
        try:
            instance = response["Reservations"][0]["Instances"][0]
        except Exception:
            instance = None
        return instance

    # ----------------------------------------------------
    def run_instance(self, template_name):
        """Run an EC2 Instance on AWS.

        Args:
            template_name: name for the launch template to be used for instantiation.

        Returns:
            instance_id: unique identifier of the new instance.

        Raises:
            NoCredentialsError: on failure to authenticate and run instance.
            ClientError: on failure to run instance.
        """
        instance_id = None
        token = self.create_token()
        instance_name = self.create_token(ec2_type="ec2")
        try:
            response = self._client.run_instances(
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
        except NoCredentialsError:
            print(f"Failed to authenticate for run_instances")
            raise
        except ClientError:
            print(f"Failed to run instance {instance_name}")
            raise
        else:
            try:
                instance = response["Instances"][0]
            except KeyError:
                print("No instance returned")
            else:
                instance_id = instance["InstanceId"]
        return instance_id

    # ----------------------------------------------------
    def get_launch_template_from_instance(self, instance_id):
        """Get or create an EC2 launch template from an EC2 instance identifier.

        Args:
            instance_id: identifier for an EC2 instance to use as a template.

        Returns:
            launch_template_data: metadata to be used as an EC2 launch template.

        Raises:
            NoCredentialsError: on failure to authenticate to get_launch_template_data.
            Exception: on failure to get_launch_template_data for instance.
        """
        try:
            launch_template_data = self._client.get_launch_template_data(
                InstanceId=instance_id)
        except NoCredentialsError:
            print(f"Failed to authenticate for get_launch_template_data")
            raise
        except Exception:
            raise
        return launch_template_data

    # ----------------------------------------------------
    def get_launch_template(self, template_name):
        """Get or create an EC2 launch template from an EC2 instance identifier.

        Args:
            template_name: unique template name for a launch template.

        Returns:
            launch_template_data: metadata to be used as an EC2 launch template.

        Raises:
            NoCredentialsError: on failure to authenticate and get launch template data.
            Exception: on failure to find launch template for name.
            Exception: on failure to get launch template data from response.
        """
        try:
            response = self._client.describe_launch_templates(
                LaunchTemplateNames=[template_name])
        except NoCredentialsError:
            print(f"Failed to authenticate for describe_launch_templates")
            raise
        except Exception:
            raise
        # LaunchTemplateName is unique
        try:
            launch_template_data = response["LaunchTemplates"][0]
        except Exception:
            raise
        return launch_template_data

    # ----------------------------------------------------
    def delete_instance(self, instance_id):
        """Delete an EC2 instance.

        Args:
            instance_id: identifier for an EC2 instance to delete.

        Returns:
            response: response from the server.

        Raises:
            NoCredentialsError: on failure to authenticate and delete instance.
            Exception: on failure to delete EC2 instance.
        """
        try:
            response = self._client.delete_instance(InstanceId=instance_id)
        except NoCredentialsError:
            print(f"Failed to authenticate for delete_instance")
            raise
        except Exception:
            raise
        return response

    # ----------------------------------------------------
    def _print_inst_info(self, reservation):
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
            filters.append(
                {"Name": "tag:TemplateName", "Values": [launch_template_name]})
        if key_name is not None:
            filters.append({"Name": "key-name", "Values": [key_name]})
        response = self._client.describe_instances(
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
                self._print_inst_info(res)
                instances.extend(res["Instances"])
        return instances


# --------------------------------------------------------------------------------------
# Methods for moving data to and from S3 buckets
# --------------------------------------------------------------------------------------
# .............................................................................
class S3:
    """Class for interacting with S3."""
    # ----------------------------------------------------
    def __init__(self, region=REGION):
        """Constructor for common S3 operations.

        Args:
            region: AWS region for the session.
        """
        self._region = region
        self._auth_session = boto3.session.Session(region_name=region)
        self._client = self._auth_session.client("s3")

    # ----------------------------------------------------
    def get_dataframe_from_csv(
            self, bucket, bucket_path, filename, delimiter, encoding="utf-8",
            quoting=QUOTE_NONE):
        """Get or create an EC2 launch template from an EC2 instance identifier.

        Args:
            bucket: name for an S3 bucket.
            bucket_path: bucket path to data of interest.
            filename (str): Filename of CSV data to read from S3.
            delimiter (str): single character indicating the delimiter in the csv file.
            encoding (str): encoding for the csv file.
            quoting (int): one of csv enumerations: 'QUOTE_ALL', 'QUOTE_MINIMAL',
                'QUOTE_NONE', 'QUOTE_NONNUMERIC'

        Returns:
            df: pandas dataframe containing the tabular CSV data.

        Raises:
            NoCredentialsError: on failure to authenticate and run instance.
            SSLError: on failure to download from S3.
            ClientError: on failure with generic AWS error to download from S3.
            Exception: on failure with unknown error to download from S3.
        """
        # Read CSV file from S3 into a pandas DataFrame
        s3_key = f"{bucket_path}/{filename}"
        s3_uri = f"s3://{bucket}/{bucket_path}/{s3_key}"
        try:
            s3_obj = self._client.get_object(Bucket=bucket, Key=s3_key)
        except NoCredentialsError:
            print(f"Failed with to download {s3_uri}")
            raise
        except SSLError:
            print(f"Failed with to download {s3_uri}")
            raise
        except ClientError:
            print(f"Failed with to download {s3_uri}")
            raise
        except Exception:
            print(f"Failed with unhandled exception to download {s3_uri}")
            raise

        df = pandas.read_csv(
            s3_obj["Body"], delimiter=delimiter, encoding=encoding, low_memory=False,
            quoting=quoting)
        return df

    # ----------------------------------------------------
    def get_dataframe_from_parquet(self, bucket, bucket_path, filename, **args):
        """Read a parquet file from a folder on S3 into a pd DataFrame.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Folder path to the S3 parquet data.
            filename (str): Filename of parquet data to read from S3.
            args: Additional arguments to be sent to the pd.read_parquet function.

        Returns:
            pd.DataFrame containing the tabular data.

        Raises:
            NoCredentialsError: on failure to authenticate and download from S3.
            SSLError: on failure with SSL error to download from S3
            ClientError: on failure with AWS error to download from S3
            Exception: on failure to save file locally
        """
        s3_key = f"{bucket_path}/{filename}"
        s3_uri = f"s3://{bucket}/{bucket_path}/{s3_key}"
        try:
            obj = self._client.get_object(Bucket=bucket, Key=s3_key)
        except NoCredentialsError:
            print(f"Failed to authenticate for get_object {s3_uri}")
            raise
        except SSLError:
            print(f"Failed with SSLError to download {s3_uri}")
            raise
        except ClientError:
            print(f"Failed with ClientError to download {s3_uri}")
            raise
        except Exception:
            print(f"Failed with unknown Exception to download {s3_uri}")
            raise

        print(f"Read {bucket}/{s3_key} from S3")
        dataframe = pandas.read_parquet(BytesIO(obj["Body"].read()), **args)
        return dataframe

    # .............................................................................
    def get_dataframe_from_parquet_folder(self, bucket, bucket_path, **args):
        """Read multiple parquets from a folder on S3 into a pd DataFrame.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Parent folder path to the S3 parquet data.
            args: Additional arguments to be sent to the pd.read_parquet function.

        Returns:
            pd.DataFrame containing the tabular data.

        Raises:
            Exception: on failure to download any part of data from S3
        """
        if not bucket_path.endswith("/"):
            bucket_path = bucket_path + "/"
        s3_conn = self._auth_session.resource("s3")

        s3_keys = [
            item.key
            for item in s3_conn.Bucket(bucket).objects.filter(Prefix=bucket_path)
            if item.key.endswith(".parquet")]
        if not s3_keys:
            print(f"No parquet found in {bucket} {bucket_path}")
            return None

        try:
            dfs = [
                self.get_dataframe_from_parquet(bucket, bucket_path, key, **args)
                for key in s3_keys
            ]
        except Exception:
            raise

        return pandas.concat(dfs, ignore_index=True)

    # .............................................................................
    def write_dataframe_to_parquet(self, df, bucket, parquet_path):
        """Convert DataFrame to Parquet format and upload to S3.

        Args:
            df: pandas DataFrame containing data.
            bucket: name of the S3 bucket destination.
            parquet_path: the data destination inside the S3 bucket

        Returns:
            path to S3 data object

        Raises:
            NoCredentialsError: on failure to authenticate and upload to S3.
            Exception: on failure to upload file to S3.
        """
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")
        parquet_buffer.seek(0)
        try:
            self._client.upload_fileobj(parquet_buffer, bucket, parquet_path)
        except NoCredentialsError:
            print(f"Failed to authenticate for upload_fileobj {parquet_path}.")
            raise
        except Exception:
            print(f"Failed with unknown error to upload_fileobj {parquet_path}.")
            raise
        return f"s3//{bucket}/{parquet_path}"

    # ----------------------------------------------------
    def upload(self, local_filename, bucket, s3_path):
        """Upload a file to S3.

        Args:
            local_filename: Full path to local file for upload.
            bucket: name of the S3 bucket destination.
            s3_path: the data destination inside the S3 bucket (without filename).

        Returns:
            uploaded_fname: the S3 path to the uploaded file.

        Raises:
            NoCredentialsError: on failure to autheticate for upload_file.
            Exception: on failure to upload_file to S3.
        """
        filename = os.path.split(local_filename)[1]
        try:
            self._client.upload_file(local_filename, bucket, s3_path)
        except NoCredentialsError:
            print(f"Failed to authenticate for upload_file {local_filename}.")
            raise
        except Exception:
            print(f"Failed to upload_file {local_filename}.")
            raise
        else:
            uploaded_fname = f"s3://{bucket}/{s3_path}/{filename}"
        return uploaded_fname

    # ----------------------------------------------------
    def list(self, bucket, bucket_folder, prefix=None):
        """List object names/keys in a bucket.

        Args:
            bucket: bucket for query
            bucket_folder: Any subfolder to limit the query
            prefix: Any object prefix to limit the query

        Returns:
            objnames: Keys for the objects in the S3 location.

        Raises:
            NoCredentialsError: on failure to list objects from S3
            SSLError: on failure list objects from S3
            ClientError: on failure list objects from S3
            Exception: on unknown failure to list objects from S3
        """
        objnames = []
        # Ensure that directory prefix ends in /
        if bucket_folder != "" and not bucket_folder.endswith("/"):
            bucket_folder += "/"
        full_prefix = bucket_folder
        # Add object prefix if present
        if prefix is not None:
            full_prefix += prefix
        s3uri = f"s3://{bucket}/{full_prefix}"
        # Try to list
        try:
            response = self._client.list_objects_v2(Bucket=bucket, Prefix=full_prefix)
        except NoCredentialsError:
            print(f"Failed to authenticate for list_objects_v2 in {s3uri}")
            raise
        except SSLError:
            print(f"Failed with SSLError to list_objects_v2 in {s3uri}")
            raise
        except ClientError:
            print(f"Failed with ClientError to list_objects_v2 in {s3uri}")
            raise
        except Exception:
            print(f"Failed with unknown Exception to list_objects_v2 in {s3uri}")
            raise

        # Any objects?
        try:
            contents = response["Contents"]
        except KeyError:
            pass
        else:
            for s3obj in contents:
                name = s3obj["Key"]
                # Do not return the folder in results
                if name != bucket_folder:
                    objnames.append(s3obj["Key"])

        return objnames

    # ----------------------------------------------------
    def download(
            self, bucket, bucket_path, filename, local_path, overwrite=True):
        """Download a file from S3 to a local file.

        Args:
            bucket (str): Bucket identifier on S3.
            bucket_path (str): Folder path to the S3 parquet data.
            filename (str): Filename of data to read from S3.
            local_path (str): local path for download.
            overwrite (boolean):  flag indicating whether to overwrite an existing file.

        Returns:
            local_filename (str): full path to local filename containing downloaded data.

        Raises:
            NoCredentialsError: on failure to authenticate and download from S3.
            SSLError: on failure to download from S3
            ClientError: on failure with AWS error to download from S3
            Exception: on failure with unexpected error to download from S3
            Exception: on failure to save file locally
        """
        local_filename = os.path.join(local_path, filename)
        obj_name = f"{bucket_path}/{filename}"
        url = f"s3://{bucket}/{obj_name}"
        # Delete if needed
        if os.path.exists(local_filename):
            if overwrite is True:
                os.remove(local_filename)
            else:
                print(f"{local_filename} already exists")
        # Download current
        if not os.path.exists(local_filename):
            try:
                self._client.download_file(bucket, obj_name, local_filename)
            except NoCredentialsError:
                print("Failed to authenticate and download_file.")
                raise
            except SSLError:
                print("Failed with SSLError to download_file.")
                raise
            except ClientError:
                print("Failed with generic AWS error to download_file.")
                raise
            except Exception:
                print("Failed with unexpected Exception to download_file.")
                raise

            # Do not return until download to complete, allow max 5 min
            count = 0
            while not os.path.exists(local_filename) and count < 10:
                sleep(seconds=30)
            if not os.path.exists(local_filename):
                raise Exception(f"Failed to download {url} to {local_filename}")
            else:
                print(f"Downloaded {url} to {local_filename}")

        return local_filename
# ----------------------------------------------------
