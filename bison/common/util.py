"""Tools to use either locally or on EC2 to initiate BISON AWS EC2 Instances."""
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
import base64
import boto3
from botocore.exceptions import ClientError
import botocore.session as bc
import csv
import datetime as DT
import logging
from logging.handlers import RotatingFileHandler
import pandas
import os
import sys
import traceback

from bison.common.constants import (
    ENCODING, INSTANCE_TYPE, KEY_NAME, LOGFILE_MAX_BYTES, LOG_FORMAT, LOG_DATE_FORMAT,
    PROJ_NAME, REGION, SECURITY_GROUP_ID, SPOT_TEMPLATE_BASENAME, USER_DATA_TOKEN, PROJ_ROLE)


# --------------------------------------------------------------------------------------
# Methods for constructing and instantiating EC2 instances
# --------------------------------------------------------------------------------------
# ----------------------------------------------------
def get_secret(secret_name, region):
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
def create_spot_launch_template_name(desc_str=None):
    """Create a name identifier for a Spot Launch Template.

    Args:
        desc_str (str): optional descriptor to include in the name.

    Returns:
        template_name (str): name for identifying this Spot Launch Template.
    """
    if desc_str is None:
        template_name = f"{PROJ_NAME}_{SPOT_TEMPLATE_BASENAME}"
    else:
        template_name = f"{PROJ_NAME}_{desc_str}_{SPOT_TEMPLATE_BASENAME}"
    return template_name


# ----------------------------------------------------
def define_spot_launch_template_data(
        template_name, user_data_filename, script_filename,
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
    user_data_64 = get_user_data(
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


# ----------------------------------------------------
def get_user_data(
        user_data_filename, script_filename=None, token_to_replace=USER_DATA_TOKEN):
    """Return the EC2 user_data script as a Base64 encoded string.

    Args:
        user_data_filename: Filename containing the user-data script to be executed on
            EC2 instantiation.
        script_filename: Filename containing a python script to be written to a file on
            the EC2 instantiation.
        token_to_replace: string within the user_data_filename which will be replaced
            by the text in the script filename.

    Returns:
        A Base64-encoded string of the user_data file to create on an EC2 instance.
    """
    # Insert an external script if provided
    if script_filename is not None:
        fill_user_data_script(user_data_filename, script_filename, token_to_replace)
    try:
        with open(user_data_filename, "r") as infile:
            script_text = infile.read()
    except Exception:
        return None
    else:
        text_bytes = script_text.encode("ascii")
        text_base64_bytes = base64.b64encode(text_bytes)
        base64_script_text = text_base64_bytes.decode("ascii")
        return base64_script_text


# ----------------------------------------------------
def fill_user_data_script(
        user_data_filename, script_filename, token_to_replace):
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
def create_token(type=None):
    """Create a token to name and identify an AWS resource.

    Args:
        type (str): optional descriptor to include in the token string.

    Returns:
        token(str): token for AWS resource identification.
    """
    if type is None:
        type = PROJ_NAME
    token = f"{type}_{DT.datetime.now().timestamp()}"
    return token


# ----------------------------------------------------
def get_today_str():
    """Get a string representation of the current date.

    Returns:
        date_str(str): string representing date in YYYY-MM-DD format.
    """
    n = DT.datetime.now()
    date_str = f"{n.year}_{n.month:02d}_{n.day:02d}"
    return date_str


# ----------------------------------------------------
def get_current_datadate_str():
    """Get a string representation of the first day of the current month.

    Returns:
        date_str(str): string representing date in YYYY-MM-DD format.
    """
    n = DT.datetime.now()
    date_str = f"{n.year}_{n.month:02d}_01"
    # date_str = "2024_08_01"
    return date_str


# ----------------------------------------------------
def get_previous_datadate_str():
    """Get a string representation of the first day of the previous month.

    Returns:
        date_str(str): string representing date in YYYY-MM-DD format.
    """
    n = DT.datetime.now()
    yr = n.year
    mo = n.month - 1
    if n.month == 0:
        mo = 12
        yr -= 1
    date_str = f"{yr}_{mo:02d}_01"
    return date_str


# ----------------------------------------------------
def create_spot_launch_template(
        ec2_client, template_name, user_data_filename, insert_script_filename=None,
        overwrite=False):
    """Create an EC2 Spot Instance Launch template on AWS.

    Args:
        ec2_client: an object for communicating with EC2.
        template_name: name for the launch template0
        user_data_filename: script to be installed and run on EC2 instantiation.
        insert_script_filename: optional script to be inserted into user_data_filename.
        overwrite: flag indicating whether to use an existing template with this name,
            or create a new

    Returns:
        success: boolean flag indicating the success of creating launch template.
    """
    success = False
    if overwrite is True:
        delete_launch_template(template_name)
    template = get_launch_template(template_name)
    if template is not None:
        success = True
    else:
        spot_template_data = define_spot_launch_template_data(
            template_name, user_data_filename, insert_script_filename)
        template_token = create_token("template")
        try:
            response = ec2_client.create_launch_template(
                DryRun=False,
                ClientToken=template_token,
                LaunchTemplateName=template_name,
                VersionDescription="Spot for GBIF/BISON process",
                LaunchTemplateData=spot_template_data
            )
        except ClientError as e:
            print(f"Failed to create launch template {template_name}, ({e})")
        else:
            success = (response["ResponseMetadata"]["HTTPStatusCode"] == 200)
    return success


# ----------------------------------------------------
def upload_trigger_to_s3(trigger_name, s3_bucket, s3_bucket_path, region=REGION):
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


# # ----------------------------------------------------
# def write_dataframe_to_s3_parquet(df, bucket, parquet_path, region=REGION):
#     """Convert DataFrame to Parquet format and upload to S3.
#
#     Args:
#         df: pandas DataFrame containing data.
#         bucket: name of the S3 bucket destination.
#         parquet_path: the data destination inside the S3 bucket
#         region: AWS region to query.
#     """
#     s3_client = boto3.client("s3", region_name=region)
#     parquet_buffer = io.BytesIO()
#     df.to_parquet(parquet_buffer, engine="pyarrow")
#     parquet_buffer.seek(0)
#     s3_client.upload_fileobj(parquet_buffer, bucket, parquet_path)

def upload_to_s3(local_filename, bucket, s3_path, region=REGION):
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
def _get_authenticated_session(region):
    session = boto3.Session()
    sts = session.client("sts", region_name=region)
    response = sts.assume_role(
        RoleArn=PROJ_ROLE, RoleSessionName="authenticated-bison-session")
    creds = response['Credentials']
    new_session = boto3.Session(
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'])
    return new_session


# ----------------------------------------------------
def get_instance(instance_id, region=REGION):
    """Describe an EC2 instance with instance_id.

    Args:
        instance_id: EC2 instance identifier.
        region: AWS region to query.

    Returns:
        instance: metadata for the EC2 instance
    """
    ec2_client = boto3.client("ec2", region_name=region)
    response = ec2_client.describe_instances(
        InstanceIds=[instance_id],
        DryRun=False,
    )
    try:
        instance = response["Reservations"][0]["Instances"][0]
    except Exception:
        instance = None
    return instance


# ----------------------------------------------------
def run_instance_spot(ec2_client, template_name):
    """Run an EC2 Spot Instance on AWS.

    Args:
        ec2_client: an object for communicating with EC2.
        template_name: name for the launch template to be used for instantiation.

    Returns:
        instance_id: unique identifier of the new Spot instance.
    """
    instance_id = None
    spot_token = create_token(type="spot")
    instance_name = create_token()
    try:
        response = ec2_client.run_instances(
            # KeyName=key_name,
            ClientToken=spot_token,
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
        print(f"Failed to instantiate Spot instance {spot_token}, ({e})")
    else:
        try:
            instance = response["Instances"][0]
        except KeyError:
            print("No instance returned")
        else:
            instance_id = instance["InstanceId"]
    return instance_id


# --------------------------------------------------------------------------------------
# Tools for experimentation
# --------------------------------------------------------------------------------------

# ----------------------------------------------------upload_to_s3
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
def find_instances(key_name, launch_template_name):
    """Describe all EC2 instances with name or launch_template_id.

    Args:
        key_name: EC2 instance name
        launch_template_name: EC2 launch template name

    Returns:
        instances: list of metadata for EC2 instances
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


# ----------------------------------------------------
def get_launch_template_from_instance(instance_id, region=REGION):
    """Return a JSON formatted template from an existing EC2 instance.

    Args:
        instance_id: unique identifier for the selected EC2 instance.
        region: AWS region to query.

    Returns:
        launch_template_data: a JSON formatted launch template.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    launch_template_data = ec2_client.get_launch_template_data(InstanceId=instance_id)
    return launch_template_data


# --------------------------------------------------------------------------------------
# On local machine: Describe the launch_template with the template_name
def get_launch_template(template_name, region=REGION):
    """Return a JSON formatted template for a template_name.

    Args:
        template_name: unique name for the requested template.
        region: AWS region to query.

    Returns:
        launch_template_data: a JSON formatted launch template.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    lnch_temp = None
    # Find pre-existing template
    try:
        response = ec2_client.describe_launch_templates(
            LaunchTemplateNames=[template_name],
        )
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
def delete_launch_template(template_name, region=REGION):
    """Delete an EC2 launch template AWS.

    Args:
        template_name: name of the selected EC2 launch template.
        region: AWS region to query.

    Returns:
        response: a JSON formatted AWS response.
    """
    response = None
    ec2_client = boto3.client("ec2", region_name=region)
    lnch_tmpl = get_launch_template(template_name)
    if lnch_tmpl is not None:
        response = ec2_client.delete_launch_template(LaunchTemplateName=template_name)
    return response


# ----------------------------------------------------
def delete_instance(instance_id, region=REGION):
    """Delete an EC2 instance.

    Args:
        instance_id: unique identifier for the selected EC2 instance.
        region: AWS region to query.

    Returns:
        response: a JSON formatted AWS response.
    """
    ec2_client = boto3.client("ec2", region_name=region)
    response = ec2_client.delete_instance(InstanceId=instance_id)
    return response


# ----------------------------------------------------
def get_logger(log_name, log_dir=None, log_level=logging.INFO):
    """Get a logger for writing logging messages to file and console.

    Args:
        log_name: Name for the log object and output log file.
        log_dir: absolute path for the logfile.
        log_level: logging constant error level (logging.INFO, logging.DEBUG,
                logging.WARNING, logging.ERROR)

    Returns:
        logger: logging.Logger object
    """
    filename = f"{log_name}.log"
    if log_dir is not None:
        filename = os.path.join(log_dir, f"{filename}")
        os.makedirs(log_dir, exist_ok=True)
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


# ----------------------------------------------------
def create_dataframe_from_gbifcsv_s3_bucket(bucket, csv_path, region=REGION):
    """Read CSV data from S3 into a pandas DataFrame.

    Args:
        bucket: name of the bucket containing the CSV data.
        csv_path: the CSV object name with enclosing S3 bucket folders.
        region: AWS region to query.

    Returns:
        df: pandas DataFrame containing the CSV data.
    """
    s3_client = boto3.client("s3", region_name=region)
    s3_obj = s3_client.get_object(Bucket=bucket, Key=csv_path)
    df = pandas.read_csv(
        s3_obj["Body"], delimiter="\t", encoding="utf-8", low_memory=False,
        quoting=csv.QUOTE_NONE)
    return df


# ...............................................
def delete_file(file_name, delete_dir=False):
    """Delete file if it exists, optionally delete newly empty directory.

    Args:
        file_name (str): full path to the file to delete
        delete_dir (bool): flag - True to delete parent directory if it becomes empty

    Returns:
        True if file was not found, or file was successfully deleted.  If
            file deletion results in an empty parent directory, directory is also
            successfully deleted.
        False if failed to delete file (and parent directories).
    """
    success = True
    msg = ''
    if file_name is None:
        msg = "Cannot delete file 'None'"
    else:
        pth, _ = os.path.split(file_name)
        if file_name is not None and os.path.exists(file_name):
            try:
                os.remove(file_name)
            except Exception as e:
                success = False
                msg = 'Failed to remove {}, {}'.format(file_name, str(e))
            if delete_dir and len(os.listdir(pth)) == 0:
                try:
                    os.removedirs(pth)
                except Exception as e:
                    success = False
                    msg = 'Failed to remove {}, {}'.format(pth, str(e))
    return success, msg


# ...............................................
def ready_filename(fullfilename, overwrite=True):
    """Delete file if it exists, optionally delete newly empty directory.

    Args:
        fullfilename (str): full path of the file to check
        overwrite (bool): flag indicating to delete the file if it already exists

    Returns:
        boolean: True if file does not yet exist, or file was successfully deleted.  If
            file deletion results in an empty parent directory, directory is also
            successfully deleted.
        False if failed to delete file (and parent directories).

    Raises:
        PermissionError: if unable to delete existing file when overwrite is true
        Exception: on other delete errors or failure to create directories
        PermissionError: if unable to create missing directories
        Exception: on other mkdir errors
        Exception: on failure to create directories
    """
    is_ready = True
    if os.path.exists(fullfilename):
        if overwrite:
            try:
                delete_file(fullfilename)
            except PermissionError:
                raise
            except Exception as e:
                raise Exception('Unable to delete {} ({})'.format(fullfilename, e))
        else:
            is_ready = False
    else:
        pth, _ = os.path.split(fullfilename)
        try:
            os.makedirs(pth)
        except FileExistsError:
            pass
        except PermissionError:
            raise
        except Exception:
            raise

        if not os.path.isdir(pth):
            raise Exception('Failed to create directories {}'.format(pth))

    return is_ready


# .............................................................................
def get_csv_dict_writer(
        csvfile, header, delimiter, fmode="w", encoding=ENCODING, extrasaction="ignore",
        overwrite=True):
    """Create a CSV dictionary writer and write the header.

    Args:
        csvfile (str): output CSV filename for writing
        header (list): header for output file
        delimiter (str): field separator
        fmode (str): Write ('w') or append ('a')
        encoding (str): Encoding for output file
        extrasaction (str): Action to take if there are fields in a record dictionary
            not present in fieldnames
        overwrite (bool): True to delete an existing file before write

    Returns:
        writer (csv.DictWriter) ready to write
        f (file handle)

    Raises:
        Exception: on invalid file mode
        Exception: on failure to create a DictWriter
        FileExistsError: on existing file if overwrite is False
    """
    if fmode not in ("w", "a"):
        raise Exception("File mode must be 'w' (write) or 'a' (append)")
    if ready_filename(csvfile, overwrite=overwrite):
        csv.field_size_limit(sys.maxsize)
        try:
            f = open(csvfile, fmode, newline="", encoding=encoding)
        except Exception as e:
            raise e
        else:
            writer = csv.DictWriter(
                f, fieldnames=header, delimiter=delimiter, extrasaction=extrasaction)
            writer.writeheader()
        return writer, f
    else:
        raise FileExistsError


# .............................................................................
def get_csv_dict_reader(
        csvfile, delimiter, fieldnames=None, encoding=ENCODING, quote_none=False,
        restkey="rest"):
    """Create a CSV dictionary reader from a file with a fieldname header.

    Args:
        csvfile (str): output CSV file for reading
        delimiter (char): delimiter between fields
        fieldnames (list): strings with corrected fieldnames, cleaned of illegal
            characters, for use with records.
        encoding (str): type of encoding
        quote_none (bool): True opens csvfile with QUOTE_NONE, False opens with
            QUOTE_MINIMAL
        restkey (str): fieldname for extra fields in a record not present in header

    Returns:
        rdr (csv.DictReader): DictReader ready to read
        f (object): open file handle

    Raises:
        FileNotFoundError: on missing csvfile
        PermissionError: on improper permissions on csvfile
    """
    csv.field_size_limit(sys.maxsize)

    if quote_none is True:
        quoting = csv.QUOTE_NONE
    else:
        quoting = csv.QUOTE_MINIMAL

    try:
        #  If csvfile is a file object, it should be opened with newline=""
        f = open(csvfile, "r", newline="", encoding=encoding)
    except FileNotFoundError:
        raise
    except PermissionError:
        raise

    if fieldnames is not None:
        rdr = csv.DictReader(
            f, fieldnames=fieldnames, quoting=quoting, delimiter=delimiter,
            restkey=restkey)
    else:
        rdr = csv.DictReader(f, quoting=quoting, delimiter=delimiter, restkey=restkey)

    return rdr, f


# ..........................
def get_traceback():
    """Get the traceback for this exception.

    Returns:
        trcbk: traceback of steps executed before an exception
    """
    exc_type, exc_val, this_traceback = sys.exc_info()
    tb = traceback.format_exception(exc_type, exc_val, this_traceback)
    tblines = []
    cr = "\n"
    for line in tb:
        line = line.rstrip(cr)
        parts = line.split(cr)
        tblines.extend(parts)
    trcbk = cr.join(tblines)
    return trcbk


# ...............................................
def combine_errinfo(errinfo1, errinfo2):
    """Combine 2 dictionaries with keys `error`, `warning` and `info`.

    Args:
        errinfo1: dictionary of errors
        errinfo2: dictionary of errors

    Returns:
        dictionary of errors
    """
    errinfo = {}
    for key in ("error", "warning", "info"):
        try:
            lst = errinfo1[key]
        except KeyError:
            lst = []
        try:
            lst2 = errinfo2[key]
        except KeyError:
            lst2 = []

        if lst or lst2:
            lst.extend(lst2)
            errinfo[key] = lst
    return errinfo


# ...............................................
def add_errinfo(errinfo, key, val_lst):
    """Add to a dictionary with keys `error`, `warning` and `info`.

    Args:
        errinfo: dictionary of errors
        key: error type, `error`, `warning` or `info`
        val_lst: error message or list of errors

    Returns:
        updated dictionary of errors
    """
    if errinfo is None:
        errinfo = {}
    if key in ("error", "warning", "info"):
        if isinstance(val_lst, str):
            val_lst = [val_lst]
        try:
            errinfo[key].extend(val_lst)
        except KeyError:
            errinfo[key] = val_lst
    return errinfo


# ......................................................
def prettify_object(print_obj):
    """Format an object for output.

    Args:
        print_obj (obj): Object to pretty print in output

    Returns:
        formatted string representation of object

    Note: this splits a string containing spaces in a list to multiple strings in the
        list.
    """
    # Used only in local debugging
    from io import StringIO
    from pprint import pp

    strm = StringIO()
    pp(print_obj, stream=strm)
    obj_str = strm.getvalue()
    return obj_str


# .............................................................................
class BisonNameOp():
    """Class for constructing filenames following a pattern for different processes."""
    separator = "_"

    # ----------------------------------------------------
    @staticmethod
    def get_annotated_riis_filename(input_riis_filename):
        """Construct a filename for current annotated version of the USGS RIIS records.

        Args:
            input_riis_filename (str): full filename of the original RIIS data.

        Returns:
            out_filename: full filename for the output file.
        """
        pth, fname = os.path.split(input_riis_filename)
        basename, _ = os.path.splitext(fname)
        datestr = get_current_datadate_str()
        out_filename = os.path.join(pth, f"{basename}_annotated_{datestr}.csv")
        return out_filename

    # ...............................................
    @staticmethod
    def get_grid_filename(basename, resolution, outpath):
        """Construct a filename for the summarized version of csvfile.

        Args:
            basename (str): basefilename for output grid shapefile.
            resolution (float): cellsize for output grid.
            outpath (str): destination directory for output grid

        Returns:
            grid_fname: output filename for the gridded shapefile.
        """
        grid_fname = os.path.join(outpath, f"grid_{basename}_{resolution}.shp")
        return grid_fname
