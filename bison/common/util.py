"""Tools to use either locally or on EC2 to initiate BISON AWS EC2 Instances."""
# --------------------------------------------------------------------------------------
# Imports
# --------------------------------------------------------------------------------------
import boto3
from botocore.exceptions import ClientError
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
    PROJ_NAME, REGION, SECURITY_GROUP_ID, SPOT_TEMPLATE_BASENAME, USER_DATA_TOKEN,
    WORKFLOW_ROLE
)


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



