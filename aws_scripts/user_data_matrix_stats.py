"""Script to install on an EC2 instance to compute matrix statistics on S3 data."""
import boto3
from botocore.exceptions import ClientError
import datetime as DT
import io
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas
import sys
# import pyarrow.parquet as pq
# import s3fs


DOWNLOAD_NAME = "0098682-230530130749713"
BUCKET = "bison-321942852011-us-east-1"
BUCKET_PATH = "out_data"
LOG_PATH = "log"

n = DT.datetime.now()
datastr = f"{n.year}-{n.month}-01"

species_county_list_basename = "county_lists_000"
species_county_list_fname = f"{species_county_list_basename}.parquet"
# Log processing progress
LOGINTERVAL = 1000000
LOG_FORMAT = " ".join(["%(asctime)s", "%(levelname)-8s", "%(message)s"])
LOG_DATE_FORMAT = '%d %b %Y %H:%M'
LOGFILE_MAX_BYTES = 52000000
LOGFILE_BACKUP_COUNT = 5

bison_bucket = "s3://bison-321942852011-us-east-1/"
output_dataname = "heatmatrix.parquet"


# ----------------------------------------------------
def get_logger(log_name, log_dir=None, log_level=logging.INFO):
    """Get a logger for saving messages to disk.

    Args:
        log_name: name of the logger and logfile
        log_dir: path for the output logfile.
        log_level: Minimum level for which to log messages

    Returns:
        logger (logging.Logger): logger instance.
        filename (str): full path for output logfile.
    """
    filename = f"{log_name}.log"
    if log_dir is not None:
        filename = os.path.join(log_dir, f"{filename}")
        os.makedirs(log_dir, exist_ok=True)
    # create file handler
    handlers = []
    # for debugging in place
    handlers.append(logging.StreamHandler(stream=sys.stdout))
    # for saving onto S3
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
    return logger, filename


# ----------------------------------------------------
def download_from_s3(bucket, bucket_path, filename, logger, overwrite=True):
    """Download a file from S3 to a local file.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 parquet data.
        filename (str): Filename of parquet data to read from S3.
        logger (object): logger for saving relevant processing messages
        overwrite (boolean):  flag indicating whether to overwrite an existing file.

    Returns:
        local_filename (str): full path to local filename containing downloaded data.
    """
    local_path = os.getcwd()
    local_filename = os.path.join(local_path, filename)
    if os.path.exists(local_filename):
        if overwrite is True:
            os.remove(local_filename)
        else:
            logger.log(logging.INFO, f"{local_filename} already exists")
    else:
        s3_client = boto3.client("s3")
        try:
            s3_client.download_file(bucket, f"{bucket_path}/{filename}", local_filename)
        except ClientError as e:
            logger.log(
                logging.ERROR,
                f"Failed to download {filename} from {bucket}/{bucket_path}, ({e})")
        else:
            logger.log(
                logging.INFO, f"Downloaded {filename} from S3 to {local_filename}")
    return local_filename


# # ----------------------------------------------------
# def read_s3_parquet_to_pandas_alt(bucket, bucket_path, filename, logger, overwrite=True):
#     s3 = s3fs.S3FileSystem()
#     s3_filename = f"s3://{bucket}/{bucket_path}/{filename}"
#     pandas_df = (pq.ParquetDataset(
#         s3_filename, filesystem=s3).read_pandas().to_pandas())
#     return pandas_df


# ----------------------------------------------------
def read_s3_parquet_to_pandas(
        bucket, bucket_path, filename, logger, s3_client=None, **args):
    """ Read a parquet file from a folder on S3 into a pandas DataFrame.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Folder path to the S3 parquet data.
        filename (str): Filename of parquet data to read from S3.
        logger (object): logger for saving relevant processing messages
        s3_client (object): object for interacting with Amazon S3.
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        pandas.DataFrame containing the tabular data.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    s3_key = f"{bucket_path}/{filename}"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
    except ClientError as e:
        logger.log(logging.ERROR, f"Failed to get {bucket}/{s3_key} from S3, ({e})")
    else:
        logger.log(logging.INFO, f"Read {bucket}/{s3_key} from S3")

    dataframe = pandas.read_parquet(io.BytesIO(obj['Body'].read()), **args)
    return dataframe

# ----------------------------------------------------
def read_s3_multiple_parquets_to_pandas(
        bucket, bucket_path, logger, s3=None, s3_client=None, verbose=False, **args):
    """ Read multiple parquets from a folder on S3 into a pandas DataFrame.

    Args:
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Parent folder path to the S3 parquet data.
        logger (object): logger for saving relevant processing messages
        s3 (object): Connection to the S3 resource
        s3_client (object): object for interacting with Amazon S3.
        verbose (boolean): flag indicating whether to log verbose messages
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        pandas.DataFrame containing the tabular data.
    """
    if not bucket_path.endswith("/"):
        bucket_path = bucket_path + "/"
    if s3_client is None:
        s3_client = boto3.client("s3")
    if s3 is None:
        s3 = boto3.resource("s3")
    s3_keys = [
        item.key for item in s3.Bucket(bucket).objects.filter(Prefix=bucket_path)
        if item.key.endswith(".parquet")]
    if not s3_keys:
        logger.log(logging.ERROR, f"No parquet found in {bucket} {bucket_path}")
    elif verbose:
        logger.log(logging.INFO, "Load parquets:")
        for p in s3_keys:
            logger.log(logging.INFO, f"   {p}")
    dfs = [read_s3_parquet_to_pandas(key, bucket=bucket, s3_client=s3_client, **args)
           for key in s3_keys]
    return pandas.concat(dfs, ignore_index=True)

# ----------------------------------------------------
def read_species_to_dict(orig_df):
    """Create a dictionary of species keys, with values containing counts for counties.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            census_state, census_county. taxonkey, species, riis_assessment, occ_count

    Returns:
        dictionary of {species: [county: count, county: count, ...]}
    """
    # for each county, create a row for species counts
    county_species_counts = {}
    for col, row in orig_df.iterrows():
        sp = row["species"]
        cty = row["state_county"]
        total = row["occ_count"]
        try:
            county_species_counts[cty].append((sp, total))
        except KeyError:
            county_species_counts[cty] = [(sp, total)]
    return county_species_counts

# ----------------------------------------------------
def reframe_to_heatmatrix(orig_df, logger):
    """Create a dataframe of species columns by county rows from county species lists.

    Args:
        orig_df (pandas.DataFrame): DataFrame of records containing columns:
            census_state, census_county. taxonkey, species, riis_assessment, occ_count
        logger (object): logger for saving relevant processing messages

    Returns:
        heat_df (Pandas.DataFrame): DF of species (columnns, x axis=1) by counties
            (rows, y axis=0, sites), with values = number of occurrences.
    """
    # make sure indexes pair with number of rows
    orig_df = orig_df.reset_index()
    orig_df["state_county"] = df["census_state"] + "_" + orig_df["census_county"]

    # Create dataframe of zeros with rows=sites and columns=species
    counties = orig_df.state_county.unique()
    species = orig_df.species.unique()
    heat_df = pandas.DataFrame(0, index=counties, columns=species)

    # Fill dataframe
    county_species_counts = read_species_to_dict(orig_df, logger)
    for cty, sp_counts in county_species_counts.iteritems():
        for (sp, count) in sp_counts:
            heat_df.loc[cty][sp] = count

    return heat_df


# ----------------------------------------------------
def upload_to_s3(full_filename, bucket, bucket_path, logger):
    """Upload a file to S3.

    Args:
        full_filename (str): Full filename to the file to upload.
        bucket (str): Bucket identifier on S3.
        bucket_path (str): Parent folder path to the S3 parquet data.
        logger (object): logger for saving relevant processing messages
        s3 (object): Connection to the S3 resource
        s3_client (object): object for interacting with Amazon S3.
        verbose (boolean): flag indicating whether to log verbose messages
        args: Additional arguments to be sent to the pandas.read_parquet function.

    Returns:
        s3_filename (str): path including bucket, bucket_folder, and filename for the
            uploaded data
    """
    s3_filename = None
    s3_client = boto3.client("s3")
    obj_name = os.path.basename(full_filename)
    if bucket_path:
        obj_name = f"{bucket_path}/{obj_name}"
    try:
        response = s3_client.upload_file(full_filename, bucket, obj_name)
    except ClientError as e:
        msg = f"Failed to upload {obj_name} to {bucket}, ({e})"
        if logger is not None:
            logger.log(logging.ERROR, msg)
        else:
            print(f"Error: {msg}")
    else:
        s3_filename = f"s3://{bucket}/{obj_name}"
        msg = f"Uploaded {s3_filename} to S3"
        if logger is not None:
            logger.log(logging.INFO, msg)
        else:
            print(f"INFO: {msg}")
    return s3_filename

# ...............................................
@property
def num_species(pam_df):
    """Get the number of species with at least one site present.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        int: The number of species that are present somewhere.

    Note:
        Also used as gamma diversity (species richness over entire landscape)
    """
    count = 0
    if pam_df is not None:
        count = int(pam_df.any(axis=0).sum())
    return count

# ...............................................
@property
def num_sites(pam_df):
    """Get the number of sites with presences.

    Returns:
        int: The number of sites that have present species.
    """
    count = 0
    if pam_df is not None:
        count = int(pam_df.any(axis=1).sum())
    return count

# ...............................................
def alpha(pam_df):
    """Calculate alpha diversity, the number of species in each site.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        A series of alpha diversity values for each site.
    """
    alpha_series = None
    if pam_df is not None:
        alpha_series = pam_df.sum(axis=1)
        alpha_series.name = "alpha_diversity"
    return alpha_series

# ...............................................
def alpha_proportional(pam_df):
    """Calculate proportional alpha diversity - percentage of species in each site.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        A series of proportional alpha diversity values for each site.
    """
    alpha_pr_series = None
    if pam_df is not None:
        alpha_pr_series = pam_df.sum(axis=1) / float(num_species(pam_df))
        alpha_pr_series.name = "alpha_proportional_diversity"
    return alpha_pr_series

# ...............................................
def whittaker_ratio(pam_df):
    """Calculate beta diversity for each site, Whitaker's ratio: gamma/alpha.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        beta_series (pandas.Series): ratio of gamma to alpha for each site.

    TODO: revisit this definition, also consider beta diversity region compared
        to region
    """
    beta_series = None
    if pam_df is not None:
        beta_series = float(num_species(pam_df)) / pam_df.sum(axis=1)
        # beta_series.replace([numpy.inf, -numpy.inf], 0, inplace=True)
        beta_series.name = "whitaker_ratio_gamma/alpha_beta_diversity"
    return beta_series

# ...............................................
def omega(pam_df):
    """Calculate the range size (number of counties) per species.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        omega_series (pandas.Series): A row of range sizes for each species.
    """
    omega_series = None
    if pam_df is not None:
        omega_series = pam_df.sum(axis=0)
        omega_series.name = "omega"
    return omega_series

# ...............................................
def omega_proportional(pam_df):
    """Calculate the mean proportional range size of each species.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        beta_series (pandas.Series): A row of the proportional range sizes for
            each species.
    """
    omega_pr_series = None
    if pam_df is not None:
        omega_pr_series = pam_df.sum(axis=0) / float(num_sites(pam_df))
    omega_pr_series.name = "omega_proportional"
    return omega_pr_series

# .............................................................................
def psi(pam_df):
    """Calculate the range richness of each species.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        psi_df (pandas.DataFrame): A 2d matrix of range richness for the sites that
            each species is present in.

    TODO: revisit this
    """
    psi_df = None
    if pam_df is not None:
        psi_df = pam_df.sum(axis=1).dot(pam_df)
    return psi_df

# .............................................................................
def psi_average_proportional(pam_df):
    """Calculate the mean proportional species diversity.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        psi_avg_df (pandas.DataFrame): A 2d matrix of proportional range richness
            for the sites that each species in the PAM is present.

    TODO: revisit this
    """
    psi_avg_df = None
    if pam_df is not None:
        psi_avg_df = (
                alpha(pam_df).dot(pam_df).astype(float)
                / (num_species(pam_df) * omega(pam_df))
        )
    return psi_avg_df

# # ...............................................
# def schluter_species_variance_ratio(self):
#     """Calculate Schluter's species variance ratio.
#
#     Returns:
#         float: The Schluter species variance ratio for the PAM.
#     """
#     sigma_species_, _hdrs = sigma_species(pam)
#     return float(sigma_species_.sum() / sigma_species_.trace())

# ...............................................
# def schluter_site_variance_ratio(self):
#     """Calculate Schluter's site variance ratio.
#
#     Returns:
#         float: The Schluter site variance ratio for the PAM.
#     """
#     sigma_sites_, _hdrs = sigma_sites(pam)
#     return float(sigma_sites_.sum() / sigma_sites_.trace())

# ...............................................
def whittaker(pam_df):
    """Calculate Whittaker's beta diversity metric for a PAM.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        float: Whittaker's beta diversity for the PAM.
    """
    val = float(num_species(pam_df) / omega_proportional(pam_df).sum())
    return "whittaker_beta_diversity", val

# ...............................................
def lande(pam_df):
    """Calculate Lande's beta diversity metric for a PAM.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        float: Lande's beta diversity for the PAM.
    """
    val = float(
        num_species(pam_df)
        - (pam_df.sum(axis=0).astype(float) / num_sites(pam_df)).sum()
    )
    return "Lande_beta_diversity", val

# ...............................................
def legendre(pam_df):
    """Calculate Legendre's beta diversity metric for a PAM.

    Args:
        pam_df (pandas.Dataframe): DataFrame containing a Presence-Absence Matrix

    Returns:
        float: Legendre's beta diversity for the PAM.
    """
    val = float(
        omega(pam_df).sum() - (float((omega(pam_df) ** 2).sum()) / num_sites(pam_df))
    )
    return "Legendre_beta_diversity", val

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    n = DT.datetime.now()
    date_str = f"{n.year}-{n.month}-{n.day}"

    # Create a logger
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger, log_filename = get_logger(f"{script_name}_{date_str}")

    # Download
    # s3_datafile = f"{BUCKET_PATH}/{species_county_list_fname}"
    # filename = download_from_s3(species_county_list_fname, BUCKET, s3_datafile, logger)

    # Read directly into DataFrame
    df = read_s3_parquet_to_pandas(
        BUCKET, BUCKET_PATH, species_county_list_fname, logger, s3_client=None
    ) #, **args)

    # Upload logfile to S3
    s3_log_filename = upload_to_s3(log_filename, BUCKET, LOG_PATH)
