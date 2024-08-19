"""Constants to use locally initiate BISON AWS EC2 Spot Instances."""
PROJ_NAME = "bison"
ENCODING = "utf-8"

GBIF_BUCKET = "gbif-open-data-us-east-1/occurrence"
GBIF_ARN = "arn:aws:s3:::gbif-open-data-us-east-1"
GBIF_ODR_FNAME = "occurrence.parquet"

PROJ_BUCKET = f"{PROJ_NAME}-321942852011-us-east-1"
SPOT_TEMPLATE_BASENAME = "launch_template"

KEY_NAME = "aimee-aws-key"
REGION = "us-east-1"
# Allows KU Dyche hall
SECURITY_GROUP_ID = "sg-0b379fdb3e37389d1"
SECRET_NAME = "admin_bison-db-test"

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
ERR_SEPARATOR = "------------"


USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"


# .............................................................................
class REPORT:
    """Common keys for process report dictionary."""
    PROCESS = "process"
    RIIS_IDENTIFIER = "riis_ids"
    RIIS_TAXA = "riis_taxa"
    RIIS_RESOLVE_FAIL = "riis_bad_species"
    TAXA_RESOLVED = "names_resolved"
    RECORDS_UPDATED = "records_updated"
    RECORDS_OUTPUT = "records_output"
    INFILE = "input_filename"
    OUTFILE = "output_filename"
    LOGFILE = "log_filename"
    REPORTFILE = "report_filename"
    SUMMARY = "summary"
    REGION = "region"
    MIN_VAL = "min_val_for_presence"
    MAX_VAL = "min_val_for_presence"
    LOCATION = "locations"
    AGGREGATION = "riis_assessments_by_location"
    SPECIES = "species"
    OCCURRENCE = "occurrences"
    HEATMATRIX = "heatmatrix"
    ROWS = "rows"
    COLUMNS = "columns"
    ANNOTATE_FAIL = "annotate_gbifid_failed"
    ANNOTATE_FAIL_COUNT = "records_failed_annotate"
    RANK_FAIL = "rank_failed"
    RANK_FAIL_COUNT = "records_failed_rank"
    MESSAGE = "message"
