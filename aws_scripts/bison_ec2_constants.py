"""Constants to use locally initiate BISON AWS EC2 Spot Instances."""
PROJ_NAME = "bison"

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

USER_DATA_TOKEN = "###SCRIPT_GOES_HERE###"
