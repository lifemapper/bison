"""Script to initiate an AWS EC2 Spot Instance to download CSV and save to S3."""
import boto3
import os

from ec2_constants import (
    INSTANCE_TYPE, KEY_NAME, REGION, SECURITY_GROUP_ID)
from ec2_utils import (
    create_spot_launch_template_name, create_spot_launch_template, get_current_date_str,
    get_logger, run_instance_spot)

# # S3
# TRIGGER_PATH = "trigger"
# TRIGGER_FILENAME = "go.txt"

user_data_matrix_stats_fname = "scripts/user_data_matrix_stats.sh"


# --------------------------------------------------------------------------------------
#
# --------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # -------  Create a logger -------
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    logger = get_logger(script_name)

    # ------- Get EC2 client -------
    ec2_client = boto3.client("ec2", region_name=REGION)

    # -------  Find or create template -------
    # Adds the script to the spot template
    desc = f"gbif_{get_current_date_str()}"
    template_name = create_spot_launch_template_name(desc_str=desc)
    success = create_spot_launch_template(
        ec2_client, template_name, INSTANCE_TYPE, SECURITY_GROUP_ID,
        user_data_matrix_stats_fname, KEY_NAME, overwrite=True)

    # -------  Run instance from template -------
    # Runs the script on instantiation
    response = run_instance_spot(ec2_client, template_name)

    # # Create and upload a file triggering an event that starts BISON data processing
    # upload_trigger_to_s3("trigger_bison_process", PROJ_BUCKET, TRIGGER_PATH)
