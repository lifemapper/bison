"""Script to run locally to initiate an AWS EC2 Spot Instance."""
import boto3
import os

from bison.common.constants import REGION
from bison.common.util import (
    create_spot_launch_template_name, create_spot_launch_template, get_current_date_str,
    get_logger, run_instance_spot)

# # S3
# TRIGGER_PATH = "trigger"
# TRIGGER_FILENAME = "go.txt"

# shell script run on EC2 instantiation
user_data_matrix_fname = "ec2_userdata_matrix_stats.sh"
# python script to call from shell script
user_data_matrix_script_fname = "bison_matrix_stats.py"


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
        ec2_client, template_name, user_data_matrix_fname,
        user_data_matrix_script_fname, overwrite=True)

    # -------  Run instance from template -------
    # Runs the script on instantiation
    response = run_instance_spot(ec2_client, template_name)
