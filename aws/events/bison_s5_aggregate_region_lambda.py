"""Lambda function to aggregate counts and lists by region."""
# Set lambda timeout to 5 minutes.
import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import time

print("*** Loading lambda function")

region = "us-east-1"
workgroup = "bison"
database = "dev"
pub_schema = "public"
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
external_schema = "redshift_spectrum"
timeout = 900
waittime = 1

# Get previous data date
yr = datetime.now().year
mo = datetime.now().month
prev_yr = yr
prev_mo = mo - 1
if mo == 1:
    prev_mo = 12
    prev_yr = yr - 1
bison_datestr = f"{yr}_{mo:02d}_01"
old_bison_datestr = f"{prev_yr}_{prev_mo:02d}_01"
bison_tbl = f"bison_{bison_datestr}"

# Define the bison bucket and table to create
bison_bucket = "bison-321942852011-us-east-1"
s3_out = f"s3://{bison_bucket}/summary"

COMMANDS = []

# Each fields tuple contains original fieldname, corresponding bison fieldname and type
ancillary_data = {
    "aiannh": {
        "table": "aiannh2023",
        "filename": "cb_2023_us_aiannh_500k.shp",
        "fields": {
            "name": ("namelsad", "aiannh_name", "VARCHAR(100)"),
            "geoid": ("geoid", "aiannh_geoid", "VARCHAR(4)")
        }
    },
    "county": {
        "table": "county2023",
        "filename": "cb_2023_us_county_500k.shp",
        "fields": {
            "state": ("stusps", "census_state", "VARCHAR(2)"),
            "county": ("namelsad", "census_county", "VARCHAR(100)"),
            "state_county": (None, "state_county", "VARCHAR(102)")
        }
    },
    "riis": {
        "table": f"riisv2_{bison_datestr}",
        # in S3; filename constructed with
        #   BisonNameOp.get_annotated_riis_filename(RIIS_BASENAME)
        "filename": f"USRIISv2_MasterList_annotated_{bison_datestr}.csv",
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

join_fld = "gbifid"
gbif_tx_fld = "taxonkey"
gbif_sp_fld = "species"
# Fields concatenated to ensure uniqueness
unique_sp_fld = "taxonkey_species"
unique_cty_fld = "state_county"
out_occcount_fld = "occ_count"
out_spcount_fld = "species_count"

# Get table, field names
cty_data = ancillary_data["county"]
b_st_fld = cty_data["fields"]["state"][1]
b_cty_fld = cty_data["fields"]["county"][1]

county_counts_tbl = f"county_counts_{bison_datestr}"
county_x_riis_counts_tbl = f"county_x_riis_counts{bison_datestr}"
county_list_tbl = f"county_x_species_list_{bison_datestr}"
# Redshift does not accept '-', but use that for parsing S3 filenames
county_list_s3key = county_list_tbl.replace("_", "-")
county_x_riis_counts_s3key = county_x_riis_counts_tbl.replace("_", "-")

state_counts_tbl = f"state_counts_{bison_datestr}"
state_x_riis_counts_tbl = f"state_x_riis_counts{bison_datestr}"
state_list_tbl = f"state_x_species_list_{bison_datestr}"
# Redshift does not accept '-', but use that for parsing S3 filenames
state_x_riis_counts_s3key = state_x_riis_counts_tbl.replace("_", "-")
state_list_s3key = state_list_tbl.replace("_", "-")

aiannh_data = ancillary_data["aiannh"]
b_nm_fld = aiannh_data["fields"]["name"][1]
b_gid_fld = aiannh_data["fields"]["geoid"][1]

aiannh_counts_tbl = f"aiannh_counts_{bison_datestr}"
aiannh_x_riis_counts_tbl = f"aiannh_x_riis_counts{bison_datestr}"
aiannh_list_tbl = f"aiannh_x_species_list_{bison_datestr}"
# Redshift does not accept '-', but use that for parsing S3 filenames
aiannh_list_s3key = aiannh_list_tbl.replace("_", "-")
aiannh_x_riis_counts_s3key = aiannh_x_riis_counts_tbl.replace("_", "-")

# RIIS data
riis_data = ancillary_data["riis"]
b_loc_fld = riis_data["fields"]["locality"][1]
b_occid_fld = riis_data["fields"]["occid"][1]
b_ass_fld = riis_data["fields"]["assess"][1]
# ...............................................
# Aggregate counts by region
# ...............................................
# Aggregate occurrence, species counts, RIIS status by region
# TODO: add RIIS assessment counts
state_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{state_counts_tbl} AS
        SELECT DISTINCT {b_st_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {unique_sp_fld} IS NOT NULL
        GROUP BY {b_st_fld};
"""
state_x_riis_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{state_x_riis_counts_tbl} AS
        SELECT DISTINCT {b_st_fld}, {b_ass_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {unique_sp_fld} IS NOT NULL
        GROUP BY {b_st_fld}, {b_ass_fld};
"""
# Note: in county agggregate, include states bc county names are not unique
county_counts_stmt = f"""
    CREATE TABLE public.{county_counts_tbl} AS
        SELECT DISTINCT {unique_cty_fld}, 
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {unique_sp_fld} IS NOT NULL
        GROUP BY {unique_cty_fld};
"""
county_x_riis_counts_stmt = f"""
    CREATE TABLE public.{county_x_riis_counts_tbl} AS
        SELECT DISTINCT {unique_cty_fld}, {b_ass_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
                            AND {unique_sp_fld} IS NOT NULL
        GROUP BY {unique_cty_fld}, {b_ass_fld};
"""

aiannh_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{aiannh_counts_tbl} AS
        SELECT DISTINCT {b_nm_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_nm_fld} IS NOT NULL
                            AND {unique_sp_fld} IS NOT NULL
        GROUP BY {b_nm_fld};
"""
aiannh_x_riis_counts_stmt = f"""
    CREATE TABLE {pub_schema}.{aiannh_x_riis_counts_tbl} AS
        SELECT DISTINCT {b_nm_fld}, {b_ass_fld},
            COUNT(*) AS {out_occcount_fld},
            COUNT(DISTINCT {gbif_tx_fld}) AS {out_spcount_fld}
        FROM  {bison_tbl} WHERE {b_nm_fld} IS NOT NULL
                            AND {unique_sp_fld} IS NOT NULL
        GROUP BY {b_nm_fld}, {b_ass_fld};
"""
# ...............................................
# Records of species, assessment, occ_count by region
# ...............................................
# Create species lists with counts and RIIS status for state, county, aiannh
state_list_stmt = f"""
    CREATE TABLE {pub_schema}.{state_list_tbl} AS
        SELECT DISTINCT {b_st_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld},
            {b_ass_fld}, COUNT(*) AS {out_occcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL AND {unique_sp_fld} IS NOT NULL
        GROUP BY {b_st_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld}, {b_ass_fld};
"""
state_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_list_tbl} ORDER BY {b_st_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{state_list_s3key}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
state_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_list_tbl} ORDER BY {b_st_fld}')
        TO '{s3_out}/{state_list_s3key}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
state_x_riis_counts_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{state_list_tbl} ORDER BY {b_st_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{state_list_s3key}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
county_list_stmt = f"""
    CREATE TABLE {pub_schema}.{county_list_tbl} AS
        SELECT DISTINCT {unique_cty_fld}, {b_st_fld}, {b_cty_fld}, {unique_sp_fld},
            {gbif_tx_fld}, {gbif_sp_fld}, {b_ass_fld}, COUNT(*) AS {out_occcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL AND {unique_sp_fld} IS NOT NULL
        GROUP BY {unique_cty_fld}, {b_st_fld}, {b_cty_fld}, {unique_sp_fld}, {gbif_tx_fld},
                 {gbif_sp_fld}, {b_ass_fld};
"""
county_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{county_list_tbl} ORDER BY {unique_cty_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{county_list_s3key}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
aiannh_list_stmt = f"""
    CREATE TABLE {pub_schema}.{aiannh_list_tbl} AS
        SELECT DISTINCT {b_nm_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld},
            {b_ass_fld}, COUNT(*) AS {out_occcount_fld}
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL AND {unique_sp_fld} IS NOT NULL
        GROUP BY {b_nm_fld}, {unique_sp_fld}, {gbif_tx_fld}, {gbif_sp_fld}, {b_ass_fld};
"""
aiannh_list_export_stmt = f"""
    UNLOAD (
        'SELECT * FROM {pub_schema}.{aiannh_list_tbl} ORDER BY {b_nm_fld}, {gbif_sp_fld}')
        TO '{s3_out}/{aiannh_list_s3key}_'
        IAM_role DEFAULT
        FORMAT AS PARQUET
        PARALLEL OFF;
"""
COMMANDS.extend([
    # Create tables of region with species counts, occurrence counts
    ("counts_by_state", state_counts_stmt),
    ("counts_by_county", county_counts_stmt),
    ("counts_by_aiannh", aiannh_counts_stmt),
    # Create lists of region with species, riis status, occurrence counts
    ("list_state_species", state_list_stmt),
    ("list_county_species", county_list_stmt),
    ("list_aiannh_species", aiannh_list_stmt),
    # Export lists of region with species, riis status, occurrence counts to S3
    ("export_state_species", state_list_export_stmt),
    ("export_county_species", county_list_export_stmt),
    ("export_aiannh_species", aiannh_list_export_stmt),
    ]
)

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)


# --------------------------------------------------------------------------------------
def lambda_handler(event, context):
    """Aggregate BISON records to species/occurrence counts and species lists by region.

    Args:
        event: AWS event triggering this function.
        context: AWS context of the event.

    Returns:
        JSON object

    Raises:
        Exception: on failure to execute Redshift command.
    """
    # Execute the commmands in order
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)
        except Exception:
            raise

        print("*** ---------------------------------------")
        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {stmt}")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete
        elapsed_time = 0
        complete = False
        while not complete:
            try:
                describe_result = client_redshift.describe_statement(Id=submit_id)
            except Exception as e:
                print(f"*** Failed to describe_statement in {elapsed_time} secs: {e}")
            else:
                status = describe_result["Status"]
                if status in ("ABORTED", "FAILED", "FINISHED"):
                    print(f"*** Status - {status} after {elapsed_time} seconds")
                    complete = True
                    if status == "FAILED":
                        try:
                            err = describe_result["Error"]
                        except Exception:
                            err = "Unknown Error"
                        print(f"***    FAILED: {err}")
                else:
                    time.sleep(waittime)
                    elapsed_time += waittime

    return {
        'statusCode': 200,
        'body': json.dumps("Lambda result logged")
    }
