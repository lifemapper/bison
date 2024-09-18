import json
import boto3
import botocore.session as bc
from botocore.client import Config
from datetime import datetime
import pprint
import time

print("*** Loading lambda function")

region = "us-east-1"
workgroup = "bison"
database = "dev"
iam_role = "arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role"
db_user = "IAMR:bison_redshift_lambda_role"
pub_schema = "public"
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
input_folder = "input"

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
        }
    },
    "riis": {
        "table": f"riisv2_{bison_datestr}",
        "filename": "{riis_prefix}{bison_datestr}.csv",
        "fields": {
            "locality": ("locality", "riis_region", "VARCHAR(3)"),
            "occid": ("occurrenceid", "riis_occurrence_id", "VARCHAR(50)"),
            "assess": ("degreeofestablishment", "riis_assessment", "VARCHAR(50)")
        }
    }
}

join_fld = "gbifid"
# Get table, field names
cty_data = ancillary_data["county"]
b_st_fld = cty_data["fields"]["state"][1]
b_cty_fld = cty_data["fields"]["county"][1]
# RIIS data
riis_data = ancillary_data["riis"]
b_loc_fld = riis_data["fields"]["locality"][1]
b_occid_fld = riis_data["fields"]["occid"][1]
b_ass_fld = riis_data["fields"]["assess"][1]
# Aggregate occurrence, species counts, RIIS status by counties
#   Note: include states bc county names are not unique
county_species_occ_counts_tbl = f"county_counts_{bison_datestr}"
aggregate_by_county_stmt = f"""
	CREATE TABLE public.{county_species_occ_counts_tbl} AS
        SELECT DISTINCT {b_cty_fld}, {b_st_fld}, {b_ass_fld},
               COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
        GROUP BY {b_cty_fld}, {b_st_fld}, {b_ass_fld};
	"""
# Aggregate occurrence, species counts, RIIS status by states
state_species_occ_counts_tbl = f"state_counts_{bison_datestr}"
aggregate_by_state_stmt = f"""
    CREATE TABLE public.{state_species_occ_counts_tbl} AS
        SELECT DISTINCT {b_st_fld}, {b_ass_fld}
               COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
        FROM  {bison_tbl} WHERE {b_st_fld} IS NOT NULL
        GROUP BY {b_st_fld}, {b_ass_fld};
"""
# Get table, field names
aiannh_data = ancillary_data["aiannh"]
b_nm_fld = aiannh_data["fields"]["name"][1]
b_gid_fld = aiannh_data["fields"]["geoid"][1]

aiannh_species_occ_counts_tbl = f"aiannh_counts_{bison_datestr}"
aggregate_by_aiannh_stmt = f"""
    CREATE TABLE public.{aiannh_species_occ_counts_tbl} AS
        SELECT DISTINCT {b_nm_fld}, {b_ass_fld},
               COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
        FROM  {bison_tbl} WHERE {b_nm_fld} IS NOT NULL
        GROUP BY {b_nm_fld}, {b_ass_fld};
"""
county_list_tbl = f"county_lists_{bison_datestr}"
state_list_tbl = f"state_lists_{bison_datestr}"
aiannh_list_tbl = f"aiannh_lists_{bison_datestr}"
list_county_stmt = f"""
    CREATE TABLE public.{county_list_tbl} AS
        SELECT DISTINCT census_state, census_county, taxonkey, species, riis_assessment,
            COUNT(*) AS occ_count
        FROM  bison_2024_08_01 WHERE census_state IS NOT NULL
        GROUP BY census_state, census_county, taxonkey, species, riis_assessment;
"""
"""
CREATE TABLE public.state_lists_2024_08_01 AS
    SELECT DISTINCT census_state, taxonkey, species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_2024_08_01 WHERE census_state IS NOT NULL
    GROUP BY census_state, taxonkey, species, riis_assessment;
CREATE TABLE public.aiannh_lists_2024_08_01 AS
    SELECT DISTINCT aiannh_name, taxonkey, species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_2024_08_01 WHERE census_state IS NOT NULL
    GROUP BY aiannh_name, taxonkey, species, riis_assessment;
"""
# Add field commands, < 30 sec total
COMMANDS.extend([
    #
    ("aggregate_county", aggregate_by_county_stmt),
    #
    ("aggregate_state", aggregate_by_state_stmt),
    #
	("aggregate_aiannh", aggregate_by_aiannh_stmt),
    #

    ]
)

# Initialize Botocore session
session = boto3.session.Session()
bc_session = bc.get_session()
session = boto3.Session(botocore_session=bc_session, region_name=region)
# Initialize Redshift client
config = Config(connect_timeout=timeout, read_timeout=timeout)
client_redshift = session.client("redshift-data", config=config)

def lambda_handler(event, context):
    # Execute the commmands in order
    for (cmd, stmt) in COMMANDS:
        # -------------------------------------
        try:
            submit_result = client_redshift.execute_statement(
                WorkgroupName=workgroup, Database=database, Sql=stmt)
        except Exception as e:
            raise Exception(e)
        print("*** ---------------------------------------")
        print(f"*** {cmd.upper()} command submitted")
        print(f"***    {stmt}")
        submit_id = submit_result['Id']

        # -------------------------------------
        # Loop til complete, then describe result
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
        'body': json.dumps(f"Lambda result logged")
    }
