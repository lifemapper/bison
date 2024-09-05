import json
import boto3
from datetime import datetime

print('Loading function')

def lambda_handler(event, context):
    region = "us-east-1"
    s3_client = boto3.client("s3", region_name=region)
    # Define the public bucket and file to query
    gbif_bucket = f"gbif-open-data-{region}"
    gbif_datestr = f"{datetime.now().year}-{datetime.now().month:02d}-01"
    parquet_key = f"occurrence/{gbif_datestr}/occurrence.parquet"

    print("Received event: " + json.dumps(event, indent=2))

    # Extract the S3 bucket and object key from the event
    trigger_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    trigger_key = event["Records"][0]["s3"]["object"]["key"]
    print(f"Event triggered by bucket/object {trigger_bucket}/{trigger_key}")

    # Double check for latest trigger file
    try:
        tr_response = s3_client.list_objects_v2(
            Bucket=trigger_bucket, Prefix=trigger_key, MaxKeys=10)
    except Exception as e:
        print(f"Error getting bucket/object {trigger_bucket}/{trigger_key} ({e})")
        raise e
    try:
        contents = tr_response["Contents"]
    except KeyError:
        msg = f"Trigger bucket/object {trigger_bucket}/{trigger_key} is not present"
    else:
        print(f"Trigger item: {contents[0]['Key']}")

        # If succeeded
        try:
            del_response = s3_client.delete_object(
                Bucket=trigger_bucket,
                Key=trigger_key
            )
        except Exception as e:
            print(f"Error deleting bucket/object {trigger_bucket}/{trigger_key} ({e})")
            raise e
        print(f"Delete response: {del_response}")

        # Check for latest GBIF content
        try:
            odr_response = s3_client.list_objects_v2(
                Bucket=gbif_bucket, Prefix=parquet_key, MaxKeys=10)
        except Exception as e:
            print(f"Error getting bucket/object {gbif_bucket}/{parquet_key} ({e})")
            raise e
        try:
            contents = odr_response["Contents"]
        except KeyError:
            msg = f"Found NO items in {gbif_bucket} in {parquet_key}"
        else:
            for item in contents:
                print(f"GBIF item: {item['Key']}")
            msg = f"Found {len(contents)}+ items in {gbif_bucket} in {parquet_key}"

    return {
        'statusCode': 200,
        'body': json.dumps(msg)
    }
