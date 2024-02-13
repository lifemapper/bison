"""Lambda function to respond to an S3 event of records deposited, then write to S3."""
import boto3
import io
import os.path
import pandas
import pyarrow

BUCKET = "bison-321942852011-us-east-1"
OUT_PATH = "raw_data"
GBIF_OCC_FNAME = "occurrence.txt"

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """Handler for lambda event.

    Args:
        event: event triggering this handler.
        context: context of the execution.

    Returns:
        JSON formatted status response.
    """
    # Get the input CSV file from S3
    input_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    input_name = event["Records"][0]["s3"]["object"]["key"]
    basename, _ = os.path.splitext(input_name)
    # Define the output Parquet file for S3
    output_name = f"{OUT_PATH}/{basename}.parquet"

    # Read CSV from S3
    csv_obj = s3_client.get_object(Bucket=input_bucket, Key=input_name)
    csv_data = csv_obj["Body"].read().decode("utf-8")

    # Convert CSV to Parquet
    dataframe = pandas.read_csv(io.StringIO(csv_data))
    table = pyarrow.Table.from_pandas(dataframe)
    s3_client.put_object(Body=table.serialize(), Bucket=BUCKET, Key=output_name)

    return {
        "statusCode": 200,
        "body": "CSV to Parquet conversion complete!"
    }
