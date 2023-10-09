#!/bin/bash
echo "Starting script..."
yum update -y
cd /root
echo "retrieving data..."

export GBIF_FNAME=0098682-230530130749713
export BUCKET=bison-321942852011-us-east-1
export BUCKET_PATH=dev_data

wget https://api.gbif.org/v1/occurrence/download/request/${GBIF_FNAME}.zip
unzip ${GBIF_FNAME}.zip occurrence.txt
aws s3 cp occurrence.txt s3://${BUCKET}/${BUCKET_PATH}/

echo "Copied to S3!"
shutdown now
