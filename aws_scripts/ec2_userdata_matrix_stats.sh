#!/bin/bash
# This is the user data script to be executed on an EC2 instance.

sudo su -
apt-get update -y
apt-get upgrade -y
apt-get install -y awscli
apt-get install -y python3-pip

aws configure set default.region us-east-1 && \
aws configure set default.output json

pip3 install boto3 pandas pyarrow requests

cat <<EOF > process_data_on_ec2.py
###SCRIPT_GOES_HERE###
EOF

python3 process_data_on_ec2.py
