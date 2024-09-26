#!/bin/bash
# This is the user data script to be executed on an EC2 instance.

aws configure set default.region us-east-1 && \
aws configure set default.output json

sudo apt update
sudo apt install apache2 certbot plocate unzip
sudo apt install docker.io
sudo apt install docker-compose-v2

# TODO: change this to pull a Docker image
git clone https://github.com/lifemapper/bison.git

sudo docker compose up -d
#sudo docker compose -f docker-compose.development.yml -f docker-compose.yml  up
# Executes from /home/bison directory, which contains bison code
sudo docker exec bison-bison-1 venv/bin/python -m bison/tools/annotate_riis
