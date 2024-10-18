#!/bin/bash
# This is the user data script to be executed on an EC2 instance.
sudo apt update
sudo apt install docker.io
sudo apt install docker-compose-v2

# TODO: change this to pull a Docker image
git clone https://github.com/lifemapper/bison.git

cd bison
sudo docker compose -f compose.annotate_riis.yml up -d
#sudo docker compose -f compose.development.yml -f compose.yml  up
# Executes from /home/bison directory, which contains bison code
sudo docker exec bison-bison-1 venv/bin/python -m bison.task.annotate_riis
