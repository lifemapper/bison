#!/bin/bash
# This is the user data script to be executed on an EC2 instance.
cd git/bison
sudo docker compose -f compose.test_task.yml up -d
# Executes from /home/bison/git/bison directory, which contains bison code
#sudo docker exec bison-bison-1 venv/bin/python -m bison.task.annotate_riis
