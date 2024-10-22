#!/bin/bash
# This is the user data script to be executed on an EC2 instance.
cd git/bison
sudo docker compose -f compose.test_task.yml up -d
