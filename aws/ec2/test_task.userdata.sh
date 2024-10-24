#!/bin/bash
sudo apt update
sudo apt install docker.io
sudo apt install docker-compose-v2
git clone https://github.com/lifemapper/bison.git
cd bison
sudo docker compose -f compose.test_task.yml up
sudo shutdown now
