#!/bin/bash
sudo apt update
sudo apt install docker.io
sudo apt install docker-compose-v2
git clone https://github.com/lifemapper/bison.git
cd bison
sudo docker compose -f compose.annotate_riis.yml up
sudo shutdown -h now
