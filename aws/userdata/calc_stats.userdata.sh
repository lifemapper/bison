#!/bin/bash
sudo apt-get -y update
sudo apt-get -y install docker.io
sudo apt-get -y install docker-compose-v2
git clone https://github.com/lifemapper/bison.git
cd bison
sudo docker compose -f compose.calc_stats.yml up
sudo shutdown -h now
