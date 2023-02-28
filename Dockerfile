FROM osgeo/gdal:ubuntu-small-latest as backend

RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y vim
RUN apt-get install -y python3-rtree
# RUN apt-get install python3-requests
RUN apt-get install -y python3-pip

# .....................................................................................
# Install lmbison projects for system

# Remove when this has been added to lmpy requirements
RUN pip install requests

RUN mkdir git

# specify-lmpy latest from Github
RUN cd git &&  \
    git clone https://github.com/lifemapper/bison.git &&  \
    cd bison \
    && pip install .

RUN pip3 install requests

ENV MAXENT_VERSION=3.4.4
ENV MAXENT_JAR=/git/Maxent/ArchivedReleases/$MAXENT_VERSION/maxent.jar

# .....................................................................................

# .....................................................................................
# Populate volumes with inputs
COPY ./data/input     /volumes/bison/input
COPY ./data/config    /volumes/bison/config
# User must place very large data in an appropriate place on the host machine
#   GBIF CSV data, geospatial data, and temporary processing files
# directory.  These data must be placed in an appropriate place on the host machine
# GBIF CSV data, geospatial data, and temporary processing files
#COPY ./data/big_data  /volumes/bison/big_data/
