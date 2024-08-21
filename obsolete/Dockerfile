# Obsolete.  Docker implementation is inadequate for geospatial intersection of
# large datasets.

FROM osgeo/gdal:alpine-small-3.6.3 as backend

ARG BIG_DATA=/

RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y vim
RUN apt-get install -y python3-numpy
RUN apt-get install -y python3-requests
RUN apt-get install -y python3-rtree
RUN apt-get install -y python3-pip

# .....................................................................................
# Install lmbison projects for system

# Remove when this has been added to lmpy requirements
#RUN pip3 install requests
#RUN pip3 install numpy
RUN pip3 install pandas

RUN mkdir git

# lmbison latest from Github
RUN cd git &&  \
    git clone --branch docker-wrap https://github.com/lifemapper/bison.git &&  \
    cd bison \
    && pip install .

# specify-lmpy latest from Github
#RUN cd /git &&  \
#    git clone https://github.com/specifysystems/lmpy.git &&  \
#    cd lmpy \
#    && pip install .

ENV MAXENT_VERSION=3.4.4
ENV MAXENT_JAR=/git/Maxent/ArchivedReleases/$MAXENT_VERSION/maxent.jar

# .....................................................................................

# .....................................................................................
# Populate volumes with small, static input data
COPY data/input     /volumes/bison/input
COPY config    /volumes/bison/config
# For large input data, the user must identify a large disk on the host machine to
#   house inputs GBIF CSV and geospatial data, and temporary processing files created
#   by tools.  Docker will bind-mount this directory into docker containers.
