[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# Year 4 Data processing

1. Download this repository to local machine
2. Set up AWS bucket and folders for processing
3. Assemble static ancillary inputs (local, AWS)
4. Resolve RIIS records to GBIF accepted taxa (local, copy to AWS S3)
5. Subset GBIF data to BISON (AWS Glue)
6. Annotate BISON subset with regions and RIIS status (AWS Redshift)
7. Summarize BISON subset by regions and RIIS status (AWS Redshift)
8. Create HeatMatrix and Presence Absence Matrix (PAM) (local)
9. Compute site and species biodiveristy statistics on PAM (local)

## 1. Set up the local environment

### Download the repository

The `LmBISON repository <https://github.com/lifemapper/bison>`_  can be installed by
downloading from Github.  This code repository contains python code, scripts for AWS 
tools, Docker composition files, configuration files, and test data for creating the 
outputs.

Type `git` at the command prompt to see if you have git installed.  If you do not,
download and install git from https://git-scm.com/downloads .

Download the LmBISON repository, containing test data and configurations, by typing at
the command line:

.. code-block::

   git clone https://github.com/lifemapper/bison

When the clone is complete, move to the top directory of the repository, `bison`.
All hands-on commands will be executed in a command prompt window from this
directory location.  

### Install dependencies

Create a virtual python environment for installing local python dependencies.

```commandline

```

## 2. Set up AWS bucket and folders for processing

Under the BISON bucket (i.e. bucket-us-east-1), create the following folders:

*   annotated_records
*   input_data
*   lib
*   log
*   out_data
*   scripts


## 3. Assemble static ancillary inputs (local, AWS)

### USGS RIIS Input

Use the most current version of the United States Register of Introduced and Invasive Species (US-RIIS)
  * Year 4 data: https://doi.org/10.5066/P95XL09Q
  * Year 5 data: TBA

The current file is named US-RIIS_MasterList_2021.csv, and is available in the 
data/input directory of this repository.  Upload this file to 
s3://<S3 bucket>/input_data

### Census data for county/state

* US Census 2021 cartographic boundaries from
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2021.html#list-tab-B7KHMTDJCFECH4SSL2
* Upload the shapefile to s3://<S3 bucket>/input_data

Census data are in EPSG:4269 (WGS84), a geographic SRS very close to EPSG:4326 (NAD83).
For 2 reasons, I did not project the census data:
* The difference is on the order of meters.
* The GBIF data usually does not specify a datum

See https://gis.stackexchange.com/questions/170839/is-re-projection-needed-from-srid-4326-wgs-84-to-srid-4269-nad-83

### American Indian/Alaska Native/Native Hawaiian Lands (AIANNH)

Annotate points with AIANNH regions for aggregation by species and RIIS status.

Data:
  * https://catalog.data.gov/dataset/tiger-line-shapefile-2019-nation-u-s-current-american-indian-alaska-native-native-hawaiian-area

Upload the shapefile to s3://<S3 bucket>/input_data

### US Protected Areas Database (US-PAD)

Unable to intersect these data with records because of the complexity of the shapefiles.  
Next time will try using AWS Redshift with a "flattened" version of the data.

Try:
* PAD-US 3.0 Vector Analysis File https://www.sciencebase.gov/catalog/item/6196b9ffd34eb622f691aca7
* PAD-US 3.0 Raster Analysis File https://www.sciencebase.gov/catalog/item/6196bc01d34eb622f691acb5
 
These are "flattened" though spatial analysis prioritized by GAP Status Code
(ie GAP 1 > GAP 2 > GAP > 3 > GAP 4), these are found on bottom of
https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download page.

The vector datasets are available only as ESRI Geodatabases.  The raster datasets are 
Erdas Imagine format.  It appears to contain integers between 0 and 92, but may have 
additional attributes for those classifications. Try both in AWS Redshift.

Upload the raster and vector flattened zip files (test which works best later) to 
s3://<S3 bucket>/input_data

## 4. Resolve RIIS records to GBIF accepted taxa

Run this locally until it is converted to an AWS step.  Make sure that the 
data/config/process_gbif.json file is present.  From the bison repo top directory, 
making sure the virtual environment is activated, run:

  ```commandline
  python process_gbif.py --config_file=data/config/process_gbif.json resolve  
  ```

Upload the output file (like data/input/US-RIIS_MasterList_2021_annotated_2024-02-01.csv
with current date string) to s3://<S3 bucket>/input_data

## 5. Subset GBIF data to BISON (AWS Glue)

### GBIF Input

* Use the Global Biodiversity Information Facility (GBIF) Species Occurrences on the
  AWS Open Data Registry  https://registry.opendata.aws/gbif/
* Previously downloaded a full Darwin Core Archive from GBIF, startint with the query
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* The Simple CSV option does not always contain the accepted taxonKey and scientific name


# Project setup

## Dependencies
Amazon Web Services account with access to EC2, S3, Glue, and Redshift

## Develop and Test

### Installing dependencies

* For local setup, development, and testing, create and activate a python virtual
  environment to hold project dependencies from [requirements.txt](requirements.txt),
  and possibly [requirements-test.txt](requirements-test.txt).

```commandline
python3 -m venv venv
. venv/bin/activate
pip3 install -r requirements.txt
pip3 install -r requirements-test.txt
```


# Documentation

* Auto-generate readthedocs:
  https://docs.readthedocs.io/en/stable/intro/getting-started-with-mkdocs.html

```commandline
(venv)$ pip3 install mkdocs
```

Build documentation:
https://docs.readthedocs.io/en/stable/intro/getting-started-with-sphinx.html
