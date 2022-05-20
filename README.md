[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# 2022 Data processing

## Data Inputs

### GBIF
* Download GBIF data, query 
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* Download option Darwin Core Archive (The taxonKey and scientific name in Simple CSV 
  option is not always accepted).
* Latest download:  GBIF.org (19 May 2022) GBIF Occurrence Download 
  https://doi.org/10.15468/dl.y9k4yc 

### USGS RIIS
United States Register of Introduced and Invasive Species (US-RIIS)
https://doi.org/10.5066/P95XL09Q

## Test USGS input files

python3 test/test_RIIS.py
python3 test/test_taxonomy.py

## Census data for determining point county/state
Used the US Census 2020 cartographic boundaries from
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

Census data are in EPSG:4269 (WGS84), a geographic SRS very close to EPSG:4326 (NAD83).
For 2 reasons, I did not project the census data:
* The difference is on the order of meters.
* The GBIF data usually does not specify a datum

See https://gis.stackexchange.com/questions/170839/is-re-projection-needed-from-srid-4326-wgs-84-to-srid-4269-nad-83

Occasionally a point would intersect with a county envelope (created for a spatial index)
but not be contained within the returned geometry.  In that case, I returned the
values from the geometry nearest to the point.

# Project setup

## Python Dependencies
for development virtual environment and production build
  * [requirements.txt](requirements.txt)

Use a Python virtual environment, by installing and activating
```commandline
python3 -m venv venv
. venv/bin/activate
pip3 install -r requirements.txt
```

# Execution

All steps are executed by running the python script ./process_gbif.py with a command
and (for all commands except `resolve`, the original input GBIF filename.  Ensure that 
the virtual environment has been activated (the command prompt will be preceded by 
"(venv)").  

The constant BIG_DATA_PATH is set in the bison/common/constants.py file and specifies  
both the location of the original input data file, and the location of logfiles and 
data outputs.  Set this variable to an appropriate location (with enough disk space) 
on the computer running the processes.

```commandline
. venv/bin/activate
python  process_gbif  resolve    --riis_filename=<US RIIS file>
python  process_gbif  <command>  --gbif_filename=<original gbif file>
```

## Split GBIF data

GBIF data used for this project is very large, and more easily processed in smaller 
chunks.  Some time-consuming processes benefit by being run in parallel on 
different processors, so we split the data into chunks based on the number of 
processors on the machine running the program.

```commandline
python  process_gbif  split  --gbif_filename=gbif_2022-05_19.csv
```

## Resolve US-RIIS taxonomy

All GBIF DwC records have the verbatim scientific name resolved to the GBIF Backbone 
Taxonomy to allow grouping records with synonyms, misspellings, and other presumed 
similar names into a single species.  This step appends the GBIF acceptedScientificName
to the US-RIIS records so they may be matched with GBIF occurrence records.

Resolve all names in US-RIIS records to the GBIF "Accepted" scientific name.  This 
reads the ./data/US-RIIS_MasterList.csv file and appends 3 fields:  

* `gbif_res_taxonkey`: the acceptedTaxonKey linked to the acceptedScientificName found   
  from the GBIF taxon service for the `scientific_name` in this record. 
* `gbif_res_scientificName`: the acceptedScientificName found from the  
  GBIF taxon service for the `scientific_name` in this record.
* `LINENO`: the line number of this record in the original file, used for debugging

The command `resolve` 

```commandline
python  process_gbif  resolve  gbif_2022-05_19.csv
```

# Development

## Pre-commit

* Instructions in [.pre-commit-config.yaml](.pre-commit-config.yaml)
* When running a commit (and the pre-commit hooks), if files are modified, make sure to
  restage them, then run commit again to ensure that changes are saved.

# Documentation

* Auto-generate readthedocs: 
  https://docs.readthedocs.io/en/stable/intro/getting-started-with-mkdocs.html

```commandline
(venv)$ pip3 install mkdocs
```

# Testing

* Include execution of tests in pre-commit hooks, example in
  [Specify7](https://github.com/specify/specify7/blob/production/.pre-commit-config.yaml)

* Create test file with first 100K records + header

```commandline
head -n 100001 0090372-210914110416597.csv > gbif_2022-01-07_100k.csv
```

