[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# 2022 Data processing

## Data Inputs

### GBIF
* Download GBIF data, query https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* Download option Darwin Core Archive (The taxonKey and scientific name in Simple option is not always accepted).
* Latest download: GBIF.org (31 January 2022) GBIF Occurrence Download  https://doi.org/10.15468/dl.7hpepm

### USGS RIIS
United States Register of Introduced and Invasive Species (US-RIIS)
https://doi.org/10.5066/P95XL09Q

## Test USGS input files

python3 test/test_RIIS.py
python3 test/test_taxonomy.py

# Project setup

## Python Dependencies
for development virtual environment and production build
  * [requirements.txt](requirements.txt)

Use a Python virtual environment, by installing and activating
```commandline
python3 -m venv venv
. venv/bin/activate
pip3 install <python dependencies>
```

# Pre-commit
* Instructions in [.pre-commit-config.yaml](.pre-commit-config.yaml)
* When running a commit (and the pre-commit hooks), if files are modified, make sure to
  restage them, then run commit again to ensure that changes are saved.

# Testing
* Include execution of tests in pre-commit hooks, example in
  [Specify7](https://github.com/specify/specify7/blob/production/.pre-commit-config.yaml)

* Create test file with first 100K records + header
```commandline
head -n 100001 0090372-210914110416597.csv > gbif_2022-01-07_100k.csv
```

# Split GBIF data
To chunk the file into more easily managed small files, first prefix each smaller file
with the header from the original file, where the header is on line 1.
For example:
```commandline
head -n1 gbif_2022-02-15.csv > gbif_2022-02-15_lines_50000-100000.csv
```

Then split using sed command output, where 1-50000 are lines to delete (ignore) and
and 100000 is the line on which to stop.
```commandline
sed -e '1,50000d;100000q' occurrence.txt >> occurrence_lines_50000-100000.csv
```

Optionally, use the get_chunk_files function in the bison.tools.util module
```commandline
python get_chunk_files
```
