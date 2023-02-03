[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# 2023 Data processing

## Data Inputs

### GBIF

* Download GBIF data, query
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* Download option Darwin Core Archive (The taxonKey and scientific name in Simple CSV
  option is not always the accepted version).
* Initial test data
  GBIF.org (26 January 2023) GBIF Occurrence Download https://doi.org/10.15468/dl.epwzn6
  Download Information
  DOI: https://doi.org/10.15468/dl.epwzn6
  Creation Date: 15:04:20 26 January 2023
  Records included: 24309003 records from 1433 published datasets
  Compressed data size: 12.3 GB
  Download format: DWCA
  Filter used:
  {
    "and" : [
      "BasisOfRecord is Specimen",
      "Continent is North America",
      "Country is United States of America",
      "HasCoordinate is true",
      "HasGeospatialIssue is false",
      "OccurrenceStatus is Present"
    ]
  }


### USGS RIIS

* Year 4 data: United States Register of Introduced and Invasive Species (US-RIIS)
  https://doi.org/10.5066/P95XL09Q
* Year 5 data: TBA

### Census data for determining point county/state

* US Census 2021 cartographic boundaries from
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2021.html#list-tab-B7KHMTDJCFECH4SSL2

Census data are in EPSG:4269 (WGS84), a geographic SRS very close to EPSG:4326 (NAD83).
For 2 reasons, I did not project the census data:
* The difference is on the order of meters.
* The GBIF data usually does not specify a datum

See https://gis.stackexchange.com/questions/170839/is-re-projection-needed-from-srid-4326-wgs-84-to-srid-4269-nad-83

Occasionally a point would intersect with a county envelope (created for a spatial
index) but not be contained within the returned geometry.  In that case, I returned the
values from the geometry nearest to the point.

### Census data for determining point county/state

# Project setup

## Develop and Test

### Pre-commit

* Instructions in [.pre-commit-config.yaml](.pre-commit-config.yaml)
* When running a commit (and the pre-commit hooks), if files are modified, make sure to
  restage them, then run commit again to ensure that changes are saved.

### Local Testing

* Use a Python virtual environment, by creating and activating virtual environment
  and installing dependencies from [requirements.txt](requirements.txt)

```commandline
python3 -m venv venv
. venv/bin/activate
pip3 install -r requirements.txt
```

* Include execution of tests in pre-commit hooks, example in
  [Specify7](https://github.com/specify/specify7/blob/production/.pre-commit-config.yaml)

* Create test file with first 100K records + header

```commandline
head -n 10001 occurrence.txt > gbif_2023-01-26_10k.csv
```


## Deploy

### Dependencies
Docker


## Documentation

* Auto-generate readthedocs:
  https://docs.readthedocs.io/en/stable/intro/getting-started-with-mkdocs.html

```commandline
(venv)$ pip3 install mkdocs
```
