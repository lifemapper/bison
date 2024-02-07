[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# Year 4 Data processing

## GBIF Input

* Use the Global Biodiversity Information Facility (GBIF) Species Occurrences on the
  AWS Open Data Registry  https://registry.opendata.aws/gbif/
* Previously downloaded a full Darwin Core Archive from GBIF, startint with the query
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* The Simple CSV option does not always contain the accepted taxonKey and scientific name

## USGS RIIS Input

* Year 4 data: United States Register of Introduced and Invasive Species (US-RIIS)
  https://doi.org/10.5066/P95XL09Q
* Year 5 data: TBA

## Geospatial input for region aggregation

### Census data for county/state

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


### American Indian/Alaska Native/Native Hawaiian Lands (AIANNH)

We annotate points with AIANNH regions for aggregation by species and RIIS status.

Data:
  * https://catalog.data.gov/dataset/tiger-line-shapefile-2019-nation-u-s-current-american-indian-alaska-native-native-hawaiian-area

### US Protected Areas Database (US-PAD)

Unable to intersect these data with records because of the complexity of the shapefiles.  
Next time will try with a "flattened" version of the data.

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
