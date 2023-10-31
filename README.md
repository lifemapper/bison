[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

# 2023 Data processing

## GBIF Input

* Download GBIF data, query
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* Download option Darwin Core Archive (The taxonKey and scientific name in Simple CSV
  option is not always the accepted version).
  * Final dataset for processing
    DOI: https://doi.org/10.15468/dl.vg6gg4 (may take some hours before being active)
    Creation Date: 15:01:13 23 August 2023
    Records included: 904377770 records from 3757 published datasets
    Compressed data size: 311.9 GB
    Download format: DWCA
    Filter used:
      GBIF.org (23 August 2023) GBIF Occurrence Download https://doi.org/10.15468/dl.epwzn6
    {
      "and" : [
        "Country is United States of America",
        "HasCoordinate is true",
        "HasGeospatialIssue is false",
        "OccurrenceStatus is Present"
      ]
    }

  * Test data

    When using this dataset please use the following citation:
    GBIF.org (25 September 2023) GBIF Occurrence Download https://doi.org/10.15468/dl.33f5eq
    Download Information
    DOI: https://doi.org/10.15468/dl.33f5eq (may take some hours before being active)
    Creation Date: 16:16:09 25 September 2023
    Records included: 1421243 records from 73 published datasets
    Compressed data size: 257.2 MB
    Download format: DWCA
    Filter used:
    {
      "and" : [
        "BasisOfRecord is Occurrence evidence",
        "Country is United States of America",
        "HasCoordinate is true",
        "HasGeospatialIssue is false",
        "OccurrenceStatus is Present"
      ]
    }

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

### US Protected Areas (US-PAD)

We annotate points with US-PAD regions for aggregation by species and
RIIS status.  US Protected Areas are split into files by Department of Interior regions,
and by state.  DOI region files are still very complex, and slow, so to efficiently
intersect points with US-PAD, we intersect with census data for the correct state
abbreviation, then intersect with the US-PAD file for that state.

Data:
  * https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download

### American Indian/Alaska Native/Native Hawaiian Lands (AIANNH)

We annotate points with AIANNH regions for aggregation by species and RIIS status.

Data:
  * https://catalog.data.gov/dataset/tiger-line-shapefile-2019-nation-u-s-current-american-indian-alaska-native-native-hawaiian-area

# Project setup

## Dependencies
Docker

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

### Data layout

* For local setup and testing, create directories to mimic the volumes created by the Dockerfile.
* Create a local /volumes/bison directory.  Everything contained in this
  directory will be a symlink to the repository or to the large data directory discussed next.
* Identify a directory with plenty of space, and create directories to contain
  large data files
  * input:
    * big_data/gbif
    * big_data/geodata
  * temporary processing files:
    * big_data/process
  * final output files:
    * big_data/output
* Example below with large data directory /mnt/sata8/bison/2023

```shell
astewart@murderbot:/mnt/sata8/bison/2023$ ll
...
drwxrwxr-x 3 astewart astewart 4096 Feb 15 12:07 big_data/
```

* In the big_data directory, place the gbif occurrence file

```shell
astewart@murderbot:/mnt/sata8/bison/2023$ ll big_data/
total 68
drwxrwxr-x  6 astewart astewart  4096 Mar  1 16:33 ./
drwxrwxr-x  4 astewart astewart  4096 Mar  1 16:33 ../
drwxrwxr-x  2 astewart astewart  4096 Mar  1 12:02 gbif/
drwxrwxr-x 15 astewart astewart  4096 Feb  9 16:05 geodata/
drwxrwxr-x  2 astewart astewart 45056 Feb 27 15:50 output/
drwxrwxr-x  2 astewart astewart  4096 Feb 27 14:50 process/
```

* In the big_data/geodata directory, place all geospatial data files.  All data within
  this directory will be referenced by relative filenames

```shell
astewart@badenov:/tank/bison/2023$ ll big_data/geodata/census
total 33184
drwxrwxr-x 2 astewart astewart     4096 Feb  9  2023 ./
drwxrwxr-x 5 astewart astewart     4096 Sep 12 15:25 ../
-rw-rw---- 1 astewart astewart        5 Apr  8  2022 cb_2021_us_aiannh_500k.cpg
-rw-rw---- 1 astewart astewart   183362 Apr  8  2022 cb_2021_us_aiannh_500k.dbf
-rw-rw---- 1 astewart astewart      165 Apr  8  2022 cb_2021_us_aiannh_500k.prj
-rw-rw---- 1 astewart astewart  2207408 Apr  8  2022 cb_2021_us_aiannh_500k.shp
-rwxrwxrwx 1 astewart astewart    37370 Apr  8  2022 cb_2021_us_aiannh_500k.shp.ea.iso.xml*
-rwxrwxrwx 1 astewart astewart    35993 Apr  8  2022 cb_2021_us_aiannh_500k.shp.iso.xml*
-rw-rw---- 1 astewart astewart     5732 Apr  8  2022 cb_2021_us_aiannh_500k.shx
-rw-rw-r-- 1 astewart astewart  1517895 Jan 26  2023 cb_2021_us_aiannh_500k.zip
-rw-rw---- 1 astewart astewart        5 Apr  8  2022 cb_2021_us_county_500k.cpg
-rw-rw---- 1 astewart astewart  1180828 Apr  8  2022 cb_2021_us_county_500k.dbf
-rw-rw---- 1 astewart astewart      165 Apr  8  2022 cb_2021_us_county_500k.prj
-rw-rw---- 1 astewart astewart 16837620 Apr  8  2022 cb_2021_us_county_500k.shp
-rwxrwxrwx 1 astewart astewart    26550 Apr  8  2022 cb_2021_us_county_500k.shp.ea.iso.xml*
-rwxrwxrwx 1 astewart astewart    35074 Apr  8  2022 cb_2021_us_county_500k.shp.iso.xml*
-rw-rw---- 1 astewart astewart    25972 Apr  8  2022 cb_2021_us_county_500k.shx
-rw-rw-r-- 1 astewart astewart 11838247 Feb  9  2023 cb_2021_us_county_500k.zip
```

* In the /volumes/bison directory create symbolic links to the large directory:
  * big_data

* and to the local repository
  * config (bison/data/config)
  * input (bison/data/input)
  * tests (bison/tests/data)

* for the following results:

```shell
astewart@murderbot:/volumes/bison$ ll
total 8
drwxrwxr-x 2 astewart astewart 4096 Mar  1 16:35 ./
drwxr-xr-x 5 astewart astewart 4096 Jan 31 15:47 ../
lrwxrwxrwx 1 astewart astewart   30 Feb 16 09:48 big_data -> /mnt/sata8/bison/2023/big_data/
lrwxrwxrwx 1 astewart astewart   36 Feb 15 12:13 config -> /home/astewart/git/bison/data/config/
lrwxrwxrwx 1 astewart astewart   35 Feb 15 12:34 input -> /home/astewart/git/bison/data/input/
lrwxrwxrwx 1 astewart astewart   35 Feb 15 12:16 tests -> /home/astewart/git/bison/tests/data/
```

### Pre-commit

* Instructions in [.pre-commit-config.yaml](.pre-commit-config.yaml)
* When running a commit (and the pre-commit hooks), if files are modified, make sure to
  restage them, then run commit again to ensure that changes are saved.

### Local Testing

* Include execution of tests in pre-commit hooks, example in
  [Specify7](https://github.com/specify/specify7/blob/production/.pre-commit-config.yaml)

* Create test file with first 100K records + header

```commandline
head -n 10001 occurrence.txt > gbif_2023-01-26_10k.csv
```

# Run all processes on GBIF data

## Subset GBIF file

Chunk the large GBIF occurrence data file into smaller subsets:

```commandline
python process_gbif.py chunk data/config/process_gbif.json
```

## Annotate RIIS with GBIF Taxa

Annotate USGS RIIS records with GBIF Accepted Taxa, in order to link GBIF occurrence
   records with RIIS records using taxon and location.

```commandline
python process_gbif.py resolve data/config/process_gbif.json
```

## Annotate GBIF with RIIS and locations

Annotate GBIF occurrence records (each subset file) with:
   * state, for assigning RIIS determination and summarizing
   * other geospatial regions for summarizing
   * RIIS determinations using state and taxon contained in both GBIF and RIIS records

```commandline
python process_gbif.py annotate data/config/process_gbif.json
```

## Summarize annotations

Summarize annotated GBIF occurrence records (each subset file), by:
   * location type (state, county, American Indian, Alaskan Native, and Native Hawaiian
     lands (AIANNH), and US-Protected Areas Database (PAD)).
   * location value
   * combined RIIS region and taxon key (RIIS region: AK, HI, L48)
   * scientific name, species name (for convenience in final aggregation outputs)
   * count

Then summarize the summaries into a single file, and aggregate summary into files of
species and counts for each region:

```commandline
python process_gbif.py summarize data/config/process_gbif.json
```

## Create a heat matrix

Create a 2d matrix of counties (rows) by species (columns) with a count for each species
found at that location.

```commandline
python process_gbif.py heat_matrix data/config/process_gbif.json
```

## Create a Presence-Absence Matrix (PAM) for counties x species, then compute statistics

Convert the heat matrix into a binary PAM, and compute diversity statistics: overall
diversity of the entire region (gamma), county diversities (alpha) and county
diversities (alpha) and total diversity to county diversities (beta).  In addition,
compute species statistics: range size (omega) and mean proportional range size
(omega_proportional).

```commandline
python process_gbif.py pam_stats data/config/process_gbif.json
```

## Compute heatmatrix, PAM, stats

Stats references for alpha, beta, gamma diversity:
* https://www.frontiersin.org/articles/10.3389/fpls.2022.839407/full
* https://specifydev.slack.com/archives/DQSAVMMHN/p1693260539704259
* https://bio.libretexts.org/Bookshelves/Ecology/Biodiversity_(Bynum)/7%3A_Alpha_Beta_and_Gamma_Diversity

# Documentation

* Auto-generate readthedocs:
  https://docs.readthedocs.io/en/stable/intro/getting-started-with-mkdocs.html

```commandline
(venv)$ pip3 install mkdocs
```

Build documentation:
https://docs.readthedocs.io/en/stable/intro/getting-started-with-sphinx.html
