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

* This project also depends on Specify-lmpy, still in active development (Mar 2023).
  To easily keep these tools current, **from the activated bison venv**, uninstall if
  necessary, then install specify-lmpy from a cloned copy of the repository.

```commandline
(venv) astewart@murderbot:~/git/bison pip uninstall specify-lmpy
Found existing installation: specify-lmpy 3.1.21.post0.dev184
Uninstalling specify-lmpy-3.1.21.post0.dev184:
  Would remove:
    /home/astewart/git/bison/venv/bin/aggregate_matrices
    /home/astewart/git/bison/venv/bin/build_grid
    /home/astewart/git/bison/venv/bin/calculate_p_values
    /home/astewart/git/bison/venv/bin/calculate_pam_stats
    /home/astewart/git/bison/venv/bin/convert_csv_to_lmm
    /home/astewart/git/bison/venv/bin/convert_lmm_to_csv
    /home/astewart/git/bison/venv/bin/convert_lmm_to_geojson
    /home/astewart/git/bison/venv/bin/convert_lmm_to_raster
    /home/astewart/git/bison/venv/bin/convert_lmm_to_shapefile
    /home/astewart/git/bison/venv/bin/create_rare_species_model
    /home/astewart/git/bison/venv/bin/create_scatter_plot
    /home/astewart/git/bison/venv/bin/create_sdm
    /home/astewart/git/bison/venv/bin/create_tree_matrix
    /home/astewart/git/bison/venv/bin/encode_layers
    /home/astewart/git/bison/venv/bin/encode_tree_mcpa
    /home/astewart/git/bison/venv/bin/mcpa_run
    /home/astewart/git/bison/venv/bin/randomize_pam
    /home/astewart/git/bison/venv/bin/rasterize_point_heatmap
    /home/astewart/git/bison/venv/bin/split_occurrence_data
    /home/astewart/git/bison/venv/bin/wrangle_matrix
    /home/astewart/git/bison/venv/bin/wrangle_occurrences
    /home/astewart/git/bison/venv/bin/wrangle_species_list
    /home/astewart/git/bison/venv/bin/wrangle_tree
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/lmpy/*
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/specify_lmpy-3.1.21.post0.dev184.dist-info/*
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/tests/test_plots/*
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/tests/test_randomize/*
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/tests/test_spatial/*
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/tests/test_statistics/*
    /home/astewart/git/bison/venv/lib/python3.8/site-packages/tests/test_tutorials/*
Proceed (Y/n)? Y
  Successfully uninstalled specify-lmpy-3.1.21.post0.dev184


(venv) astewart@murderbot:~/git/bison cd ../lmpy
(venv) astewart@murderbot:~/git/lmpy$ git pull
Already up to date.
(venv) astewart@murderbot:~/git/lmpy$ cd ../bison
(venv) astewart@murderbot:~/git/lmpy$ pip install --upgrade pip
(venv) astewart@murderbot:~/git/bison$ pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org ~/git/lmpy
Processing /home/astewart/git/lmpy
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Preparing metadata (pyproject.toml) ... done
Requirement already satisfied: defusedxml in ./venv/lib/python3.8/site-packages (from specify-lmpy==3.1.21.post0.dev175) (0.7.1)
Requirement already satisfied: dendropy in /home/astewart/.local/lib/python3.8/site-packages (from specify-lmpy==3.1.21.post0.dev175) (4.5.2)
Requirement already satisfied: matplotlib in /usr/lib/python3/dist-packages (from specify-lmpy==3.1.21.post0.dev175) (3.1.2)
Requirement already satisfied: gdal in /usr/lib/python3/dist-packages (from specify-lmpy==3.1.21.post0.dev175) (3.0.4)
Requirement already satisfied: rtree in ./venv/lib/python3.8/site-packages (from specify-lmpy==3.1.21.post0.dev175) (0.9.7)
Requirement already satisfied: numpy in /home/astewart/.local/lib/python3.8/site-packages (from specify-lmpy==3.1.21.post0.dev175) (1.21.4)
Requirement already satisfied: requests in /usr/lib/python3/dist-packages (from specify-lmpy==3.1.21.post0.dev175) (2.22.0)
Requirement already satisfied: setuptools in ./venv/lib/python3.8/site-packages (from dendropy->specify-lmpy==3.1.21.post0.dev175) (60.5.0)
Building wheels for collected packages: specify-lmpy
  Building wheel for specify-lmpy (pyproject.toml) ... done
  Created wheel for specify-lmpy: filename=specify_lmpy-3.1.21.post0.dev175-py3-none-any.whl size=207498 sha256=d0c8cb7c04edc2675ec08553490669a60efd0d648cf0334bfed4f4a0f050043e
  Stored in directory: /tmp/pip-ephem-wheel-cache-19fai64l/wheels/1b/cc/3e/3bbc265f1071e7556f3c5562910676711a9fc5a56d8b8f672c
Successfully built specify-lmpy
Installing collected packages: specify-lmpy
Successfully installed specify-lmpy-3.1.21.post0.dev175
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

* In the big_data/geodata directory, place all geospatial data files.  All data within this directory will be
  referenced by relative filenames

```shell
astewart@badenov:/tank/bison/2023$ ll big_data/geodata/
...
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:34 aiannh/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 15:05 county/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:14 PADUS3_0_Region_10_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_11_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_12_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:19 PADUS3_0_Region_1_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:34 PADUS3_0_Region_2_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:14 PADUS3_0_Region_3_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:14 PADUS3_0_Region_4_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_5_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_6_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_7_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_8_SHP/
drwxrwxr-x  2 astewart astewart  4096 Feb  8 13:15 PADUS3_0_Region_9_SHP/
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
./lmbison.sh chunk_large_file data/config/chunk_large_file.json
```

## Annotate RIIS with GBIF Taxa

Annotate USGS RIIS records with GBIF Accepted Taxa, in order to link GBIF occurrence
   records with RIIS records using taxon and location.

```commandline
./lmbison.sh annotate_riis data/config/annotate_riis.json
```

## Annotate GBIF with RIIS and locations

Annotate GBIF occurrence records (each subset file) with:
   * state, for assigning RIIS determination and summarizing
   * other geospatial regions for summarizing
   * RIIS determinations using state and taxon contained in both GBIF and RIIS records

```commandline
./lmbison.sh annotate_gbif data/config/annotate_gbif.json
```

## Summarize annotations

Summarize annotated GBIF occurrence records (each subset file), by:
   * location type (state, county, AIANNH, PAD)
   * location value
   * combined RIIS region and taxon key (RIIS region: AK, HI, L48)
   * scientific name, species name (for convenience in final aggregation outputs)
   * count

```commandline
./lmbison.sh summarize_annotations data/config/summarize_annotations.json
```

## Combine summaries

Summarize summaries (each subset file) into a single summary:

```commandline
./lmbison.sh combine_summaries data/config/combine_summaries.json
```

## Aggregate summary

Aggregate summary into files of species and counts for each region:

```commandline
./lmbison.sh aggregate_summary data/config/aggregate_summary.json
```

## Split annotated records by taxon

Split the annotated records into CSV files for each species.  Annotation identifies (and
flags and reports) records with rank higher than species, so tested the number of
records and unique taxa when grouping by `acceptedScientificName` and `species`.
Grouping by species filters out the same number of records filtered in the annotation
step, and simplifies the file/species name.

```commandline
./lmbison.sh split_annotated_gbif data/config/split_annotated_gbif.json
```


# species: 10784 lines, 2127 headers/files = 8657 records (will leave out rank > species)
# acceptedKey: 12290 lines, 2300 header/file = 9990 records
# 9990 - 8657 = 1343 dropped, annotate_gbif.rpt totals the same



# Documentation

* Auto-generate readthedocs:
  https://docs.readthedocs.io/en/stable/intro/getting-started-with-mkdocs.html

```commandline
(venv)$ pip3 install mkdocs
```
