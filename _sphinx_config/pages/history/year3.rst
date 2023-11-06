==================
Year 3, 2021-2022
==================

2022, Year 3 SOW specifies building the project as a well-documented Python application,
that can be run by technically proficient USGS personnel with adequate computing
resources.

The initial processes include annotating **GBIF occurrence data** from the
US, with designations from the **US Registry of Introduced and Invasive Species**
(RIIS), then summarizing the data by different regions.

Regions include **US Census state and county boundaries**.  States are required
in order to identify whether the occurrence of a particular species falls within the
a RIIS region (Alaska, Hawaii, or Lower 48), where it is identified as "Introduced"
or "Invasive".  Region summaries include both state and county boundaries.

All regions (state, county) will be summarized by count and proportion for species,
occurrences, and RIIS status.


Project setup
--------------

* For a development virtual environment and production build, use a python virtual
  environment.  Install and activate:

::

    python3 -m venv venv
    . venv/bin/activate
    pip3 install -r requirements.txt

Data Inputs
--------------

GBIF
.............................................

* Download GBIF data, query
  https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
* Download option Darwin Core Archive (The taxonKey and scientific name in Simple CSV
  option is not always accepted).
* Latest download:  GBIF.org (19 May 2022) GBIF Occurrence Download
  https://doi.org/10.15468/dl.y9k4yc

USGS RIIS
.............................................

* United States Register of Introduced and Invasive Species (US-RIIS)
  https://doi.org/10.5066/P95XL09Q
* Test USGS input files


Census data for determining point county/state
.............................................

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


Execution
--------------

All steps are executed by running the python script ./process_gbif.py with a command
and (for all commands except resolve, the original input GBIF filename.  Ensure that
the virtual environment has been activated (the command prompt will be preceded by
"(venv)").

The constant BIG_DATA_PATH is set in the bison/common/constants.py file and specifies
both the location of the original input data file, and the location of logfiles and
data outputs.  Set this variable to an appropriate location (with enough disk space)
on the computer running the processes.

1. Split GBIF data
.............................................

* Command: split
* Input: gbif_2022-05-19.csv
* Output: list of files of pattern gbif_2022-05-19_chunk-<start_line>-<stop_line>_raw.csv

```commandline
. venv/bin/activate
python  process_gbif.py  split  --gbif_filename=gbif_2022-05-19.csv
```

This step prepares the GBIF data by splitting it into smaller chunks for easier
processing.  We split the data into chunks based on the number of processors
(chunk_count = processor_count-2).  Splitting the data and processing it in chunks
serves several purposes:

1. Smaller datasets can be moved more easily.
2. During debugging, errors can be found more quickly on smaller datasets.
3. Some time-consuming processes are run in parallel on different processors, speeding
execution.

2. Resolve US-RIIS taxonomy
.............................................

RIIS Data: https://www.sciencebase.gov/catalog/item/6357fcfed34ebe4425031fb6

* Command: resolve
* Input: US-RIIS_MasterList.csv
* Output: US-US-RIIS_MasterList_updated_gbif.csv

```commandline
. venv/bin/activate
python  process_gbif.py  resolve  --riis_filename=data/US-RIIS_MasterList.csv
```

This step prepares the US-RIIS data ("US-RIIS_MasterList.csv") by resolving each
record's scientificName to the acceptedScientificName in the GBIF Backbone Taxonomy.
Most GBIF DwC records have a verbatimScientificName that has been resolved to
acceptedScientificName, grouping records with synonyms, misspellings, and other
presumed similar names into a single species.  This step facilitates identifying GBIF
records to a US-RIIS status of introduced, invasive, or presumed_native based on the
GBIF acceptedScientificName.

This step appends 3 fields to the US-RIIS data:

* `gbif_res_taxonkey`: the acceptedTaxonKey linked to the acceptedScientificName found
  from the GBIF taxon service for the scientific_name in this record.
* `gbif_res_scientificName`: the acceptedScientificName found from the
  GBIF taxon service for the scientific_name in this record.
* `LINENO`: the line number of this record in the original file, used for debugging

## 3. Annotate DwC records

* Command: annotate
* Input: gbif_2022-05-19.csv
* Output: list of files of pattern gbif_2022-05-19_chunk-<start_line>-<stop_line>_annotated.csv

```commandline
. venv/bin/activate
python  process_gbif.py  annotate  --gbif_filename=gbif_2022-05-19.csv
```

This step annotates all GBIF DwC records with 5 additional fields, of 3 categories:

1) Geographic determined by intersecting coordinates with US Census Boundaries

   * `georef_cty`: County as determined by census boundaries
   * `georef_st`: State as determined by census boundaries

2) a flag indicating whether to annotate this record and include it in summaries, by
   marking all records identified to taxonRank species and below as True, all above
   species as False.

   * `do_summarize`: Mark records identified to taxonRank species or below
     (subspecies, variety, form, infraspecific_name, infrasubspecific_name)
     as True, all above as False.

3) RIIS identifier, and RIIS designation introduced, invasive, or presumed native. This
   assessment is computed from the occurrence record's taxon and region (Alaska, Hawaii,
   or the Lower 48 states). If an occurrence record is determined to a level below
   species (subspecies, variety, form, infraspecific_name, infrasubspecific_name),
   check also the species (higher level) and location are identified as introduced or
   invasive.

   * `riis_occurrence_id`: Matching RIIS unique identifier determination for this
      record's acceptedScientificName and location.
   * `riis_assessment`: RIIS assessment of introduced, invasive, or presumed_native, for
      this record's taxon and location.

This step then writes out the annotated, flagged records.

3. Summarize each file of annotated DwC records
.............................................

This step summarizes each annotated chunk by county and state, then writes out a summary
for each file

--------------
Development
--------------

Pre-commit
.............................................

* Instructions in [.pre-commit-config.yaml](.pre-commit-config.yaml)
* When running a commit (and the pre-commit hooks), if files are modified, make sure to
  restage them, then run commit again to ensure that changes are saved.

Documentation
.............................................

* Auto-generate readthedocs:
  https://docs.readthedocs.io/en/stable/intro/getting-started-with-mkdocs.html

```commandline
(venv)$ pip3 install mkdocs
```

Testing
.............................................

* Include execution of tests in pre-commit hooks, example in
  [Specify7](https://github.com/specify/specify7/blob/production/.pre-commit-config.yaml)

* Create test file with first 100K records + header

```commandline
head -n 100001 0090372-210914110416597.csv > gbif_2022-01-07_100k.csv
```
