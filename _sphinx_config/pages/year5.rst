#######################
Year 5, 2023-2024
#######################

2023, Year 5 SOW specifies building the project as an AWS application, so that USGS
personnel with little to no technical experience, and no local resources, can run the
analyses on a regular schedule.

The initial processes include annotating **GBIF occurrence data** from the
US, with designations from the **US Registry of Introduced and Invasive Species**
(RIIS), then summarizing the data by different regions, then aggregating data by a
geospatial grid of counties, and computing biodiversity statistics.

Regions include **US Census state and county boundaries**.  States are required
in order to identify whether the occurrence of a particular species falls within the
a RIIS region (Alaska, Hawaii, or Lower 48), where it is identified as "Introduced"
or "Invasive".  Region summaries include both state and county boundaries.

Regions also include **American Indian, Alaskan Native, and Native Hawaiian** (AIANNH)
regions and possibly **US Federal Protected Areas** (US‐PAD). US-PAD has proved
problematic and has failed to intersect in reasonable times, and crashed python when
attempting  insertion into an AWS PostgreSQL/PostGIS database.

All regions (state, county, AIANNH, PAD?) will be summarized by count and proportion
for species, occurrences, and RIIS status.


******************
Current decisions
******************
* Region: us-east-1
* Inputs: same as  :ref:_Year 4 Data Preparation

Setup
---------------------------
* install aws-cli on local dev machine
* Add reference data to S3 manually
* create EC2 for test/debug connection
  * update/upgrade apt, install stuff
  * add aws config and credentials


******************
Data Preparation
******************

2023, Year 5 SOW specifies data inputs:
  * US Registry of Introduced and Invasive Species
  * GBIF occurrence data from the US with coordinates
  * US Census state and county boundaries
  * American Indian, Alaskan Native, and Native Hawaiian (AIANNH) Lands

Data inputs may be updated regularly, so constants in some files may change with the
updates.  Below are constants and their file locations that should be checked and
possibly modified anytime input data is updated.

The US-PAD dataset proved unsupportable in any configuration tried so far.  More
information is below under **Protected Areas Database**.

Currently, much of our input data (GBIF, census county/state and AIANNH) are in
EPSG:4326, using decimal degrees.

USGS may choose to change the geospatial regions for aggregation.  If so, the REGION
class in `constants.py <../../bison/common/constants.py>`_
must be changed, and code changed slightly.  Only the county/state data is required for
matching RIIS records to occurrence records.

USGS RIIS data
----------------

US-RIIS V2.0, November 2022, available at https://doi.org/10.5066/P9KFFTOD
webpage: https://www.sciencebase.gov/catalog/item/62d59ae5d34e87fffb2dda99

US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.  Any other species/region combinations encountered will be
identified as "presumed-native"

The latest US-RIIS data is present in this Github repository in the `data/input
<../../data/input>`_ directory.  If a new
version is available, update it, and any data constants that may have changed.

Data constants
^^^^^^^^^^^^^^^^
* Check/modify attributes in the RIIS_DATA class in the `constants.py
  <../../bison/common/constants.py>`_ file:
* Edit the filename in DATA_DICT_FNAME
* Check the file header, and if necessary, edit the fields in SPECIES_GEO_HEADER and
  matching fields in SPECIES_GEO_KEY, GBIF_KEY, ITIS_KEY, LOCALITY_FLD, KINGDOM_FLD,
  SCINAME_FLD, SCIAUTHOR_FLD, RANK_FLD, ASSESSMENT_FLD, TAXON_AUTHORITY_FLD.


GBIF data options
----------------

**Option1:** Get a current version of GBIF data from the GBIF portal
  * Create a user account on the GBIF website, then login and
  * request the data by putting the following URL in a browser:
    https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
  * adding a restriction to occurrence data identified to species or a lower rank
    will reduce the amount of data that will be filtered out.

Verify that the file occurrence.txt contains GBIF-annotated records that will be the
primary input file.  The primary input file will contain fieldnames in the first line
of the file, and those listed as values for GBIF class attributes with (attribute)
names ending in _FLD or _KEY should all be among the fields.

The query will request a download, which will take some time for GBIF to assemble.
GBIF will send an email with a link for downloading the Darwin Core Archive, a
very large zipped file.  Only the occurrence.txt file is required for data processing.
Rename the file with the date for clarity on what data is being used. Use
the following pattern gbif_yyyy-mm-dd.csv so that interim data filenames can be
created and parsed consistently.  Note the underscore (_) between 'gbif' and the date, and
the dash (-) between date elements.

::

    unzip <dwca zipfile> occurrence.txt
    mv occurrence.txt gbif_2023-08-23.csv

**Option 2:** Use the GBIF Open Data Registry on AWS S3.  The data contains a subset of
Darwin Core fields.  More information is in `GBIF Ingestion`_ below.


Data constants
^^^^^^^^^^^^^^^^
Check/modify attributes in the GBIF class in the `constants.py
<../..//bison/common/constants.py>`_ file:

* Edit the filename in DATA_DICT_FNAME
* Verify that the DWCA_META_FNAME is still the correct file for field definitions.


Region Data
----------------

**Data location**:  The geospatial data may be placed in any accessible directory, but
must be specified in the "geo_path" value of the configuration file `process_gbif.json
<../../data/config/process_gbif.json>`_.
Relative filepaths to the data are specified in the REGION class of the file
`constants.py <https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ .

Census: State and County
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Up-to-date census data including state and county boundaries are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

Shapefiles used for 2023 processing (2022 was not yet available at time of download):
Census, Cartographic Boundary Files, 2021
* https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

**Counties**
* 1:500,000, cb_2021_us_county_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<../../common/constants.py>`_ file:
including:  COUNTY["file"] for the filename and the keys in COUNTY["map"] for
fieldnames within that shapefile.

Census: AIANNH
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Up-to-date census data, including American Indian, Alaska Native, and Native Hawaiian,
are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

**American Indian/Alaska Native Areas/Hawaiian Home Lands**, AIANNH
* 1:500,000, cb_2021_us_aiannh_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<../../bison/common/constants.py>`_ file:
including:  AIANNH["file"] for the filename and the keys in AIANNH["map"] for
fieldnames within that shapefile.

Protected Areas Database, US-PAD (not currently used)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected Areas Database
of the United States (PAD-US) 3.0: U.S. Geological Survey data release,
https://doi.org/10.5066/P9Q9LQ4B.

The US-PAD dataset proved too complex to intersect at an acceptable speed.  Intersecting
with 900 million records was projected to take 60 days.  I tested this data in
multiple implementations (local machine or Docker containers) and with multiple versions
of the data (split by Dept of Interior, DOI, regions, or by states) and with multiple
Docker configurations, with no success.  For this reason, US-PAD was abandoned until a
good solution can be found.

The next configuration to try will use different AWS tools.  I was unable to insert
these data into AWS RDS, PostgreSQL with PostGIS (other polygon datasets succeeded).

The PAD data is divided into datasets by Department of Interior (DOI) region, but
those datasets are still too large and complex.
Download the PAD data for states, this also removes the need for another intersect.

Project the dataset to EPSG:4326 with commands like A sample script is in
`project_doi_pad.sh <../../bison/data/preprocess/project_doi_pad.sh>`_

Reported problems with projected dataset:
* TopologyException: side location conflict
* Invalid polygon with 3 points instead of 0 or >= 4

* US_PAD for DOI regions 1-12
    * https://www.sciencebase.gov/catalog/item/62226321d34ee0c6b38b6be3
    * Metadata: https://www.sciencebase.gov/catalog/item/622262c8d34ee0c6b38b6bcf
    * Citation:
        U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022,
        Protected Areas Database of the United States (PAD-US) 3.0:
        U.S. Geological Survey data release, https://doi.org/10.5066/P9Q9LQ4B.
    * Geographic areas in separate shapefiles for Designation, Easement, Fee,
      Proclamation, Marine
    * target GAP status 1-3
        * 1 - managed for biodiversity - disturbance events proceed or are mimicked
        * 2 - managed for biodiversity - disturbance events suppressed
        * 3 - managed for multiple uses - subject to extractive (e.g. mining or logging) or OHV use
        * 4 - no known mandate for biodiversity protection
  * Citation: U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected
    Areas Database of the United States (PAD-US) 3.0: U.S. Geological Survey data
    release, https://doi.org/10.5066/P9Q9LQ4B.


******************
Workflow options
******************

Data ingestion and processing will be executed on Amazon Web Services (AWS), utilizing
several AWS tools.  Other data inputs will be placed in AWS resources, such as RDS or
S3, for easy access by AWS tools.  In order to minimize costs, we will experiment with
different data storage and processing strategies - they each have speed and cost pros
and cons.


GBIF Ingestion
---------------------------
* **Option 1:** Subset GBIF Open Data Registry to Bison S3 bucket, serverless, script
  `glue_bison_subset_gbif.py <../../scripts/glue_bison_subset_gbif.py>`_
* **Option 2:** Query GBIF portal manually, then initiate an EC2 Spot instance to
  download and subset it, saving it to S3. The script that downloads, subsets, and
  uploads data from the EC2 Spot instance is installed on the EC2
  instance on creation, `user_data_for_ec2spot.py
  <../../scripts/user_data_for_ec2spot.py>`_.  The script that builds and instantiates
  the EC2 Spot instance is: `gbif_to_s3.py <../../scripts/gbif_to_s3.py>`_ .

Reference Data
-----------------
Reference data consists of US-RIIS data and geospatial data for intersections.
Reference data will reside on AWS S3, and will be updated manually when new versions
becomes available.  These data are uploaded to S3 manually.

As part of a workflow, a process will add the reference data in S3 to a database in RDS.
The database must first be created with the SQL script
`init_database.sql <../../scripts/init_database.sql>`_.  This script will initialize a
PostgreSQL database in an existing RDS instance, and add PostGIS extensions for
geospatial data and operations.

A subsequent part of a workflow will add the data to RDS with the script
`populate_rds.py <../../scripts/populate_rds.py>`_.  This handles both standard CSV
data (RIIS) and geospatial data (census, AIANNH, PAD).

Setup
---------------------------
* install aws-cli on local dev machine
* Add reference data to S3 manually
* create EC2 for test/debug connection
  * update/upgrade apt, install stuff
  * add aws config and credentials
* Populate RDS
    * add postgis to postgres:
      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html
    * insert RIIS
    * insert geospatial data: census boundaries, native lands, PAD

* Redshift?

Experiment
---------------------------
* Find bucket, specify_athena

gbif_extract: 303237553
gbif_parquet_extract: 301669806

Use python libs **awscli** and **boto3** to connect with AWS

* query (Norway only):

  https://www.gbif.org/occurrence/download?basis_of_record=PRESERVED_SPECIMEN&basis_of_record=FOSSIL_SPECIMEN&basis_of_record=OCCURRENCE&country=NO&occurrence_status=present

* DwCA 9 GB data (2 GB zipped)
* 5,293,875 records
* download: https://www.gbif.org/occurrence/download/0098682-230530130749713



Workflow
---------------------------

* download GBIF data (~350 GB)

  * directly to EC2 instance using wget or script

* upload to S3

  * put-object with AWS CLI v2
    https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/put-object.html
  * AWS Python SDK put_object using Boto3
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html#

* pyspark
