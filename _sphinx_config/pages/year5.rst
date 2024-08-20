#######################
Year 5, 2024
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
* Inputs: same as  :ref:`Year 4 Data Preparation`

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


GBIF Data
--------------
GBIF data can be requested and retrieved from the data portal in DarwinCore format,
as in Year 4, or subsetted directly from the GBIF Open Data Registry (ODR) in AWS S3.

Methods are documented below in `GBIF Data Ingestion`_.


Region Data
----------------

**Data location**:  The geospatial data may be placed in any accessible directory, but
must be specified in the "geo_path" value of the configuration file `process_gbif.json
<../../data/config/process_gbif.json>`_.

Census: State and County
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Up-to-date census data including state and county boundaries are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

Shapefiles used for 2023 processing (2022 was not yet available at time of download):
Census, Cartographic Boundary Files, 2021
* https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

**Counties**
* 1:500,000, cb_2021_us_county_500k.zip

Census: AIANNH
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Up-to-date census data (2023), including American Indian, Alaska Native, and Native Hawaiian,
are available at:
https://catalog.data.gov/dataset/tiger-line-shapefile-current-nation-u-s-american-indian-alaska-native-native-hawaiian-areas-aia

**American Indian/Alaska Native Areas/Hawaiian Home Lands**, AIANNH
* 1:500,000, cb_2021_us_aiannh_500k.zip


Protected Areas Database, US-PAD (not currently used)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^


U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2024, Protected Areas Database
of the United States (PAD-US) 4.0: U.S. Geological Survey data release,
https://doi.org/10.5066/P96WBCHS.

Mark Wiltermuth, USGS, suggested the "flattened" dataset, now on version 4.0.
PAD-US 4.0 Vector Analysis and Summary Statistics .
https://www.sciencebase.gov/catalog/item/652d4ebbd34e44db0e2ee458

This dataset is in ESRI geodatabase format in Data is in SRS
EPSG:102039 - USA Contiguous Albers Equal Area Conic - USGS Version.

All geospatial intersections are performed in Amazon Redshift on the unprojected
occurrence data (aka EPSG:4326).  Redshift only takes CSV or shapefile format.  I
transformed a subset of the data where GAP_Sts = 1 (or more) into a shapefile using
the ogr2ogr tool, then projected it to EPSG 4326.


******************
Workflow options
******************

Data ingestion and processing will be executed on Amazon Web Services (AWS), utilizing
several AWS tools.  Other data inputs will be placed in AWS resources, such as RDS or
S3, for easy access by AWS tools.  In order to minimize costs, we will experiment with
different data storage and processing strategies - they each have speed and cost pros
and cons.


GBIF Data Ingestion
--------------------

**Option1:** Darwin Core via GBIF data portal

To get a current version of GBIF data via the portal:
  * Create a user account on the GBIF website, then login and
  * request the data by putting the following URL in a browser:
    https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
  * adding a restriction to occurrence data identified to species or a lower rank
    will reduce the amount of data that will be filtered out.

The query will request a download, which will take some time for GBIF to assemble.
GBIF will send an email with a link for downloading the Darwin Core Archive, a
very large zipped file.  The download file will have an identifier that is used as the
name of the download file.  Note this identifier and edit the variable DOWNLOAD_NAME in
the user_data_for_ec2spot.sh script.  Only the occurrence.txt file is required for data
processing.  Rename the file with the date for clarity on what data is being used. Use
the following pattern gbif_yyyy-mm-dd.csv so that interim data filenames can be
created and parsed consistently.  Note the underscore (_) between 'gbif' and the date,
and the dash (-) between date elements.

Verify that the file occurrence.txt within the zipfile contains GBIF-annotated records
that will be the primary input file.  The primary input file will contain fieldnames in
the first line of the file, and those listed as values for GBIF class attributes with
(attribute) names ending in _FLD or _KEY should all be among the fields.

Two scripts are used to ingest the data:

  * `gbif_to_s3.py <../../scripts/gbif_to_s3.py>`_ launches a Spot EC2 instance which
    will download the data, extract the occurrence dataset, then upload it to S3.
  * `user_data_for_ec2spot.sh <../../scripts/user_data_for_ec2spot.sh>`_ is a script
    that is written to the EC2 Spot instance and then executed on instantiation.  In
    Update the variable DOWNLOAD_NAME in this script with the identifier for the
    download file.

References:

* `AWS S3 Select Doc
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html>`_
* `blog post
  <https://aws.amazon.com/blogs/storage/querying-data-without-servers-or-databases-using-amazon-s3-select/>`_

**Option 2:** Occurrence Records via AWS S3 Open Data Registry

The ODR data contains a subset of data fields, but includes the key fields of
accepted scientific name (resolved to the GBIF Backbone Taxonomy), taxonomic rank,
and latitude and longitude.
Ingest in AWS Glue Studio ETL Job: bison_subset_gbif, also documented in
`glue_bison_subset_gbif.py <../../scripts/glue_bison_subset_gbif.py>`_ file.

As of 2023/11/20, subseting via this Glue job took 14 hours and resulted in about
923 million records.


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

Glue
--------------

* Can add python code from files in S3
* https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html

    * In Job Details tab, Advanced Properties, Job Parameters add
        key --additional-python-modules
        value  s3://<bucket_name>/lib/SQLAlchemy-2.0.23.tar.gz

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

******************
Data constants
******************

Various constants indicate fieldnames or values of interest in code.  Check/modify
attributes in the `constants.py <../../bison/common/constants.py>`_ file:

GBIF:

In the GBIF class:
    * Edit the filename in DATA_DICT_FNAME
    * Verify that the DWCA_META_FNAME is still the correct file for field definitions.

USGS aggregation regions:

USGS may choose to change the geospatial regions for aggregation.  If so, the REGION
class must be changed, and code changed slightly.  Only the county/state data is
required for matching RIIS records to occurrence records. Each region type
(class member) in this class contains a dictionary of metadata relating to that region.
The key "file" contains the relative path to the shapefile, and the key "map" contains
a dictionary of fieldnames within that shapefile mapped to the corresponding fieldnames
to be appended to the occurrence data.

US Registry for Introduced and Invasive Species (RIIS):

In the RIIS_DATA class:
    * Edit the filename in DATA_DICT_FNAME
    * Check the file header, and if necessary, edit the fields in RIIS_DATA if
      changed.
