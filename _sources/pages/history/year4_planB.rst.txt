####################
Year 4, Part 2, 2023-2024
####################
The Docker implementation created for Year 4 was unable to handle the most
resource-intensive step, geospatial intersection of records with shapefiles for
annotating records with state, county, PAD, AIANNH.

The overall goals include annotating a subset of **GBIF occurrence** records with
designations from the **US Registry of Introduced and Invasive Species**
(RIIS), then summarizing the data by different regions and RIIS status, and finally
aggregating data counts by species into a geospatial grid of counties, and computing
biodiversity statistics.

Regions include **US Census state and county boundaries**,
**American Indian, Alaskan Native, and Native Hawaiian** (AIANNH) regions and
**US Federal Protected Areas** (US‐PAD).  All regions (state, county, AIANNH, PAD) will
be summarized by count and proportion for species, occurrences, and RIIS status.

GBIF record annotation and region summaries are performed on AWS, while RIIS record
resolution to GBIF names (an interim step, not a deliverable), and computing
biodiversity statistics are executed locally.

**********************
Installation
**********************

The resource intensive steps in this workflow use AWS, while the first and last steps
are only implemented locally.  This is the first step towards a fully AWS workflow in
Year 5.

Local Hardware requirements
==========================

Since only the first and last steps of the workflow, both not resource-intensive,
are performed locally, a smaller machine is appropriate for execution.  Since these
steps will be moved to AWS in the next iteration of the workflow, they are not
performed in Docker containers, and do require some software dependencies.

Download this Repository
==========================

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
directory location.  In Linux or OSX, open a Terminal
window.

Install dependencies in a Virtual Python Environment
=====================================

Setup a virtual python environment named `venv` at the top level of the repository,
i.e. ~/git/bison.

.. code-block::

    python -m venv venv
    . venv/bin/activate
    pip install -r requirements.txt

If GDAL fails to install because its underlying library in the OS is not met, first
install libgdal using instructions here:
https://mapscaping.com/installing-gdal-for-beginners/ , then run
**pip install -r requirements.txt** again.


**********************
Data Preparation
**********************

2023, Year 4 SOW specifies:
  * US Registry of Introduced and Invasive Species
  * GBIF occurrence data from the US with coordinates
  * US Census state and county boundaries
  * American Indian and Alaskan Native Land Area Representations (AIAN‐LAR)
  * US Federal Protected Areas (US‐PAD)
  * Summarize (count and proportion) regions by species name/RIIS

Data inputs may be updated regularly, so constants in some files may change with the
updates.  In each section are constants and their file locations that should be checked
and possibly modified anytime input data is updated.

The US-PAD dataset proved unsupportable in any configuration tried so far.  More
information is below under **Protected Areas Database**.  New strategies will be
tried in Year 5.

Download newest versions of geospatial data.  Links and more information at `Input Data
<data_input>`_ .  In each case, new versions of the data might have different
fieldnames which are used as constants in the project.  Fields and their meaning/use
are identified in the same file, along with the constants that may need editing.  If
no new version is available, constants and fieldnames do not have to be checked.

Required Data not included in Github repo:

* GBIF occurrence data is updated monthly on AWS Open Data Registry
* US-RIIS data should be provided by the USGS.
* Protected Areas Database will be revisited in Year 5
* Census data:
  * county (includes state field)
  * American Indian/Alaska Native Areas/Hawaiian Home Lands (AIANNH)

Amazon S3 storage
==========================

All input data, except GBIF records, are to be placed in a bucket on Amazon S3 in a
folder called `input_data`.  Details on data upload procedure are in the sections
describing each dataset.

Choose a region, then create an S3 bucket for BISON inputs and outputs, using the naming
convention <name>-<region>, for example bison-us-east-1.  Under
the bucket, create the directories (and subdirectories):

* **annotated_records**: for annotated GBIF occurrence records
* **input_data**: for
    * US census data AIANNH and US counties
    * US-RIIS annotated data (with GBIF accepted taxon name and key)
* **lib**: for python libraries needed by scripts, namely SQLAlchemy
* **log**: for output logfiles
* **out_data**: for output datafiles
* **scripts**: for scripts to be run on AWS resources


GBIF data
==========================

GBIF data is updated monthly on the AWS Registry of Open Data
https://registry.opendata.aws/gbif/, and available for all regions.
Our process mounts GBIF data in Amazon Redshift directly from the AWS Open Data Registry
in S3.  From this mounted dataset, the process mounts a subset of the records into
another table with filters:

* countrycode = 'US'
* decimallatitude IS NOT NULL
* decimallongitude IS NOT NULL
* occurrencestatus = 'PRESENT'
* taxonrank IN
  ('SPECIES', 'SUBSPECIES', 'FORM', 'INFRASPECIFIC_NAME', 'INFRASUBSPECIFIC_NAME')
* basisofrecord IN
  ('HUMAN_OBSERVATION', 'OBSERVATION', 'OCCURRENCE', 'PRESERVED_SPECIMEN');

**Move to Amazon Redshift**: Mount the original GBIF data in Amazon Redshift directly from
the AWS Open Data Registry in S3.  From this mounted dataset, create a  subset of the
records into another table with filters.  (**Processing Steps / Step 2**)

USGS RIIS data
==========================

US-RIIS V2.0, November 2022, available at https://doi.org/10.5066/P9KFFTOD
webpage: https://www.sciencebase.gov/catalog/item/62d59ae5d34e87fffb2dda99

US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.  Any other species/region combinations encountered will be
identified as "presumed-native"

The latest US-RIIS data is present in this Github repository in the `data/input
<https://github.com/lifemapper/bison/tree/main/data/input>`_ directory.  If a new
version is available, update it, and the following:

**Move to Amazon S3**: Using the S3 console interface, upload the original US-RIIS
datafile, and the annotated version (created in **Processing Steps / Step 1**) to the
input_data folder in the BISON bucket.


Census: State and County, AIANNH
==========================

Up-to-date census data including state and county boundaries are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

Shapefiles used for 2023 processing (2022 was not yet available at time of download):
Census, Cartographic Boundary Files, 2021
* https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

* US County Boundaries, 1:500,000
    * cb_2021_us_county_500k.shp
* American Indian/Alaska Native Areas/Hawaiian Home Lands, AIANNH, 1:500,000
    * cb_2021_us_aiannh_500k.zip

**Move to Amazon S3**: Using the S3 console interface, upload the shapefiles to the
input_data folder in the BISON bucket.


Protected Areas Database, US-PAD (not currently used)
==========================

U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected Areas Database
of the United States (PAD-US) 3.0: U.S. Geological Survey data release,
https://doi.org/10.5066/P9Q9LQ4B.

The US-PAD dataset proved too complex to intersect at an acceptable speed.  Intersecting
with 900 million records was projected to take 60 days.  I tested this data in
multiple implementations (local machine or Docker containers) and with multiple versions
of the data (split by Dept of Interior, DOI, regions, or by states) and with multiple
Docker configurations, with no success.  For this reason, US-PAD was abandoned until a
good solution can be found.

Reported problems during local processing with dataset:
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

Mark Wiltermuth, USGS, suggested the "flattened" datasets:
In terms of PAD-US v 3.0, I'd recommend the following options, if decided to revisit:
PAD-US 3.0 Vector Analysis File https://www.sciencebase.gov/catalog/item/6196b9ffd34eb622f691aca7
PAD-US 3.0 Raster Analysis File https://www.sciencebase.gov/catalog/item/6196bc01d34eb622f691acb5
 Tese are "flattened" though spatial analysis prioritized by GAP Status Code
(ie GAP 1 > GAP 2 > GAP > 3 > GAP 4), these are found on bottom of
https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download page.
However, the vector datasets are available only as ESRI Geodatabases.  Try these and the
flattened raster dataset in AWS Redshift.

The next configuration to try will use different AWS tools.  I was unable to insert
these data into AWS RDS, PostgreSQL with PostGIS (other polygon datasets succeeded).
Future experiments will use AWS Redshift and the flattened/simplified datasets described
below.


**********************
Processing Steps
**********************

To address new difficulties processing the data locally with newly added datasets,
we moved some steps from local or Docker execution to Amazon Web Services AWS, and kept
others to be executed locally.  To simplify, we abandoned the Docker implementation,
since the steps it was intended to speed up were moved to AWS.

The goal is to also move
locally executed steps over to AWS, removing the requirement of local installation, and
allowing the workflow to be initiated by an event, with the completion of each step
triggering subsequent steps.

Local processing consists of 2 steps, each initiated with the process_gbif.py script,
a parameter file, and a command.  Local data files, input and output paths, and other
parameters are specified in the configuration file.  The local file used by the author
to test and execute the workflow is in the `process_gbif.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_gbif.json>`_ file.

Required parameters include:

* riis_filename (str): full filename of input USGS RIIS data in CSV format.
* gbif_filename (str): full filename of input GBIF occurrence data in CSV format.
* do_split (bool): Flag indicating whether the GBIF data is to be (or has been) split into
   smaller subsets. The JSON value must be true or false (no quotes).
* run_parallel (bool): Flag indicating whether the annotation process is to be run in
  parallel threads. The JSON value must be true or false (no quotes).
* geo_path (str): Source directory containing geospatial input data.
* process_path (str): Destination directory for temporary data.
* output_path (str): Destination directory for output data.


Step 1: (Local) Annotate RIIS with GBIF Taxa
-------------------------------
We determine the Introduced and Invasive Species status of a GBIF record by first
resolving the scientificName in the US Registry of Introduced and Invasive Species
(RIIS) to the closest matching name in GBIF.

For this step, we will
* use the GBIF API to find the GBIF acceptedScientificName, and its acceptedTaxonKey,
  corresponding to every RIIS record scientificName, and
* append acceptedScientificName and acceptedTaxonKey to each RIIS record

::

    $ python ./aws_scripts/bison_annotate_riis.py --riis_file=data/input/US-RIIS_MasterList_2021.csv


Step 2: (Local to AWS) Upload ancillary input data to AWS S3
-----------------------------------------------
**Move to Amazon S3**: In the AWS S3 console interface, upload the following data file
to the **<BISON bucket>/input_data** folder:

* original US-RIIS datafile
* annotated US-RIIS datafile ("_annotated_yyyy-mm-dd" appended to the base filename.
* US County Boundaries, cb_2021_us_county_500k.shp (and supporting files)
* AIANNH, cb_2021_us_aiannh_500k.shp (and supporting files)


Step 3: (AWS) Subset GBIF data into Amazon Redshift
-----------------------------------------------

Mount the GBIF data directly from the Amazon Open Data Registry into Redshift for
filtering into a BISON subset for processing.

In the AWS Redshift console interface, open the "query editor", and paste in the
contents of the file **aws_scripts/rs_subset_gbif.sql**.  Run the script to mount GBIF
data, filter GBIF data into a **BISON subset** table, and unmount the GBIF data.

Step 4: (AWS) Load input data from S3 into Amazon Redshift
-------------------------------------------------------

Mount the input data uploaded to S3 in Step 2 to Redshift for later processing with
the BISON subset.

In the AWS Redshift console interface, open the "query editor", and paste in the
contents of the file **aws_scripts/rs_load_ancillary_data.sql**.  Run the script to load
county, AIANNH, and RIIS data into tables.

Step 5: (AWS) In Amazon Redshift, annotate BISON subset, and write to S3
-------------------------------------------------------------------------------------

Intersect the ancillary geospatial datasets with the BISON subset, and join BISON
records with annotated RIIS records to annotate BISON with geospatial regions and
RIIS status.

In the AWS Redshift console interface, open the "query editor", and paste in the
contents of the file **aws_scripts/rs_intersect_append.sql**.  Run the script to
annotate BISON records with county, state, and AIANNH regions and RIIS determinations,
then export data in CSV format to the **<BISON bucket>/annotated_records** folder on S3.

Step 4: (AWS) Summarize annotations
-------------------------------

Summarize BISON records by region and RIIS status, with counts of occurrences and
species.  Create lists of species for regions, including RIIS status and occurrence
counts.

In the AWS Redshift console interface, open the "query editor", and paste in the
contents of the file **aws_scripts/rs_aggregate_export.sql**.  Run the script to
summarize records by region, then export to the **<BISON bucket>/out_data** folder on
S3.  Tables named <region>_counts_yyyy_mm_dd contain records of
    region, riis status, occurrence count, and species count.
Outputs named <region>_lists_yyyy_mm_dd contain records of
    region, taxonkey, species, riis_assessment, and occurrence count


Step 5: (Local) Create a Presence-Absence Matrix (PAM) and compute stats
----------------------------------------------------

Create a 2d 'heat matrix' of counties (rows) by species (columns) with a count for each
species found at that location.  Convert the heat matrix into a binary PAM, and compute
diversity statistics: overall diversity of the entire region (gamma), county
diversities (alpha) and total diversity to county diversities (beta).  In addition,
compute species statistics: range size (omega) and mean proportional range size
(omega_proportional).

::

    $ python ./aws_scripts/bison_matrix_stats.py

Stats references for alpha, beta, gamma diversity:
* https://www.frontiersin.org/articles/10.3389/fpls.2022.839407/full
* https://specifydev.slack.com/archives/DQSAVMMHN/p1693260539704259
* https://bio.libretexts.org/Bookshelves/Ecology/Biodiversity_(Bynum)/7%3A_Alpha_Beta_and_Gamma_Diversity
