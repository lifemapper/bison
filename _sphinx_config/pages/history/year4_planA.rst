####################
Year 4, 2022-2023
####################
2023, Year 4 SOW specifies building the project as a Docker application, so that USGS
personnel can run the analyses on their desktop machines, whether using a Windows, Mac,
or Linux operating system.

The initial processes include annotating **GBIF occurrence data** from the
US, with designations from the **US Registry of Introduced and Invasive Species**
(RIIS), then summarizing the data by different regions, then aggregating data by a
geospatial grid of counties, and computing biodiversity statistics.

Regions include **US Census state and county boundaries**.  States are required
in order to identify whether the occurrence of a particular species falls within the
a RIIS region (Alaska, Hawaii, or Lower 48), where it is identified as "Introduced"
or "Invasive".  Region summaries include both state and county boundaries.

Regions also include **American Indian, Alaskan Native, and Native Hawaiian** (AIANNH)
regions and **US Federal Protected Areas** (US‐PAD).

All regions (state, county, AIANNH, PAD) will be summarized by count and proportion
for species, occurrences, and RIIS status.

**********************
Installation
**********************

LMBison can be run either locally on a powerful machine with a large amount of storage,
or on AWS.  The workflow is different for each of these options.


Hardware requirements
==========================

Data processing for BISON annotation, summary, and statistics requires a powerful
machine with a large amount of available storage.  The most processing intensive
step, annotate, intersects each record with 4 geospatial data files.  This
implementation can run this process in parallel, and uses the number of CPUs on the
machine minus 2.  In Aug 2023, using 18 (of 20) cores, on 904 million
records, the process took 5 days.

These processes are all written in Python, and the implementation has been tested
on a machine running Ubuntu Linux.  Scripts will need minimal modfication to run
on Windows or OSX successfully.

Download this Repository
==========================

The `LmBISON repository <https://github.com/lifemapper/bison>`_  can be installed by
downloading from Github.  This code repository contains scripts, Docker composition
files, configuration files, and test data for creating the outputs.

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

Download Large Data
==========================

Download newest versions of geospatial data.  Links and more information at `Input Data
<data_input>`_ .  In each case, new versions of the data might have different
fieldnames which are used as constants in the project.  Fields and their meaning/use
are identified in the same file, along with the constants that may need editing.  If
no new version is available, constants and fieldnames do not have to be checked.

Required Data not included in Github repo:

* US-RIIS data should be provided by the USGS.
* GBIF data may be downloaded at any time.  Fieldnames should remain constant.
* Census data:
  * county (includes state field)
  * American Indian/Alaska Native Areas/Hawaiian Home Lands (AIANNH)

Create expected file structure
==========================

Base data paths are specified in the user-created configuration file.  The configuration
file used by the author to test and execute the workflow is in the `process_gbif.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_gbif.json>`_ file.

.. _Year 4 Data Preparation:

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
updates.  Below are constants and their file locations that should be checked and
possibly modified anytime input data is updated.

The US-PAD dataset proved unsupportable in any configuration tried so far.  More
information is below under **Protected Areas Database**.


USGS RIIS data
==========================

US-RIIS V2.0, November 2022, available at https://doi.org/10.5066/P9KFFTOD
webpage: https://www.sciencebase.gov/catalog/item/62d59ae5d34e87fffb2dda99

US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.  Any other species/region combinations encountered will be
identified as "presumed-native"

**Data location**:  The RIIS data may be placed in any accessible directory, but must
be specified in the "riis_filename" value of the configuration file `process_gbif.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_gbif.json>`_.  The
RIIS annotation process will place the annotated file, with a postfix of "_annotated"
in the same directory.

The latest US-RIIS data is present in this Github repository in the `data/input
<https://github.com/lifemapper/bison/tree/main/data/input>`_ directory.  If a new
version is available, update it, and the following:

* Check/modify attributes in the RIIS_DATA class in the `constants.py
  <https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:
* Edit the filename in DATA_DICT_FNAME
* Check the file header, and if necessary, edit the fields in SPECIES_GEO_HEADER and
  matching fields in SPECIES_GEO_KEY, GBIF_KEY, ITIS_KEY, LOCALITY_FLD, KINGDOM_FLD,
  SCINAME_FLD, SCIAUTHOR_FLD, RANK_FLD, ASSESSMENT_FLD, TAXON_AUTHORITY_FLD.


GBIF data
==========================

To get a current version of GBIF data:
  * Create a user account on the GBIF website, then login and
  * request the data by putting the following URL in a browser:
    https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present
  * adding a restriction to occurrence data identified to species or a lower rank
    will reduce the amount of data that will be filtered out.

The query will request a download, which will take some time for GBIF to assemble.
GBIF will send an email with a link for downloading the Darwin Core Archive, a
very large zipped file.  Only the occurrence.txt file is required for data processing.
Rename the file with the date for clarity on what data is being used. Use
the following pattern gbif_yyyy-mm-dd.csv so that interim data filenames can be
created and parsed consistently.  Note the underscore (_) between 'gbif' and the date,
and the dash (-) between date elements.

**Data location**:  The GBIF data may be placed in any accessible directory, but must
be specified in the "gbif_filename" value of the configuration file `process_gbif.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_gbif.json>`_.  The
temporary output files, such as raw chunks, annotated chunks, and summaries of chunks,
will be placed in the directory specified in the "process_path" value of the
configuration file, with postfixes "_raw", "_annotate", and "_summary" respectively.
Final output files will be placed in the directory specified in the "output_path" value.

Verify that the file occurrence.txt contains GBIF-annotated records that will be the
primary input file.  The primary input file will contain fieldnames in the first line
of the file, and those listed as values for GBIF class attributes with (attribute)
names ending in _FLD or _KEY should all be among the fields.

    .. code-block::
    unzip <dwca zipfile> occurrence.txt
    mv occurrence.txt gbif_2023-08-23.csv

Check/modify attributes in the GBIF class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:

* Edit the filename in DATA_DICT_FNAME
* Verify that the DWCA_META_FNAME is still the correct file for field definitions.


Geographic Data for aggregation
==========================

Data location
----------------------------
The geospatial data may be placed in any accessible directory, but
must be specified in the "geo_path" value of the configuration file `process_gbif.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_gbif.json>`_.
Relative filepaths to the data are specified in the REGION class of the file
`constants.py <https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ .

Pre-Processing
----------------------------

Currently, much of our input data (GBIF, census county/state and AIANNH) are in
EPSG:4326, using decimal degrees.  The DOI dataset is in NAD_1983_Albers/EPSG:6269, and
the PAD datasets are in USA_Contiguous_Albers_Equal_Area_Conic_USGS_version/EPSG:9822.
These, and possibly other updated datasets must be projected to EPSG:4326 before
intersecting points and annotating records.  A sample script is in `project_doi_pad.sh
<https://github.com/lifemapper/bison/tree/main/bison/data/project_doi_pad.sh>`_

USGS may choose to change the geospatial regions for aggregation.  If so, the REGION
class in `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_
must be changed, and code changed slightly.  Only the county/state data is required for
matching RIIS records to occurrence records.

Census: State and County
----------------------------
Up-to-date census data including state and county boundaries are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

Shapefiles used for 2023 processing (2022 was not yet available at time of download):
Census, Cartographic Boundary Files, 2021
* https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

**Counties**
* 1:500,000, cb_2021_us_county_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:
including:  COUNTY["file"] for the filename and the keys in COUNTY["map"] for
fieldnames within that shapefile.

Census: AIANNH
----------------------------

2021 American Indian, Alaska Native, and Native Hawaiian lands
are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

**American Indian/Alaska Native Areas/Hawaiian Home Lands**, AIANNH
* 1:500,000, cb_2021_us_aiannh_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:
including:  AIANNH["file"] for the filename and the keys in AIANNH["map"] for
fieldnames within that shapefile.

Protected Areas Database, US-PAD (not currently used)
----------------------------

U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected Areas Database
of the United States (PAD-US) 3.0: U.S. Geological Survey data release,
https://doi.org/10.5066/P9Q9LQ4B.`


Plan: Annotate points with US-PAD regions for aggregation by species and
RIIS status.  US Protected Areas are split into files by Department of Interior regions,
and by state.  DOI region files are still very complex, and slow, so to efficiently
intersect points with US-PAD, we intersect with census data for the correct state
abbreviation, then intersect with the US-PAD file for that state.

Data:
  * https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download

The state US-PAD datasets still proved too complex to intersect at an acceptable speed.
Intersecting with 900 million records was projected to take 60 days.  I tested this data
in multiple implementations (local machine or Docker containers) and with multiple
versions of the data (split by Dept of Interior, DOI, regions, or by states) and with
multiple Docker configurations, all without success.  For this reason, US-PAD was
abandoned until a good solution can be found.

The next configuration to try will use different AWS tools.  I was unable to insert
these data into AWS RDS, PostgreSQL with PostGIS (other polygon datasets succeeded).

Project the dataset to EPSG:4326 with commands like A sample script is in
`project_doi_pad.sh
<https://github.com/lifemapper/bison/tree/main/bison/data/project_doi_pad.sh>`_

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

Local Data Layout
==========================
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

* In the big_data/gbif directory, place the gbif occurrence file
* In the big_data/geodata directory, place all geospatial data files.  All data within
  this directory will be referenced by relative filenames


* In the /volumes/bison directory create symbolic links to the large directory:
  * big_data

* and to the local repository
  * config (bison/data/config)
  * input (bison/data/input)
  * tests (bison/tests/data)

**********************
Processing Steps
**********************

Processing consists of 6 unique steps, each initiated with the process_gbif.py script
and 2 arguments: command and a parameter file.

Local data files, input and output paths, and other parameters are specified in the
user-created configuration file.  The local file used by the author to test and execute
the workflow is in the `process_gbif.json
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

Step 1: Annotate RIIS with GBIF Taxa
-------------------------------
We determine the Introduced and Invasive Species status of a GBIF record by first
resolving the scientificName in the US Registry of Introduced and Invasive Species
(RIIS) to the closest matching name in GBIF.

For this step, we will
* use the GBIF API to find the GBIF acceptedScientificName, and its acceptedTaxonKey,
  corresponding to every RIIS record scientificName, and
* append acceptedScientificName and acceptedTaxonKey to each RIIS record

::

    $ python process_gbif.py resolve data/config/process_gbif.json


Step 2: Split large GBIF data into manageable files
-------------------------------
We split the large GBIF data file into smaller chunks to reduce the memory footprint
of each process, allow for easier debugging, and facilitate parallel processing for
time-intensive, but not CPU-intensive, data processes.

::

    $ python process_gbif.py chunk data/config/process_gbif.json


Step 3: Annotate GBIF records with RIIS determinations and geographical regions
-------------------------------
RIIS annotation:
* US-RIIS records consist of a list of species and the areas in which they are
  considered Introduced or Invasive.

Geographical Areas:
* Census County and State boundaries from 2021 County file
* Census AIANNH from 2021 file
* (new, and failed in year 4, consider alternate methods) US_PAD

::

    $ python process_gbif.py annotate data/config/process_gbif.json

Step 4: Summarize annotations
-------------------------------

Summarize annotated GBIF occurrence records (each subset file), by:
   * location type (state, county, American Indian, Alaskan Native, and Native Hawaiian
     lands (AIANNH), and US-Protected Areas Database (PAD)).
   * location value
   * combined RIIS region and taxon key (RIIS region: AK, HI, L48)
   * scientific name, species name (for convenience in final aggregation outputs)
   * count

Then summarize the subset summaries into a single file, and aggregate single summary
into files of species and counts for each region:

::

    $ python process_gbif.py summarize data/config/process_gbif.json

Step 5: Create a heat matrix for counties x species
----------------------------------------------------

Create a 2d matrix of counties (rows) by species (columns) with a count for each species
found at that location.

::

    $ python process_gbif.py heat_matrix data/config/process_gbif.json


Step 6: Create a Presence-Absence Matrix (PAM) and compute stats
-----------------------------------------------------------------------

Convert the heat matrix into a binary PAM, and compute diversity statistics: overall
diversity of the entire region (gamma), county diversities (alpha) and county
diversities (alpha) and total diversity to county diversities (beta).  In addition,
compute species statistics: range size (omega) and mean proportional range size
(omega_proportional).

::

python process_gbif.py pam_stats data/config/process_gbif.json

Stats references for alpha, beta, gamma diversity:
* https://www.frontiersin.org/articles/10.3389/fpls.2022.839407/full
* https://specifydev.slack.com/archives/DQSAVMMHN/p1693260539704259
* https://bio.libretexts.org/Bookshelves/Ecology/Biodiversity_(Bynum)/7%3A_Alpha_Beta_and_Gamma_Diversity


####################
Run all processes on GBIF data
####################

Subset GBIF file
*************************

Chunk the large GBIF occurrence data file into smaller subsets:

    ```commandline
    python process_gbif.py chunk data/config/process_gbif.json
    ```

Annotate RIIS with GBIF Taxa
*************************

Annotate USGS RIIS records with GBIF Accepted Taxa, in order to link GBIF occurrence
   records with RIIS records using taxon and location.

    ```commandline
    python process_gbif.py resolve data/config/process_gbif.json
    ```

Annotate GBIF with RIIS and locations
*************************

Annotate GBIF occurrence records (each subset file) with:
   * state, for assigning RIIS determination and summarizing
   * other geospatial regions for summarizing
   * RIIS determinations using state and taxon contained in both GBIF and RIIS records

    ```commandline
    python process_gbif.py annotate data/config/process_gbif.json
    ```

Summarize annotations
*************************

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

Create a heat matrix
*************************

Create a 2d matrix of counties (rows) by species (columns) with a count for each species
found at that location.

    ```commandline
    python process_gbif.py heat_matrix data/config/process_gbif.json
    ```

Create a Presence-Absence Matrix (PAM) for counties x species, then compute statistics
*************************

Convert the heat matrix into a binary PAM, and compute diversity statistics: overall
diversity of the entire region (gamma), county diversities (alpha) and county
diversities (alpha) and total diversity to county diversities (beta).  In addition,
compute species statistics: range size (omega) and mean proportional range size
(omega_proportional).

    ```commandline
    python process_gbif.py pam_stats data/config/process_gbif.json
    ```

Compute heatmatrix, PAM, stats
*************************

Stats references for alpha, beta, gamma diversity:
* https://www.frontiersin.org/articles/10.3389/fpls.2022.839407/full
* https://specifydev.slack.com/archives/DQSAVMMHN/p1693260539704259
* https://bio.libretexts.org/Bookshelves/Ecology/Biodiversity_(Bynum)/7%3A_Alpha_Beta_and_Gamma_Diversity
