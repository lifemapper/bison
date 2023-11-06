===============
Input data
===============

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

Pre-Processing
----------------

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

USGS RIIS data
----------------

US-RIIS V2.0, November 2022, available at https://doi.org/10.5066/P9KFFTOD
webpage: https://www.sciencebase.gov/catalog/item/62d59ae5d34e87fffb2dda99

US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.  Any other species/region combinations encountered will be
identified as "presumed-native"

**Data location**:  The RIIS data may be placed in any accessible directory, but must
be specified in the "riis_filename" value of the configuration file `process_bison.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_bison.json>`_.  The
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


GBIF  data
----------------

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
created and parsed consistently.  Note the underscore (_) between 'gbif' and the date, and
the dash (-) between date elements.

**Data location**:  The GBIF data may be placed in any accessible directory, but must
be specified in the "gbif_filename" value of the configuration file `process_bison.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_bison.json>`_.  The
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
----------------

**Data location**:  The geospatial data may be placed in any accessible directory, but
must be specified in the "geo_path" value of the configuration file `process_bison.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_bison.json>`_.
Relative filepaths to the data are specified in the REGION class of the file
`constants.py <https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ .

Census: State and County
................
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
.........

Up-to-date census data, including American Indian, Alaska Native, and Native Hawaiian,
are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

**American Indian/Alaska Native Areas/Hawaiian Home Lands**, AIANNH
* 1:500,000, cb_2021_us_aiannh_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:
including:  AIANNH["file"] for the filename and the keys in AIANNH["map"] for
fieldnames within that shapefile.

Protected Areas Database, US-PAD (not currently used)
...................................................

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
