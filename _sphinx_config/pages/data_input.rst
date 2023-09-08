-----------------------
Input data
-----------------------

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

Pre-Processing
************

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
***********

US-RIIS V2.0, November 2022, available at https://doi.org/10.5066/P9KFFTOD
webpage: https://www.sciencebase.gov/catalog/item/62d59ae5d34e87fffb2dda99

Check/modify attributes in the RIIS_DATA class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:

* Edit the filename in DATA_DICT_FNAME
* Check the file header, and edit the fields in SPECIES_GEO_HEADER and
  matching fields in SPECIES_GEO_KEY, GBIF_KEY, ITIS_KEY, LOCALITY_FLD, KINGDOM_FLD,
  SCINAME_FLD, SCIAUTHOR_FLD, RANK_FLD, ASSESSMENT_FLD, TAXON_AUTHORITY_FLD if
  necessary.

US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.  Any other species/region combinations encountered will be
identified as "presumed-native"

GBIF  data
***********

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
********************************

Census data
----------------
Up-to-date census data, including state and county boundaries, and American Indian,
Alaska Native, and Native Hawaiian, are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

Shapefiles used for 2023 processing (2022 was not yet available at time of download):
Census, Cartographic Boundary Files, 2021
* https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html


State and County
.................

**Counties**
* 1:500,000, cb_2021_us_county_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:
including:  COUNTY["file"] for the filename and the keys in COUNTY["map"] for
fieldnames within that shapefile.

AIANNH
.........

**American Indian/Alaska Native Areas/Hawaiian Home Lands**, AIANNH
* 1:500,000, cb_2021_us_aiannh_500k.zip

Check/modify attributes in the REGION class in the `constants.py
<https://github.com/lifemapper/bison/tree/main/bison/common/constants.py>`_ file:
including:  AIANNH["file"] for the filename and the keys in AIANNH["map"] for
fieldnames within that shapefile.

Protected Areas Database
------------------------

DOI
....

Because the PAD data is divided into datasets by Department of Interior (DOI) region,
we intersect first with DOI to identify which PAD dataset to search.

* DOI regions
    * https://www.doi.gov/employees/reorg/unified-regional-boundaries

US-PAD
........
U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected Areas Database of the United States (PAD-US) 3.0: U.S. Geological Survey data release, https://doi.org/10.5066/P9Q9LQ4B.

* US_PAD for DOI regions 1-12
    * https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download
    * Metadata: https://www.sciencebase.gov/catalog/item/622262c8d34ee0c6b38b6bcf
    * Citation:
        U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022,
        Protected Areas Database of the United States (PAD-US) 3.0:
        U.S. Geological Survey data release, https://doi.org/10.5066/P9Q9LQ4B.
    * Geographic areas for Designation, Easement, Fee, Proclamation, Marine
    * target GAP status 1-3
        * 1 - managed for biodiversity - disturbance events proceed or are mimicked
        * 2 - managed for biodiversity - disturbance events suppressed
        * 3 - managed for multiple uses - subject to extractive (e.g. mining or logging) or OHV use
        * 4 - no known mandate for biodiversity protection
