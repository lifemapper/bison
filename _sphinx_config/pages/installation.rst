===============
LMBison Preparation
===============

-----------------------
Hardware requirements
-----------------------

Data processing for BISON annotation, summary, and statistics requires a powerful
machine with a large amount of available storage.  The most processing intensive
step, annotate, intersects each record with 4 geospatial data files.  This
implementation can run this process in parallel, and uses the number of CPUs on the
machine minus 2.  In Aug 2023, using 18 (of 20) cores, on 904 million
records, the process took 5 days.

These processes are all written in Python, and the implementation has been tested
on a machine running Ubuntu Linux.  Scripts will need minimal modification to run
on Windows or OSX successfully.

-----------------------
Download this Repository
-----------------------

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

-----------------------
Download and check Data Inputs
-----------------------

Data inputs may be updated regularly, so constants in some files may change with the
updates.  Below are constants and their file locations that should be checked and
possibly modified anytime input data is updated.

RIIS data
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

GBIF  data
***********

To get a current version of GBIF data:
  * Create a user account on the GBIF website, then login and
  * request the data by putting the following URL in a browser:
    https://www.gbif.org/occurrence/search?country=US&has_coordinate=true&has_geospatial_issue=false&occurrence_status=present

The query will request a download, which will take some time for GBIF to assemble.
GBIF will send an email with a link for downloading the Darwin Core Archive, a
very large zipped file.  Only the occurrence.txt file is required for data processing.
Rename the file with the date for clarity on what data is being used. Use
the following pattern gbif_yyyy-mm-dd.csv so that interim data filenames can be
created and parsed consistently.  Note the underscore (_) between 'gbif' and the date, and
the dash (-) between date elements.

    .. code-block::
    unzip <dwca zipfile> occurrence.txt
    mv occurrence.txt gbif_2023-08-23.csv

Census data
***********

Up-to-date census data, including state aand county boundaries, and American Indian,
Alaska Native, and Native Hawaiian, are available at:
https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html