===============
Lifemapper-BISON processing
===============

Processing consists of 6 unique steps, each initiated with the process_gbif.py script
and 2 arguments: command and a parameter file.

Local data files, input and output paths, and other parameters are specified in the
user-created configuration file.  The local file used by the author to test and execute
the workflow is in the `process_bison.json
<https://github.com/lifemapper/bison/tree/main/data/config/process_bison.json>`_ file.

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
__________________
We determine the Introduced and Invasive Species status of a GBIF record by first
resolving the scientificName in the US Registry of Introduced and Invasive Species
(RIIS) to the closest matching name in GBIF.

For this step, we will
* use the GBIF API to find the GBIF acceptedScientificName, and its acceptedTaxonKey,
  corresponding to every RIIS record scientificName, and
* append acceptedScientificName and acceptedTaxonKey to each RIIS record

```commandline
python process_gbif.py resolve data/config/process_gbif.json
```


Step 2: Split large GBIF data into manageable files
__________________
We split the large GBIF data file into smaller chunks to reduce the memory footprint
of each process, allow for easier debugging, and facilitate parallel processing for
time-intensive, but not CPU-intensive, data processes.

```commandline
python process_gbif.py chunk data/config/process_gbif.json
```

Step 3: Annotate GBIF records with geographical areas and RIIS determinations
__________________
Geographical Areas:
* Census County and State boundaries from 2021 County file
* Census AIANNH from 2021 file
* (failed in 2023, consider alternate methods) US_PAD from DOI regions 1-12
  * Geographic areas for Designation, Easement, Fee, Proclamation, Marine
  * target GAP status 1-3
    * 1 - managed for biodiversity - disturbance events proceed or are mimicked
    * 2 - managed for biodiversity - disturbance events suppressed
    * 3 - managed for multiple uses - subject to extractive (e.g. mining or logging) or OHV use
    * 4 - no known mandate for biodiversity protection
  * Citation: U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected
    Areas Database of the United States (PAD-US) 3.0: U.S. Geological Survey data
    release, https://doi.org/10.5066/P9Q9LQ4B.

RIIS annotation:
* US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.

Step 4: Summarize annotations
-------------------------------

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

Step 5: Create a heat matrix
-----------------------------

Create a 2d matrix of counties (rows) by species (columns) with a count for each species
found at that location.

```commandline
python process_gbif.py heat_matrix data/config/process_gbif.json
```

Step 6: Create a Presence-Absence Matrix (PAM) for counties x species
-----------------------------------------------------------------------

Convert the heat matrix into a binary PAM, and compute diversity statistics: overall
diversity of the entire region (gamma), county diversities (alpha) and county
diversities (alpha) and total diversity to county diversities (beta).  In addition,
compute species statistics: range size (omega) and mean proportional range size
(omega_proportional).

```commandline
python process_gbif.py pam_stats data/config/process_gbif.json
```

Step 7: Compute diversity statistics
----------------------------------------

Stats references for alpha, beta, gamma diversity:
* https://www.frontiersin.org/articles/10.3389/fpls.2022.839407/full
* https://specifydev.slack.com/archives/DQSAVMMHN/p1693260539704259
* https://bio.libretexts.org/Bookshelves/Ecology/Biodiversity_(Bynum)/7%3A_Alpha_Beta_and_Gamma_Diversity

