===============
Lifemapper-BISON processing
===============

Every unique step in the process has a command line interface (CLI) that accepts a
configuration file of arguments.  These tools are in the bison/tools directory.

Step 1: Annotate RIIS with GBIF Taxa
__________________
We determine the Introduced and Invasive Species status of a GBIF record by first
resolving the scientificName in the US Registry of Introduced and Invasive Species
(RIIS) to the closest matching name in GBIF.

For this step, we will
* use the GBIF API to find the GBIF acceptedScientificName, and its acceptedTaxonKey,
  corresponding to every RIIS record scientificName, and
* append acceptedScientificName and acceptedTaxonKey to each RIIS record

The `annotate_riis_with_gbif_taxa` tool in the bison/tools directory performs this task.


Step 2: Split large GBIF data into manageable files
__________________
We split the large GBIF data file into smaller chunks to reduce the memory footprint
of each process, allow for easier debugging, and facilitate parallel processing for
time-intensive, but not CPU-intensive, data processes.

The `chunk_large_file` tool performs this task.


Step 3: Annotate GBIF records with geographical areas and RIIS determinations
__________________
Geographical Areas:
* Census County and State boundaries from 2021 County file
* US_PAD from DOI regions 1-12
  * Geographic areas for Designation, Easement, Fee, Proclamation, Marine
  * target GAP status 1-3
    * 1 - managed for biodiversity - disturbance events proceed or are mimicked
    * 2 - managed for biodiversity - disturbance events suppressed
    * 3 - managed for multiple uses - subject to extractive (e.g. mining or logging) or OHV use
    * 4 - no known mandate for biodiversity protection
* US-RIIS records consist of a list of species and the areas in which they are considered
Introduced or Invasive.

U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2022, Protected Areas Database of the United States (PAD-US) 3.0: U.S. Geological Survey data release, https://doi.org/10.5066/P9Q9LQ4B.
