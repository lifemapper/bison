--------------------------------------------------
BISON data load meeting
--------------------------------------------------
7/23-7/25/2019
Participants:: 
  Liz Sellers, Derek Masaki, Shayne Urbanowski (tele), Paul Hoyeu, Derek Dong, Aimee Stewart

Some Goals
~~~~~~~~~~
USGS Occurrences prioritization
    * invasive species
    * USGS data
    * Other federal agencies

Release BISON production monthly

Notes
~~~~~
Denver (Paul/Derek, contractors) for data --> SOLR and PostgreSQL
    * GBIF data load took 9 months
    * Used VM with 4tb storage, 100GB RAM
    * Can we use AWS for that?
    * Removed characters that XML rejects – why?  B/c Java was using UTF-16?  Can set JAVA_OPTIONS to include UTF-8

Environments:
    * Dev and Staging are USGS local
    * Prod is cloud (CHS/AWS)
    * SOLR testing in Dev – move to Staging
    * Regression testing – move to Prod

Decision: Use delta GBIF for next load?  No, can’t distinguish changed records, can’t track deleted records

Extract, Transform, Load:  Aimee will do Extract, Transform, Denver will do Load

Shayne and Derek: Must rethink entire process.  
    * PostgreSQL (9.6)
        * Used for web applications
        * Paul – try table partitioning
        * Shayne – manual vs auto-partitioning
        * Postgresql parameter tuning tool:  https://pgtune.leopard.in.ua/#/ , not good for high mem system
        * num indexes for I/O
        * Run vacuum every weekend
        * Update records in chunks – partitioning would solve this
        * Maybe get rid of?  
        * Data is huge and barely relational.
        * Consider non-relational DB, better suited for large data.  MongoDB?

Misc: 
    * SSD HD needs less RAM in Postgres
    * EBS – External Block Storage in cloud, EC2 instance
    * CSHCloud (AWS)
    *     *    * Large data queries, solution?

What?
    * AWS and lambdas
    * Neo4j

Performance:
    * Python
    * Panda
    * Data frames

Data loads
    * 2016. ORNL, GeoCloud
    * 2017-8 BISON only
    * 2018 fall GeoCloud  CSHCloud
    * 2019 spring GBIF only

Data Load
~~~~~~~~~
Inputs:
=======
    * GBIF data in 2 datasets 
        * US
        * US territories and Canada 
    * New BISON provider data from Liz 
        * One file per dataset
        * Same first BISON ~47 columns, optional spacer column, optional other columns to be ignored
        * processed to the same point as Process 1c below
    * Old BISON provider data from Paul
        * Ordered BISON 47 columns
    * Geo files from someone:
        * EEZ
        * Political boundaries
    * Liz-provided EstablishmentMeans table

Outputs:
========
    * Data load CSV (chunked into smaller files)
    * Lookup tables (verify fields for each from existing Postgres tables)
        * BISON resource 
        * BISON provider 
        * Geography
        * ScientificNames
        * VernacularNames

Process: (note LUT = lookup table)
==================================
    * Process GBIF data, mostly as 2018, with changes
        * Process GBIF download to CSV file of GBIF data.  Temp result = step1.csv
        * Filter records that fail for X reason
            * Empty string --> null
            * No scientificName or taxonKey
            * Question:  BISON provider, identified by???
        * Correct/standardize data values
        * If verbatimLocality is not null, BISON verbatim_locality = verbatimLocality
          elif locality is not null, BISON verbatim_locality = locality
          else BISON verbatim_locality = habitat
          Question: Precedence b/w habitat/locality/verbatimLocality?
        * Use ‘$’ delimiter in CSV output
        * Generate 2 lists (no duplicates) during dataset processing: 
            * Provider UUIDS 
            * ScientificName/taxonKey
        * Create LUTs
            * Provider: with GBIF API + provider UUID. Temp result: Provider LUT
            * Resource: Temp result: Resource LUT
                * Create list of dataset UUIDs from Dataset EML files
                * Create LUT from GBIF API + dataset UUID
            * CanonicalName: from GBIF parser + scientificName or taxonKey + API. 
          Temp result: sciName_or_taxonKey-canName LUT
        * Process edited step1.csv, replacing lookup values. 
          Temp result = GBIFdata.step3.csv
            * Fill Provider name, code, url, etc from Provider LUT 
            * Fill Resource name, code, url, etc from Resource LUT 
            * Overwrite ScientificName with CanonicalName in LUT
            * Remove any temporary columns for final BISON 47 columns 
    * Process BISON Provider data.  Temp result = updatedBISONprovider.csv 
        * Note:
            * New BISON provider data will be processed to same point as 1c above 
              (GBIFdata.step3.csv)
            * Old BISON provider data will have BISON 47 columns
        * Step 4: Identify datasets in new BISON provider data by:
            * ProviderID = 440 
            * ResourceID = 1000xx
            * ResourceURL like %bison.usgs.gov%
        * Step 5: Delete datasets identified in step 4 from old BISON provider data 
        * Step 6: Add new BISON provider data (BISON 47 columns) to edited old 
          BISON provider data 
    * Process All Data Load. Result: dataLoad.x.csv (multiple smaller files)
        * ITIS lookup: 
            * Find ITIS Name from ScientificName and ITIS API to get TSN, hierarchy, 
              vernacular. Temp result: ITIS LUT 
              (Scientific Name/TSN/hierarchy/vernacularName)
            * Create TSN/vernacular lookup.  Result: Vernacular LUT
        * Process all data, updating geo, marine, and names. Result: Geography LUT
            * Update Geo: 
                * Use existing geometries for 2019 data load
                * Do attribute join to get upper level geo from lower level 
                  (i.e. get state from county/fips)
                * Fill in geo record values based on Liz decision tree, use 
                  Python and GDAL
                * Primary, secondary, tertiary changes depending on region
                * Save/write LUT during processing.  
                * Question: what values are retained from old BISON provider data?  
                  Lat/long edited by Liz?
            * Update Marine EEZ from Geo and EEZ file
            * Update Names, fill in record values from ITIS lookup
        * Process all data, updating EstablishmentMeans (EM): If TSN is in 
          EstablishmentMeans table, update EM record value
          elif scientificName is in EstablishmentMeans table (exact match), 
          update EM record value


Actions
~~~~~~~
    * Liz/Derek will provide logins for Aimee to JIRA, Confluence
    * Shayne will export geometries from Postgres to shapefile
    * 8/5/2019: Aimee download GBIF data 
    * 9/30/2019: Aimee processed GBIF data (scripts written, tested, documented)
    * Other dates in Liz’s ppt 
        * ETA pre-Thanksgiving, dependent on KU hosted meeting, OS upgrade, Taiwan workshop: 
        * Aimee combined old and new BISON provider data
        * Aimee final processing complete 
             * inc scripts written, tested, documented for ITIS names, geo, EEZ, EstablishmentMeans
             * output: all data (GBIF + BISON provider) to BISON 47 column CSVs and 5 LUTs

Future
~~~~~~~
    * Possible IPT workflow: 
        * Liz uploads provider dataset to IPT, which checks (and sometimes corrects?) data
        * Aimee downloads corrected data from IPT, then continues editing
    * Update source data for geometries for next data load





