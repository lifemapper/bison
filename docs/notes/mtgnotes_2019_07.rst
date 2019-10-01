--------------------------------------------------
BISON data load meeting
--------------------------------------------------
7/23-7/25/2019
Participants:: 
  Liz Sellers, Derek Masaki, Shayne Urbanowski (tele), Paul Hoyeu, Derek Dong, Aimee Stewart

Some Goals
~~~~~~~~~~
USGS Occurrences prioritization
<R>* invasive species
<R>* USGS data
<R>* Other federal agencies

Release BISON production monthly

Notes
~~~~~
Denver (Paul/Derek, contractors) for data --> SOLR and PostgreSQL
<R>* GBIF data load took 9 months
<R>* Used VM with 4tb storage, 100GB RAM
<R>* Can we use AWS for that?
<R>* Removed characters that XML rejects – why?  B/c Java was using UTF-16?  Can set JAVA_OPTIONS to include UTF-8

Environments:
<R>* Dev and Staging are USGS local
<R>* Prod is cloud (CHS/AWS)
<R>* SOLR testing in Dev – move to Staging
<R>* Regression testing – move to Prod

Decision: Use delta GBIF for next load?  No, can’t distinguish changed records, can’t track deleted records

Extract, Transform, Load:  Aimee will do Extract, Transform, Denver will do Load

Shayne and Derek: Must rethink entire process.  
<R>* PostgreSQL (9.6)
<R><R>* Used for web applications
<R><R>* Paul – try table partitioning
<R><R>* Shayne – manual vs auto-partitioning
<R><R>* Postgresql parameter tuning tool:  https://pgtune.leopard.in.ua/#/ , not good for high mem system
<R><R>* num indexes for I/O
<R><R>* Run vacuum every weekend
<R><R>* Update records in chunks – partitioning would solve this
<R><R>* Maybe get rid of?  
<R><R>* Data is huge and barely relational.
<R><R>* Consider non-relational DB, better suited for large data.  MongoDB?

Misc: 
<R>* SSD HD needs less RAM in Postgres
<R>* EBS – External Block Storage in cloud, EC2 instance
<R>* CSHCloud (AWS)
<R>* <R>*<R>* Large data queries, solution?

What?
<R>* AWS and lambdas
<R>* Neo4j

Performance:
<R>* Python
<R>* Panda
<R>* Data frames

Data loads
<R>* 2016. ORNL, GeoCloud
<R>* 2017-8 BISON only
<R>* 2018 fall GeoCloud  CSHCloud
<R>* 2019 spring GBIF only

Data Load
~~~~~~~~~
Inputs:
=======
<R>* GBIF data in 2 datasets 
<R><R>* US
<R><R>* US territories and Canada 
<R>* New BISON provider data from Liz 
<R><R>* One file per dataset
<R><R>* Same first BISON ~47 columns, optional spacer column, optional other columns to be ignored
<R><R>* processed to the same point as Process 1c below
<R>* Old BISON provider data from Paul
<R><R>* Ordered BISON 47 columns
<R>* Geo files from someone:
<R><R>* EEZ
<R><R>* Political boundaries
<R>* Liz-provided EstablishmentMeans table

Outputs:
========
<R>* Data load CSV (chunked into smaller files)
<R>* Lookup tables (verify fields for each from existing Postgres tables)
<R><R>* BISON resource 
<R><R>* BISON provider 
<R><R>* Geography
<R><R>* ScientificNames
<R><R>* VernacularNames

Process: (note LUT = lookup table)
==================================
<R>* Process GBIF data, mostly as 2018, with changes
<R><R>* Process GBIF download to CSV file of GBIF data.  Temp result = GBIFdata.step1.csv
<R><R>* Filter records that fail for X reason
<R><R><R>* Empty string --> null
<R><R><R>* No scientificName or taxonKey
<R><R><R>* Question:  BISON provider, identified by???
<R><R>* Correct/standardize data values
<R><R>* If verbatimLocality is not null, BISON verbatim_locality = verbatimLocality
<R><R>  elif locality is not null, BISON verbatim_locality = locality
<R><R>  else BISON verbatim_locality = habitat
<R><R>  Question: Precedence b/w habitat/locality/verbatimLocality?
<R><R>* Use ‘$’ delimiter in CSV output
<R><R>* Generate 2 lists (no duplicates) during dataset processing: 
<R><R><R>* Provider UUIDS 
<R><R><R>* ScientificName/taxonKey
<R><R>* Create LUTs
<R><R><R>* Provider: with GBIF API + provider UUID. Temp result: Provider LUT
<R><R><R>* Resource: Temp result: Resource LUT
<R><R><R><R>* Create list of dataset UUIDs from Dataset EML files
<R><R><R><R>* Create LUT from GBIF API + dataset UUID
<R><R><R>* CanonicalName: from GBIF parser + scientificName or taxonKey + API. 
<R><R>  Temp result: sciName_or_taxonKey-canName LUT
<R><R>* Process edited GBIFdata.step1.csv, replacing lookup values. 
<R><R>  Temp result = GBIFdata.step3.csv
<R><R><R>* Fill Provider name, code, url, etc from Provider LUT 
<R><R><R>* Fill Resource name, code, url, etc from Resource LUT 
<R><R><R>* Overwrite ScientificName with CanonicalName in LUT
<R><R><R>* Remove any temporary columns for final BISON 47 columns 
<R>* Process BISON Provider data.  Temp result = updatedBISONprovider.csv 
<R><R>* Note:
<R><R><R>* New BISON provider data will be processed to same point as 1c above 
<R><R><R>  (GBIFdata.step3.csv)
<R><R><R>* Old BISON provider data will have BISON 47 columns
<R><R>* Step 4: Identify datasets in new BISON provider data by:
<R><R><R>* ProviderID = 440 
<R><R><R>* ResourceID = 1000xx
<R><R><R>* ResourceURL like %bison.usgs.gov%
<R><R>* Step 5: Delete datasets identified in step 4 from old BISON provider data 
<R><R>* Step 6: Add new BISON provider data (BISON 47 columns) to edited old 
<R><R>  BISON provider data 
<R>* Process All Data Load. Result: dataLoad.x.csv (multiple smaller files)
<R><R>* ITIS lookup: 
<R><R><R>* Find ITIS Name from ScientificName and ITIS API to get TSN, hierarchy, 
<R><R><R>  vernacular. Temp result: ITIS LUT 
<R><R><R>  (Scientific Name/TSN/hierarchy/vernacularName)
<R><R><R>* Create TSN/vernacular lookup.  Result: Vernacular LUT
<R><R>* Process all data, updating geo, marine, and names. Result: Geography LUT
<R><R><R>* Update Geo: 
<R><R><R><R>* Use existing geometries for 2019 data load
<R><R><R><R>* Do attribute join to get upper level geo from lower level 
<R><R><R><R>  (i.e. get state from county/fips)
<R><R><R><R>* Fill in geo record values based on Liz decision tree, use 
<R><R><R><R>  Python and GDAL
<R><R><R><R>* Primary, secondary, tertiary changes depending on region
<R><R><R><R>* Save/write LUT during processing.  
<R><R><R><R>* Question: what values are retained from old BISON provider data?  
<R><R><R><R>  Lat/long edited by Liz?
<R><R><R>* Update Marine EEZ from Geo and EEZ file
<R><R><R>* Update Names, fill in record values from ITIS lookup
<R><R>* Process all data, updating EstablishmentMeans (EM): If TSN is in 
<R><R>  EstablishmentMeans table, update EM record value
<R><R>  elif scientificName is in EstablishmentMeans table (exact match), 
<R><R>  update EM record value


Actions
~~~~~~~
<R>* Liz/Derek will provide logins for Aimee to JIRA, Confluence
<R>* Shayne will export geometries from Postgres to shapefile
<R>* 8/5/2019: Aimee download GBIF data 
<R>* 9/30/2019: Aimee processed GBIF data (scripts written, tested, documented)
<R>* Other dates in Liz’s ppt 
<R><R>* ETA pre-Thanksgiving, dependent on KU hosted meeting, OS upgrade, Taiwan workshop: 
<R><R>* Aimee combined old and new BISON provider data
<R><R>* Aimee final processing complete 
<R><R> <R>* inc scripts written, tested, documented for ITIS names, geo, EEZ, EstablishmentMeans
<R><R> <R>* output: all data (GBIF + BISON provider) to BISON 47 column CSVs and 5 LUTs

Future
~~~~~~~
<R>* Possible IPT workflow: 
<R><R>* Liz uploads provider dataset to IPT, which checks (and sometimes corrects?) data
<R><R>* Aimee downloads corrected data from IPT, then continues editing
<R>* Update source data for geometries for next data load





