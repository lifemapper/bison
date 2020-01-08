
.. highlight:: rest

GBIF data load 2019/2020
=======================
.. contents::  

.. _BISON Data Load: docs/notes/dataload_2019-2020.rst

For overview please see `BISON Data Load`_

Download Data from GBIF 
-----------------------

US
  * http://www.gbif.org/occurrence/search?COUNTRY=US
       * GBIF.org (15 October 2019) GBIF Occurrence Download https://doi.org/10.15468/dl.zqo0nh 


US Territories and Canada
  * http://www.gbif.org/occurrence/search?COUNTRY=AS&COUNTRY=CA&COUNTRY=FM&COUNTRY=GU&COUNTRY=MH&COUNTRY=MP&COUNTRY=PR&COUNTRY=PW&COUNTRY=UM&COUNTRY=VI 
  * GBIF.org (15 October 2019) GBIF Occurrence Download https://doi.org/10.15468/dl.5qoflq 
  * GBIF.org (06 December 2019) GBIF Occurrence Download https://doi.org/10.15468/dl.trksri 
  
    * American Samoa 
    * Canada 
    * Micronesia, Federated States of 
    * Guam 
    * Marshall Islands 
    * Northern Mariana Islands 
    * Puerto Rico 
    * Palau 
    * United States Minor Outlying Islands 
    * Virgin Islands, U.S. 

GBIF data for BISON ingest
--------------------------
Note data comes from GBIF darwin core download, including:

* dataset eml files with datasetKeys (bison resource) for included data.
  dataset files include providingOrganizationKeys (bison provider) for the 
  dataset.  Use GBIF APIs with datasetKey and providingOrganizationKey
  to populate bison resource and provider fields.
* occurrence.txt file with records to ingest
* Fields reference gbif column names in included meta.xml file.  

  * gbif/gbif = gbif column, gbif field definition
  * gbif/dwc = gbif column, darwin core field definition
  * gbif/dc = gbif column, dublin core field definition

* Not currently used: verbatim.txt file.  These data had errors previously, 
  they seem to be fixed now.  Next time, will pull some fields from these 
  records.

Process: (LUT = lookup table)
-----------------------------
Prepare lookup tables and process GBIF records into BISON records.  Iterate 
through all records three times, to satisfy data dependencies in later steps.

Resource and Provider LUT preparation, before Iteration 1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Create 2 lookup tables (LUTs) then initial process occ records from GBIF to BISON, 
  
    * Create list of dataset UUIDs from Dataset EML files
    * Create Resource LUT and list of publishingOrganization UUIDs from 
      GBIF dataset API + dataset UUID
    * Create Provider LUT from GBIF publisher API + publishingOrganization UUID 

Process Records Iteration 1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Fill Resource/Provider fields from LUTs and 

    * Fill Resource name, code, url, and publishingOrganizationKey, discarding USGS records
      from datasetKey and Resource LUT 
    * Fill Provider name, code, url, etc 
      from publishingOrganizationKey and Provider LUT 
    * Discard records from BISON providers 
      Publisher: USGS, publishingOrganizationKey=c3ad790a-d426-4ac1-8e32-da61f81f0117 
      AND
      resource_url (from organization metadata) starts with https://bison.usgs.gov/ipt/resource?r=
     
#. Discard records with no scientificName or taxonKey
        
#. Replace some fields with controlled vocab, fill with alternate field values

    * NA, n/a, null --> ''
    * Correct/standardize basis_of_record to BISON controlled vocabulary
    * BISON verbatim_locality = either 1)verbatimLocality 2) locality or 3)habitat
    * BISON id = either 1) id or 2) collector_number

#. Test for float values in longitude and latitude; if one is invalid, clear both
#. Convert gbif eventDate to BISON occurrence_date in ISO 8601, ex: 2018-08-01 or 2018
#. Save provided_scientific_name and taxonKey to file for name parsing or key lookup

Name/TaxonKey LUT preparation, after Iteration 1, before 2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Create name/taxonkey LUT for clean_provided_scientific_name, using GBIF name
   parser service on name first, taxonkey API if name parsing fails.
   process occ records to replace names
    
Process Records Iteration 2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. fill clean_provided_scientific_name from name LUT. 
#. Remove any temporary columns for final BISON 48 columns 

Process Records Iteration 3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Process as "GBIF and BISON provider common processing" in `BISON Data Load`_
