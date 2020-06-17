
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
       * GBIF.org (06 June 2020) GBIF Occurrence Download  

US Territories and Canada
  * http://www.gbif.org/occurrence/search?COUNTRY=AS&COUNTRY=CA&COUNTRY=FM&COUNTRY=GU&COUNTRY=MH&COUNTRY=MP&COUNTRY=PR&COUNTRY=PW&COUNTRY=UM&COUNTRY=VI 
  * GBIF.org (07 May 2020) GBIF Occurrence Download  
  
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

* GBIF download in DarwinCore format includes:
    * citations.txt  : references for downloaded data
    * rights.txt  : URLs for rights of each dataset represented
    * dataset  directory of EML files of each dataset represented
    * metadata.xml  : record count for each dataset represented
    * meta.xml  : list of index/fields (with URL for term) for each datafile
    * datafiles:
        * multimedia.txt  
        * occurrence.txt  
        * verbatim.txt

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

Prep: Resource and Provider LUT
======================================================
#. Merge 2 old provider and resource tables with current GBIF metadata
  
   * Input: existing provider and resource tables from BISON database
   * Input: dataset EML files from GBIF download
   * Merge these datasets, calling GBIF APIs to  Create Resource LUT and list of 
     publishingOrganization UUIDs from GBIF dataset API + dataset UUID.  
     Fill legacyid from existing resource table or dataset metadata. If legacyid
     does not exist for a new dataset, put default value (-9999) in the 
     table to indicate it does not exist.
   * Create Provider LUT from GBIF publisher API + publishingOrganization UUID.
     Fill legacyid from existing provider table or organization metadata. 
   * If a legacyid does not exist for a provider or resource, use the GBIF UUID
     for the organization or dataset respectively.


Step 1: Convert from GBIF fields to BISON field, fill Provider and Resource
============================================================================

#. Pull appropriate GBIF fields for BISON output, and minimal calculations 

  * Negate longitude if positive value on US and Canada points
  * basis_of_record: convert GBIF vals to BISON controlled vocabulary 
  * occurrence_date: format GBIF eventDate to YYYY-MM-DD if full date, or YYYY
  * Convert gbif eventDate to BISON occurrence_date in ISO 8601, or 4-digit year
    ex: 2018-08-01 or 2018
  * year: Use gbif/dwc year or pull from occurrence_date calc

#. Overall field checks:

  * Convert NA, #NA, N/A, None, NONE to ''
  * Remove quotes from around fields
  * Escape within-string quotes
  * Remove within-string delimiter ('$')
  * Strip leading or trailing invisible characters (space, tab)
  * IF thumb_URL=TRUE then associated_media=TRUE (2021 load)

#. Geo field checks:

  * Check 'COUNTRY' value is filled (pulling data by country, so it is)
  * If latitude/longitude, check they can be converted to float value, if one is 
    invalid, clear both
  * IF iso_country_code=US or CA then longitude=negative values

#. Fill Resource/Provider fields, discard BISON

   * Fill resource, resource_url, resource_id with dataset title, homepage/url,
     and legacyid from datasetKey and Resource LUT
   * Fill provider, provider_url, provider_id with organization title, 
     homepage/url from publishingOrganizationKey and Provider LUT 
     
#. Discards:

   * Discard records from BISON providers 
     Publisher: USGS, publishingOrganizationKey=c3ad790a-d426-4ac1-8e32-da61f81f0117 
     AND
     resource_url (from organization metadata) starts with https://bison.usgs.gov/ipt/resource?r=     
   * Discard records with no scientificName or taxonKey
   * Discard records with occurrenceStatus = absent

#. Replace some fields with controlled vocab, fill with alternate field values

   * NA, n/a, null, None --> ''
   * Correct/standardize basis_of_record to BISON controlled vocabulary
   * BISON verbatim_locality = either 1)verbatimLocality 2) locality or 3)habitat
   * BISON id = either 1) id or 2) collector_number

#. Save provided_scientific_name and taxonKey to file for name parsing or key lookup

Name/TaxonKey LUT preparation, after Step 1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Use the provided_scientific_name and taxonKey file to create a 
   name/taxonkey LUT for clean_provided_scientific_name.  Use GBIF name
   parser service on name first, taxonkey API if name parsing fails.
    
Step 2: Fill records with clean scientific name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Fill clean_provided_scientific_name from name LUT. 
#. Remove any temporary columns for final BISON 48 columns 

Steps 3 and 4
~~~~~~~~~~~~~
#. Process as Steps 3 and 4 in "GBIF and BISON provider common processing", 
   `BISON Data Load`_

Correct reference data
----------------------
#. Assemble US and Canada political boundaries into single simplified shapefile

Merge us_counties, can_counties:
B_STATE = if (PRNAME = 0, STATE_NAME, replace(PRNAME, ' Canada', ''))
B_COUNTY = if (PRNAME = 0, NAME, CDNAME)
B_FIPS = if (PRNAME = 0, concat(STATE_FIPS, CNTY_FIPS), CDUID)
B_CENTROID = geom_to_wkt (centroid( $geometry ) )

GBIF download, US, 6 June 2020
---------------------------------
https://www.gbif.org/occurrence/download/0080473-200221144449610

Citation
    GBIF.org (06 June 2020) GBIF Occurrence Download https://doi.org/10.15468/dl.ewy2wd 
License
    CC BY-NC 4.0 
File
    136 GB Darwin Core Archive 
Involved datasets
    3,024 

468,186,188 occurrences downloaded

GBIF download, US Territories and Canada, 7 May 2020
-----------------------------------------------------
http://api.gbif.org/v1/occurrence/download/request/0058175-200221144449610.zip

country=AS&country=CA&country=FM&country=GU&country=MH&country=MP&country=PR&country=PW&country=UM&country=VI

Total
    74,804,578 

Citation
    GBIF.org (07 May 2020) GBIF Occurrence Download https://doi.org/10.15468/dl.vacx3a 
License
    CC BY-NC 4.0 
File
    22 GB Darwin Core Archive 
Involved datasets
    2,186 
    
74,804,578 occurrences downloaded


provided_tsn - mod val:
Truncate gbifid 2251459304 field provided_tsn value http://www.boldsystems.org/index.php/Public_BarcodeCluster?clusteruri=BOLD:AAG4886 to width 64


