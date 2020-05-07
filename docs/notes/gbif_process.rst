
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
  
   * Get existing provider and resource tables from BISON database
   * Create list of dataset UUIDs from Dataset EML files
   * Create Resource LUT and list of publishingOrganization UUIDs from 
     GBIF dataset API + dataset UUID.  Fill legacyid from existing resource 
     table or dataset metadata.
   * Create Provider LUT from GBIF publisher API + publishingOrganization UUID.
     Fill legacyid from existing provider table or organization metadata. 

Process Records Iteration 1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Fill Resource/Provider fields from LUTs and 

   * Fill resource, resource_url, resource_id with dataset title, homepage/url,
     and legacyid from datasetKey and Resource LUT 
   * Fill provider, provider_url, provider_id with organization title, 
     homepage/url from publishingOrganizationKey and Provider LUT 
   * Discard records from BISON providers 
     Publisher: USGS, publishingOrganizationKey=c3ad790a-d426-4ac1-8e32-da61f81f0117 
     AND
     resource_url (from organization metadata) starts with https://bison.usgs.gov/ipt/resource?r=
     
#. Discard records with no scientificName or taxonKey
#. Discard records with occurrenceStatus = absent
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

Correct reference data
----------------------
#. Assemble US and Canada political boundaries into single simplified shapefile

Merge us_counties, can_counties:
B_STATE = if (PRNAME = 0, STATE_NAME, replace(PRNAME, ' Canada', ''))
B_COUNTY = if (PRNAME = 0, NAME, CDNAME)
B_FIPS = if (PRNAME = 0, concat(STATE_FIPS, CNTY_FIPS), CDUID)
B_CENTROID = geom_to_wkt (centroid( $geometry ) )

GBIF download, country=US, 7 May 2020
---------------------------------------

Total
    467,237,837 
License
    CC BY-NC 4.0 
Year range
    1601–2020 
With year
    98 % 
With coordinates
    96 % 
With taxon match
    99.7 % 

Known issues

A part of the GBIF processing is to flag occurrences that have suspicious fields
* 10,859,178 Geodetic datum invalid 
* 7,645,032 References uri invalid 
* 4,485,870 Country derived from coordinates 
* 3,325,294 Taxon match higherrank 
* 1,196,123 Taxon match none 
* 1,115,291 Type status invalid 
* 1,066,422 Recorded date invalid 
* 1,005,128 Taxon match fuzzy 
* 919,444 Country invalid 
* 836,609 Recorded date mismatch 
* 819,170 Coordinate precision invalid 
* 473,785 Basis of record invalid 
* 452,631 Continent invalid 
* 338,872 Coordinate invalid 
* 292,935 Coordinate uncertainty meters invalid 
* 259,812 Individual count invalid 
* 131,893 Country coordinate mismatch 
* 59,274 Depth min/max swapped 
* 59,225 Presumed negated longitude 
* 44,041 Depth unlikely 
* 29,918 Zero coordinate 
* 27,743 Depth non numeric 
* 22,758 Modified date unlikely 
* 17,696 Recorded date unlikely 
* 9,798 Identified date unlikely 
* 8,499 Elevation min/max swapped 
* 7,469 Multimedia uri invalid 
* 5,745 Depth not metric 
* 4,017 Elevation non numeric 
* 3,973 Coordinate reprojection suspicious 
* 3,570 Coordinate out of range 
* 2,347 Elevation not metric 
* 971 Country mismatch 
* 884 Presumed negated latitude 
* 381 Multimedia date invalid 
* 339 Presumed swapped coordinate

Fossils
There are fossils among your results. That can mean species occurrences at unexpected locations
Living specimens
Your search includes living specimens such as occurrences in botanical and zoological gardens.


GBIF download, US Territories and Canada, 7 May 2020
-----------------------------------------------------

country=AS&country=CA&country=FM&country=GU&country=MH&country=MP&country=PR&country=PW&country=UM&country=VI

Total
    74,804,578 
License
    CC BY-NC 4.0 
Year range
    1601–2020 
With year
    99 % 
With coordinates
    97 % 
With taxon match
    99.6 % 

Known issues

A part of the GBIF processing is to flag occurrences that have suspicious fields
* 3,855,657 Country derived from coordinates
* 1,994,390 Geodetic datum invalid 
* 1,821,473 Taxon match higherrank 
* 1,510,152 Coordinate precision invalid 
* 1,444,710 References uri invalid 
* 496,677 Basis of record invalid 
* 282,489 Taxon match none 
* 279,271 Continent invalid 
* 135,652 Taxon match fuzzy 
* 120,860 Recorded date invalid 
* 80,724 Recorded date mismatch 
* 53,250 Coordinate invalid 
* 52,561 Country coordinate mismatch 
* 21,419 Country mismatch 
* 19,792 Type status invalid 
* 15,319 Coordinate uncertainty meters invalid 
* 13,171 Presumed negated longitude 
* 12,997 Zero coordinate 
* 11,711 Country invalid 
* 7,983 Identified date unlikely 
* 7,106 Individual count invalid 
* 4,011 Depth min/max swapped 
* 3,162 Depth non numeric 
* 2,358 Coordinate out of range 
* 1,637 Multimedia uri invalid 
* 1,164 Depth unlikely 
* 1,014 Recorded date unlikely 
* 939 Presumed negated latitude 
* 858 Coordinate reprojection suspicious 
* 815 Modified date unlikely 
* 759 Elevation min/max swapped 
* 655 Depth not metric 
* 440 Presumed swapped coordinate 
* 417 Elevation non numeric 
* 151 Multimedia date invalid 
* 42 Coordinate reprojection failed 
* 31 Elevation not metric

Fossils
There are fossils among your results. That can mean species occurrences at unexpected locations
Living specimens
Your search includes living specimens such as occurrences in botanical and zoological gardens.
