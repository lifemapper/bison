BISON data load Oct 2019
=======================

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

Process: (LUT = lookup table)
-----------------------------
Process GBIF data, mostly as 2018, with changes

* Step 1: Create 2 LUTs prior to occ record processing

  * Resource LUT
    
    * Create list of dataset UUIDs from Dataset EML files
    * Create LUT and list of publishingOrganization UUIDs from 
      GBIF API + dataset UUID
  * Provider LUT: from GBIF API + provider UUID 
    
* Step 2: Process GBIF download to CSV file of GBIF data.  Temp result = step2_occ.csv

  * Edit values for fields:
    
    * NA, n/a, null --> ''
    * Correct/standardize data values
    * BISON verbatim_locality = either 1)verbatimLocality 2) locality or 3)habitat
          
  * Fill Resource name, code, url, and publishingOrganizationKey 
    from datasetKey and Resource LUT 
  * Fill Provider name, code, url, etc 
    from publishingOrganizationKey and Provider LUT 
  * Discard records that fail for X reason
    
    * No scientificName or taxonKey
    * BISON provider or resource with url containing 'bison.' 
    * QUESTION: discard with url like bison.ornl.gov?
        
  * Use ‘$’ delimiter in CSV output
  * Generate ScientificName/taxonKey list during data processing: 
    
* Step 3: Create Name LUT after to occ record processing

  * CanonicalName: from GBIF parser + scientificName or taxonKey + API. 
    
* Step 4: Process edited step2_occ CSV file

  * fill clean_provided_scientific_name from name LUT. 
  * Remove any temporary columns for final BISON 47 columns 
  
  
New things 
----------
To be done on BISON 48, either from GBIF or data providers,
process info at: https://my.usgs.gov/confluence/display/DEV/SAS+Development

* Step 1/2: ITIS lookup 
  
  * Lookup TSN from clean_provided_scientific_name to get:

    * itis_common_name
    * itis_tsn
    * valid_accepted_scientific_name
    * valid_accepted_tsn
    * if blank, get kingdom

  * Process: 
  
    * Use downloaded database?  From https://www.itis.gov/downloads/index.html
      or provided by ITIS developers
    * Use API?
  
* Step 1/2: Geo lookup

  * shapefiles from Shayne for point in polygons:

    * US Counties.zip for US and Canada: https://my.usgs.gov/jira/browse/BISA-1143 
    * World_EEZ_v8_20140228_splitpolygons.zip, using attributes MRGID & EEZEEZ: 
      https://my.usgs.gov/jira/browse/BISA-763 
      Shayne says: "I think we will always need to check EEZ as there is some 
      overlap with the other layers"

  * Process
  
    * Check (point in poly) USCounties to get US County, State, FIPs or CA values
    * Check (point in poly) Marine EEZ for waterbody (buffered) names

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

           
BISON 48 fields with raw or calculated values for GBIF-provided data
--------------------------------------------------------------------
#. clean_provided_scientific_name

   * Calc: 1) gbif name parser and scientificName OR 
     2) gbif species api and taxonKey
           
#. itis_common_name

   * Calc: from ITIS lookup (of itis_tsn calc)
   
#. itis_tsn 

   * Calc: with ITIS lookup + clean_provided_scientific_name

#. basis_of_record

   * gbif/dwc basisOfRecord + controlled vocabulary 

#. occurrence_date

   * gbif/dwc eventDate - formatted to YYYY-MM-DD if full date, or YYYY

#. year 

   * gbif/dwc year or pulled from occurrence_date calc

#. verbatim_event_date

   * gbif/dwc verbatimEventDate

#. provider

   * gbif/dwc institutionCode (2018: title from gbif organization metadata?)

#. provider_url

   * gbif/dwc institutionID (2018: homepage from gbif organization metadata?)

#. resource

   * Calc: dataset api + datasetKey, retrieve title (dataset name)

#. resource_url (https://bison.usgs.gov/ipt/resource?r= or other link) (DwC: collectionID)

   * Calc: gbif dataset api + datasetKey, retrieve homepage (dataset url)
   
#. occurrence_url

   * gbif/dwc occurrenceID
   
#. catalog_number

   * gbif/dwc catalogNumber
   
#. collector

   * gbif/dwc recordedBy
   
#. collector_number

   * gbif/dwc recordNumber
   
#. valid_accepted_scientific_name

   * Calc from ITIS lookup

#. valid_accepted_tsn

   * Calc from ITIS lookup

#. provided_scientific_name

   * original gbif/dwc scientificName (AMS: later, check verbatim file)

#. provided_tsn

   * gbif/dwc taxonID

#. latitude

   * first pass: gbif/dwc decimalLatitude if exist and valid
   * second pass if missing: Calc: Geo lookup from centroids of smallest 
     enclosing polygon in provided shapefiles

#. longitude (DwC: decimalLongitude)

   * first pass: gbif/dwc decimalLongitude if exist and valid
   * second pass if missing: Calc: Geo lookup from centroids of smallest 
     enclosing polygon in provided shapefiles
   
#. verbatim_elevation

   * gbif/dwc verbatimElevation
   
#. verbatim_depth

   * gbif/dwc verbatimDepth
   
#. calculated_county_name

   * Calc: Geo lookup - coordinates + county polygons
   
#. calculated_fips

   * Calc: Geo lookup - coordinates + fips polygons
   
#. calculated_state_name

   * Calc: Geo lookup - coordinates + state polygons
   
#. centroid

   * Calc: populate if coordinates calculated from Geo lookup to polygon
   * Do not overwrite existing values in BISON-provided datasets
   
#. provided_county_name

   * gbif/dwc county
   
#. provided_fips

   * gbif/dwc higherGeographyID
   
#. provided_state_name

   * gbif/dwc stateProvince
   
#. thumb_url

   * ignore
   
#. associated_media

   * not present in gbif occurrence.txt (next, get from verbatim.txt)
   
#. associated_references

   * gbif/dwc associatedReferences
   
#. general_comments

   * gbif/dwc eventRemarks
   
#. id

   * Calc: gbif/dwc 1) occurrenceID or 2) recordNumber 

#. provider_id

   * Calc: gbif publishingOrganizationKey from retrieved gbif dataset metadata 
   
#. resource_id

   * gbif/gbif datasetKey
   
#. provided_common_name

   * gbif/dwc vernacularName
   
#. kingdom

   * gbif/dwc kingdom is blank, resolve with ITIS calc
   
#. geodetic_datum

   * not present in GBIF occurrence.txt (AMS: next, parse from another field 
     which includes 'GEODETIC_DATUM_ASSUMED' or get from verbatim.txt)

#. coordinate_precision

   * gbif/dwc coordinatePrecision
   
#. coordinate_uncertainty

   * gbif/dwc coordinateUncertaintyInMeters
   
#. verbatim_locality

   * Calc: gbif/dwc 1) verbatimLocality 2) locality 3) habitat
   
#. mrgid

   * Calc: after Geo lookup, polygon + coordinates
   
#. calculated_waterbody 

   * Calc: after Geo lookup geo, polygon + coordinates
   
#. establishment_means

   * Calc: after ITIS lookup, from establishmentMeans table + itis_tsn
     (now or later? if not itis_tsn, calc from establishmentMeans table + 
     clean_provided_scientific_name)
   
#. iso_country_code

   * gbif/dwc countryCode
   
#. license

   * gbif/dc license 
   


