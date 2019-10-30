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

  * Using downloaded database?  https://www.itis.gov/downloads/index.html
  * or API?
  
* Step 1/2: Geo resolution

  * shapefiles from Shayne for point in polygons
  * World_EEZ_v8_20140228_splitpolygons.zip, using attributes MRGID & EEZEEZ: https://my.usgs.gov/jira/browse/BISA-763 
  * US Counties.zip for US and Canada: https://my.usgs.gov/jira/browse/BISA-1143 

           
BISON 48 fields with contents from GBIF dump
----------------------------------------------
#. clean_provided_scientific_name

   * Calc: 1) gbif name parser and scientificName OR 2) gbif species api and taxonKey
#. itis_common_name

   * Calc: from ITIS lookup (of itis_tsn calc)
#. itis_tsn 

   * Calc: with ITIS lookup + clean_provided_scientific_name
#. basis_of_record

   * gbif basisOfRecord + controlled vocabulary 
#. occurrence_date

   * gbif eventDate - formatted to YYYY-MM-DD if full date, or YYYY
#. year 

   * gbif year or pulled from occurrence_date calc
#. verbatim_event_date

   * gbif verbatimEventDate
#. provider

   * Q: 'BISON' constant or gbif institutionCode?
#. provider_url

   * Q: 'https://bison.usgs.gov' constant or gbif institutionID from organization metadata
#. resource

   * Calc: gbif dataset api + datasetKey, retrieve title (dataset name)
#. resource_url (https://bison.usgs.gov/ipt/resource?r= or other link) (DwC: collectionID)

   * Calc: gbif dataset api + datasetKey, retrieve homepage (dataset url)
#. occurrence_url

   * gbif occurrenceID
#. catalog_number

   * gbif catalogNumber
#. collector

   * gbif recordedBy
#. collector_number

   * gbif recordNumber
#. valid_accepted_scientific_name

   * Calc
   * Q: from ITIS lookup?
#. valid_accepted_tsn

   * Calc:
   * Q: from ITIS lookup? 
#. provided_scientific_name

   * Q: scientificName OR taxonRemarks?
#. provided_tsn

   * Calc:
   * Q: from ITIS lookup? or use GBIF taxonKey?
#. latitude

   * first pass: gbif decimalLatitude if exist and valid
   * second pass if missing: Calc: Georeference from 
#. longitude (DwC: decimalLongitude)

   * first pass: gbif decimalLongitude if exist and valid
   * second pass if missing: Calc: Georeference
#. verbatim_elevation

   * gbif verbatimElevation
#. verbatim_depth

   * gbif verbatimDepth
#. calculated_county_name

   * Calc: Georeference - coordinates + county polygons
#. calculated_fips

   * Calc: Georeference - coordinates + fips polygons
#. calculated_state_name

   * Calc: Georeference - coordinates + state polygons
#. centroid

   * Calc: georeferenceRemarks + Controlled vocab e.g. county = county centroid; zip code = zip code centroid; etc.)
   * Q: populate [only or also] if coordinates from Georeferencing to polygon?
#. provided_county_name

   * gbif county
#. provided_fips

   * gbif higherGeographyID
#. provided_state_name

   * gbif stateProvince
#. thumb_url

   * Q: ???
#. associated_media

   * gbif associatedMedia
#. associated_references

   * gbif associatedReferences
#. general_comments

   * gbif eventRemarks
#. id

   * Calc: 1) gbif occurrenceID or 2) gbif recordNumber 
#. provider_id

   * Calc: gbif publishingOrganizationKey from retrieved gbif dataset metadata 
#. resource_id

   * gbif datasetKey
#. provided_common_name

   * gbif vernacularName
#. kingdom

   * Q: gbif kingdom gbif kingdomKey+API or from ITIS calc?
#. geodetic_datum

   * gbif geodeticDatum
#. coordinate_precision

   * gbif coordinatePrecision
#. coordinate_uncertainty

   * gbif coordinateUncertaintyInMeters
#. verbatim_locality

   * gbif verbatimLocality
#. mrgid

   * Calc: after Georeference, polygon + coordinates
#. calculated_waterbody 

   * Calc: after Georeference geo, polygon + coordinates
#. establishment_means

   * Calc: after ITIS lookup, from establishmentMeans table + TSN
#. iso_country_code

   * gbif country
#. license

   * Q: gbif license OR constant 'http://creativecommons.org/publicdomain/zero/1.0/legalcode'?
   
   

(layerid >= 1490121 and layerid <= 1490260) or (layerid  >= 3785572 and layerid < =  3785583)

[root@yeti archive]# du -skh a*
15G     aemelton
3.3G    amelton
19M     amritesh
11M     anon
17M     aramoscabr
28M     asiel
[root@yeti archive]# du -skh b*
74M     beach53
1.4G    botany_demo
9.5K    bsenterre
[root@yeti archive]# du -skh c*
30M     camayal
414M    cjgrady
179M    cj_monday_tester
1.4G    cj_tuesday_demo
100M    cshl
[root@yeti archive]# du -skh e*
21M     ellienau
[root@yeti archive]# du -skh D*
16M     DANIELC
[root@yeti archive]# du -skh d*
17M     darunabas
1.6G    demo_user

