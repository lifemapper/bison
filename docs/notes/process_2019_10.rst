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

Process: (note LUT = lookup table)
--------------------------------------
    * Process GBIF data, mostly as 2018, with changes
        * Step 1: Create 2 LUTs prior to occ record processing
            * Resource LUT
                * Create list of dataset UUIDs from Dataset EML files
                * Create LUT and list of publishingOrganization UUIDs from 
                  GBIF API + dataset UUID
            * Provider LUT: from GBIF API + provider UUID 
        * Step 2: Process GBIF download to CSV file of GBIF data.  Temp result = step2_occ.csv
            * Edit values for fields:
                * Empty string --> null
                * Correct/standardize data values
                * If verbatimLocality is not null, BISON verbatim_locality = verbatimLocality
                  elif locality is not null, BISON verbatim_locality = locality
                  else BISON verbatim_locality = habitat
                  Question: Precedence b/w habitat/locality/verbatimLocality?
            * Fill Resource name, code, url, and publishingOrganizationKey 
              from datasetKey and Resource LUT 
            * Fill Provider name, code, url, etc 
              from publishingOrganizationKey and Provider LUT 
            * Discard records that fail for X reason
                * No scientificName or taxonKey
                * BISON provider or resource with url like bison.usgs.gov
                * QUESTION: discard with url like bison.ornl.gov?
            * Use ‘$’ delimiter in CSV output
            * Generate ScientificName/taxonKey list during data processing: 
        * Step 3: Create Name LUT after to occ record processing
            * CanonicalName: from GBIF parser + scientificName or taxonKey + API. 
        * Step 4: Process edited step2_occ CSV file
            * fill clean_provided_scientific_name from name LUT. 
            * Remove any temporary columns for final BISON 47 columns 
            
BISON 47 fields with contents from GBIF dump
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
   
   

