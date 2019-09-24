
.. highlight:: rest

Get and Process GBIF Data for BISON
===================================
.. contents::  


2018
=====


Download Data from GBIF 
-----------------------

* for US and US Territories
  * http://www.gbif.org/occurrence/search?COUNTRY=US
  * http://www.gbif.org/occurrence/search?COUNTRY=AS&COUNTRY=CA&COUNTRY=FM&COUNTRY=GU&COUNTRY=MH&COUNTRY=MP&COUNTRY=PR&COUNTRY=PW&COUNTRY=UM&COUNTRY=VI 


* Run portaldownload script to edit, filter and format data into a CSV file

* GBIF download includes:
  * citations.txt  : references for downloaded data
  * rights.txt  : URLs for rights of each dataset represented
  * dataset  directory of EML files of each dataset represented
  * metadata.xml  : record count for each dataset represented
  * meta.xml  : list of index/fields (with URL for term) for each datafile
  * datafiles:
    * multimedia.txt  
    * occurrence.txt  
    * verbatim.txt
    
Dependencies
-------------
* python2.7  (badenov /usr/bin/python)
* unicodecsv: 
  * Centos7: yum search --enablerepo base unicodecsv
  * https://pypi.org/project/unicodecsv/


Steps 2019
-----------

* On smaller, CA/Territories file
  * sort datafiles (multimedia.txt, occurrence.txt, verbatim.txt) on gbifID

Liz's email
-----------

* Initial extract of requested GBIF data fields based on our 11 
  iso_country_codes e.g. AS,CA, FM, GU, MH, MP, PR, PW, UM, US, and VI
  (see Pg 20 of BISON Data Workflow (July 3. 2018).pdf)
* Remove all records from the BISON Provider (440, or 362?, use UUID) 
  to avoid duplication in BISON
* Run scientificName values through either the Python script (attached 
  pyGbifLoad.zip) OR GBIF name parser (https://www.gbif.org/tools/name-parser) 
  and replace with resulting canonicalName values (we want the original clean 
  scientific name submitted by the Data Providers sans taxon author and year; 
  rather than GBIF's 'interpreted' scientificName values)
* Remove all records with scientificName=blank
* Remove all records with occurrenceStatus=absent 
* For records with 0,0 coordinates - change any decimalLatitude and 
  decimalLongitude field '0' (zero) values to null/blank (we/BISON may still 
  be able to georeference these records)
  
* Convert GBIF basisOfRecord (http://rs.tdwg.org/dwc/terms/#basisOfRecord) 
  values to simpler BISON values 
  e.g. humanObservation and machineObservation=observation; 
  FossilSpecimen=fossil, LivingSpecimen=living;... 
  
* Provider/publisher/organization, dataset/resource/collection, 

   * None of these should ever be blank::
  
      *  Provider/publisher/organization
          * providerID (numeric code)
          * institutionCode (text string/name of provider)
    
      * Dataset/resource/collection
          * resourceID (numeric code)
          * ownerInstitutionCode (text string/name of dataset)

   * These are sometimes blank:

      * Provider/publisher/organization
         * institutionID (Provider's organizational URL - not a GBIF URL)
    
      * Dataset/resource/collection
         * collectionID (Dataset's URL if on the Web elsewhere - not a GBIF URL)



Overall FIELD CHECKS
-----------------------

* CHECK FOR NA, #NA, N/A AND REMOVE/REPLACE AS NEEDED (BLANK FIELDS SHOULD NOT CONTAIN ANY CHARACTERS)
* CHECK TEXT/STRING FIELDS FOR UNACCEPTABLE SPECIAL CHARACTERS OR MISINTERPRETED (E.G. UTF8) CHARACTERS
* CHECK THAT ALL FORMULAS (e.g. MSExcel) HAVE BEEN REPLACED WITH VALUES
* CHECK FOR LEADING OR TRAILING SPACES OR TABS
* CHECK FOR AT LEAST 'COUNTRY' GEOGRAPHIC INFORMATION IN EACH RECORD
* IF thumb_URL=TRUE then associated_media=TRUE
* IF latitude=TRUE then longitude=TRUE
* IF iso_country_code=US or CA then longitude=negative values
* IF latitude AND longitude=FALSE then provided_county_name or provided_state_name=TRUE

Field list and order
-----------------------

1. clean_provided_scientific_name (DwC:scientificName)
1. itis_common_name (Calculated, usually not mapped to DwC because this field is 
   populated during post-processing. But if populated could use: vernacularName)
1. itis_tsn (Calculated, usually not mapped to DwC because this field is 
   populated during post-processing. But if populated could use: DwC: taxonID 
   and nameAccordingToID with value of the latter set to "Integrated Taxonomic 
   Information System (ITIS). https://www.itis.gov/")
1. basis_of_record (Controlled vocab) (DwC: basisOfRecord)
1. occurrence_date (YYYY-MM-DD) DwC: eventDate) *Proposed for renaming to 
   'event_date' for consistency across BISON Data Schema
1. year (YYYY) DwC: year)
1. verbatim_event_date (DwC: verbatimEventDate) *Added to BISON Data Schema FY16/17
1. provider (BISON) (DwC: institutionCode)
1. provider_url (https://bison.usgs.gov)(DwC: institutionID)
1. resource (dataset name) (DwC: collectionCode & datasetName)
1. resource_url (https://bison.usgs.gov/ipt/resource?r= or other link) 
   (DwC: collectionID)
1. occurrence_url (DwC: occurrenceID or IPT: occurrenceDetails)
1. catalog_number (DwC: catalogNumber)
1. collector (DwC: recordedBy) (DwC: recordedBy)
1. collector_number (DwC: recordNumber)
1. valid_accepted_scientific_name (Calculated. But could use: 
   DwC: acceptedNameUsage)
1. valid_accepted_tsn (Calculated. But could use DwC:taxonID if not already 
   mapped to itis_tsn; or DwC: acceptedNameUsageID)
1. provided_scientific_name (DwC: taxonRemarks)
1. provided_tsn (DwC: taxonID if not already mapped to itis_tsn; and 
   nameAccordingToID with value of the latter set to "Integrated Taxonomic 
   Information System (ITIS). http://www.itis.gov/")
1. latitude (DwC: decimalLatitude)
1. longitude (DwC: decimalLongitude)
1. verbatim_elevation (DwC: verbatimElevation)
1. verbatim_depth (DwC: verbatimDepth)
1. calculated_county_name (Calculated, DwC: n/a)
1. calculated_fips (Calculated, DwC: n/a)
1. calculated_state_name (Calculated, DwC: n/a)
1. centroid (Controlled vocab) (DwC: georeferenceRemarks WITH a 'Translation' 
   e.g. county = county centroid; zip code = zip code centroid; etc.)
1. provided_county_name (DwC: county)
1. provided_fips (DwC: higherGeographyID)
1. provided_state_name (DwC: stateProvince)
1. thumb_url (DwC: n/a)
1. associated_media (DwC: associatedMedia)
1. associated_references (DwC: associatedReferences)
1. general_comments (DwC: eventRemarks)
1. id (DwC: occurrenceID or to recordNumber IF NO Collector Number!)
1. provider_id (440) (DwC: n/a)
1. resource_id (Could be mapped to DwC: datasetID)
1. provided_common_name (DwC: vernacularName)
1. kingdom (ITIS controlled vocab) (DwC: kingdom) *Re-labeled for DwC and 
   BISON Data Schema consistency
1. geodetic_datum (DwC: geodeticDatum)
1. coordinate_precision (DwC: coordinatePrecision)
1. coordinate_uncertainty (DwC: coordinateUncertaintyInMeters)
1. verbatim_locality (DwC: verbatimLocality)
1. mrgid (DwC: n/a) *added to BISON Data Schema FY16/17 (added and populated 
   by Dev team during data ingest; no blank column necessary in BISON-munged datasets)
1. calculated_waterbody (DwC: waterBody) *added to BISON Data Schema FY16/17 
   (added and populated by Dev team during data ingest; no blank column 
   necessary in BISON-munged datasets)
1. establishment_means (DwC: establishmentMeans WITH a 'Translation' 
   e.g. AK = nonnative in Alaska; HI = nonnative in Hawaii; L48 =
1. nonnative in the contiguus United States (CONUS); **Be sure to provide a 
   translation for any unique combination of these values that
1. appears in your dataset) *added to BISON Data Schema FY18 (added and 
   populated by Dev team during data ingest; no blank column necessary in 
   BISON-munged datasets)
1. iso_country_code (Controlled vocab) (DwC: country & countryCode, unless 
   there is a separate country name field)
1. license (http://creativecommons.org/publicdomain/zero/1.0/legalcode) 
   (DwC: license) *added to BISON Data Schema FY16/17 (added and populated by 
   Dev team during data ingest; no blank column necessary in BISON-munged 
   datasets)
   
   