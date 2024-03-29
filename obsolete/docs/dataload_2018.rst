
.. highlight:: rest

Get and Process GBIF Data for BISON: 2018
===========================================
.. contents::


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

Data Transformations
=====================

Liz's email
------------

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
* Provider/publisher/organization, dataset/resource/collection
     * Computed fields:
          *  Provider/publisher/organization
               * providerID (required; numeric code in GBIF publisher field)
               * institutionCode (required; text string/name of provider, pulled from provider API)
               * institutionID (may be blank; Provider's organizational URL, pulled from provider API - not a GBIF URL)

          * Dataset/resource/collection
               * resourceID (required; numeric code in GBIF datasetKey field)
               * ownerInstitutionCode (required; text string/name of dataset, pulled from dataset API)
               * collectionID (may be blank; Dataset's URL if on the Web elsewhere, pulled from dataset API - not a GBIF URL)



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

Field list and order, required 47
---------------------------------

#. clean_provided_scientific_name (DwC:scientificName)
#. itis_common_name (Calculated, usually not mapped to DwC because this field is populated during post-processing. But if populated could use: vernacularName)
#. itis_tsn (Calculated, usually not mapped to DwC because this field is populated during post-processing. But if populated could use: DwC: taxonID and nameAccordingToID with value of the latter set to "Integrated Taxonomic Information System (ITIS). https://www.itis.gov/")
#. basis_of_record (Controlled vocab) (DwC: basisOfRecord)
#. occurrence_date (YYYY-MM-DD) DwC: eventDate) *Proposed for renaming to 'event_date' for consistency across BISON Data Schema
#. year (YYYY) DwC: year)
#. verbatim_event_date (DwC: verbatimEventDate) *Added to BISON Data Schema FY16/17
#. provider (BISON) (DwC: institutionCode)
#. provider_url (https://bison.usgs.gov)(DwC: institutionID)
#. resource (dataset name) (DwC: collectionCode & datasetName)
#. resource_url (https://bison.usgs.gov/ipt/resource?r= or other link) (DwC: collectionID)
#. occurrence_url (DwC: occurrenceID or IPT: occurrenceDetails)
#. catalog_number (DwC: catalogNumber)
#. collector (DwC: recordedBy) (DwC: recordedBy)
#. collector_number (DwC: recordNumber)
#. valid_accepted_scientific_name (Calculated. But could use: DwC: acceptedNameUsage)
#. valid_accepted_tsn (Calculated. But could use DwC:taxonID if not already mapped to itis_tsn; or DwC: acceptedNameUsageID)
#. provided_scientific_name (DwC: taxonRemarks)
#. provided_tsn (DwC: taxonID if not already mapped to itis_tsn; and nameAccordingToID with value of the latter set to "Integrated Taxonomic Information System (ITIS). http://www.itis.gov/")
#. latitude (DwC: decimalLatitude)
#. longitude (DwC: decimalLongitude)
#. verbatim_elevation (DwC: verbatimElevation)
#. verbatim_depth (DwC: verbatimDepth)
#. calculated_county_name (Calculated, DwC: n/a)
#. calculated_fips (Calculated, DwC: n/a)
#. calculated_state_name (Calculated, DwC: n/a)
#. centroid (Controlled vocab) (DwC: georeferenceRemarks WITH a 'Translation' e.g. county = county centroid; zip code = zip code centroid; etc.)
#. provided_county_name (DwC: county)
#. provided_fips (DwC: higherGeographyID)
#. provided_state_name (DwC: stateProvince)
#. thumb_url (DwC: n/a)
#. associated_media (DwC: associatedMedia)
#. associated_references (DwC: associatedReferences)
#. general_comments (DwC: eventRemarks)
#. id (DwC: occurrenceID or to recordNumber IF NO Collector Number!)
#. provider_id (440) (DwC: n/a)
#. resource_id (Could be mapped to DwC: datasetID)
#. provided_common_name (DwC: vernacularName)
#. kingdom (ITIS controlled vocab) (DwC: kingdom) *Re-labeled for DwC and BISON Data Schema consistency
#. geodetic_datum (DwC: geodeticDatum)
#. coordinate_precision (DwC: coordinatePrecision)
#. coordinate_uncertainty (DwC: coordinateUncertaintyInMeters)
#. verbatim_locality (DwC: verbatimLocality)
#. mrgid (DwC: n/a) *added to BISON Data Schema FY16/17 (added and populated by Dev team during data ingest; no blank column necessary in BISON-munged datasets)
#. calculated_waterbody (DwC: waterBody) *added to BISON Data Schema FY16/17 (added and populated by Dev team during data ingest; no blank column necessary in BISON-munged datasets)
#. establishment_means

   * (DwC: establishmentMeans WITH a 'Translation' e.g. AK = nonnative in Alaska; HI = nonnative in Hawaii; L48 =  US Lower 48 states )
   * nonnative in the contiguus United States (CONUS);
   * Be sure to provide a translation for any unique combination of these values that appears in your dataset)
   * added to BISON Data Schema FY18 (added and populated by Dev team during data ingest; no blank column necessary in BISON-munged datasets)

#. iso_country_code (Controlled vocab) (DwC: country & countryCode, unless there is a separate country name field)
#. license (http://creativecommons.org/publicdomain/zero/1.0/legalcode)  (DwC: license) *added to BISON Data Schema FY16/17 (added and populated by Dev team during data ingest; no blank column necessary in BISON-munged datasets)
