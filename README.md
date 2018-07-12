
.. highlight:: rest

Get and Process GBIF Data for BISON
===================================
.. contents::  


Download Data from GBIF 
-----------------------

* for US and US Territories
  * http://www.gbif.org/occurrence/search?COUNTRY=US
  * http://www.gbif.org/occurrence/search?COUNTRY=AS&COUNTRY=CA&COUNTRY=FM&COUNTRY=GU&COUNTRY=MH&COUNTRY=MP&COUNTRY=PR&COUNTRY=PW&COUNTRY=UM&COUNTRY=VI 


* Run portaldownload script to edit, filter and format data into a CSV file


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
#. clean_provided_scientific_name (DwC:scientificName)
itis_common_name (Calculated, usually not mapped to DwC because this field is populated during post-processing. But if populated
could use: vernacularName)
itis_tsn (Calculated, usually not mapped to DwC because this field is populated during post-processing. But if populated could use:
DwC: taxonID and nameAccordingToID with value of the latter set to "Integrated Taxonomic Information System (ITIS). https://www.itis.
gov/")
basis_of_record (Controlled vocab) (DwC: basisOfRecord)
occurrence_date (YYYY-MM-DD) DwC: eventDate) *Proposed for renaming to 'event_date' for consistency across BISON Data Schema
year (YYYY) DwC: year)
verbatim_event_date (DwC: verbatimEventDate) *Added to BISON Data Schema FY16/17
provider (BISON) (DwC: institutionCode)
provider_url (https://bison.usgs.gov)(DwC: institutionID)
resource (dataset name) (DwC: collectionCode & datasetName)
resource_url (https://bison.usgs.gov/ipt/resource?r= or other link) (DwC: collectionID)
occurrence_url (DwC: occurrenceID or IPT: occurrenceDetails)
catalog_number (DwC: catalogNumber)
collector (DwC: recordedBy) (DwC: recordedBy)
collector_number (DwC: recordNumber)
valid_accepted_scientific_name (Calculated. But could use: DwC: acceptedNameUsage)
valid_accepted_tsn (Calculated. But could use DwC:taxonID if not already mapped to itis_tsn; or DwC: acceptedNameUsageID)
Overall FIELD CHECKS
CHECK FOR NA, #NA, N/A AND REMOVE/REPLACE AS NEEDED (BLANK FIELDS SHOULD NOT CONTAIN ANY
CHARACTERS)
CHECK TEXT/STRING FIELDS FOR UNACCEPTABLE SPECIAL CHARACTERS OR MISINTERPRETED (E.G. UTF8)
CHARACTERS
CHECK THAT ALL FORMULAS (e.g. MSExcel) HAVE BEEN REPLACED WITH VALUES
CHECK FOR LEADING OR TRAILING SPACES OR TABS
CHECK FOR AT LEAST 'COUNTRY' GEOGRAPHIC INFORMATION IN EACH RECORD
IF thumb_URL=TRUE then associated_media=TRUE
IF latitude=TRUE then longitude=TRUE
IF iso_country_code=US or CA then longitude=negative values
IF latitude AND longitude=FALSE then provided_county_name or provided_state_name=TRUE
2
18.
19.
20.
21.
22.
23.
24.
25.
26.
27.
28.
29.
30.
31.
32.
33.
34.
35.
36.
37.
38.
39.
40.
41.
42.
43.
44.
45.
46.
47.
48.
provided_scientific_name (DwC: taxonRemarks)
provided_tsn (DwC: taxonID if not already mapped to itis_tsn; and nameAccordingToID with value of the latter set to "Integrated
Taxonomic Information System (ITIS). http://www.itis.gov/")
latitude (DwC: decimalLatitude)
longitude (DwC: decimalLongitude)
verbatim_elevation (DwC: verbatimElevation)
verbatim_depth (DwC: verbatimDepth)
calculated_county_name (Calculated, DwC: n/a)
calculated_fips (Calculated, DwC: n/a)
calculated_state_name (Calculated, DwC: n/a)
centroid (Controlled vocab) (DwC: georeferenceRemarks WITH a 'Translation' e.g. county = county centroid; zip code = zip code
centroid; etc.)
provided_county_name (DwC: county)
provided_fips (DwC: higherGeographyID)
provided_state_name (DwC: stateProvince)
thumb_url (DwC: n/a)
associated_media (DwC: associatedMedia)
associated_references (DwC: associatedReferences)
general_comments (DwC: eventRemarks)
id (DwC: occurrenceID or to recordNumber IF NO Collector Number!)
provider_id (440) (DwC: n/a)
resource_id (Could be mapped to DwC: datasetID)
provided_common_name (DwC: vernacularName)
kingdom (ITIS controlled vocab) (DwC: kingdom) *Re-labeled for DwC and BISON Data Schema consistency
geodetic_datum (DwC: geodeticDatum)
coordinate_precision (DwC: coordinatePrecision)
coordinate_uncertainty (DwC: coordinateUncertaintyInMeters)
verbatim_locality (DwC: verbatimLocality)
mrgid (DwC: n/a) *added to BISON Data Schema FY16/17 (added and populated by Dev team during data ingest; no blank column
necessary in BISON-munged datasets)
calculated_waterbody (DwC: waterBody) *added to BISON Data Schema FY16/17 (added and populated by Dev team during data ingest;
no blank column necessary in BISON-munged datasets)
establishment_means (DwC: establishmentMeans WITH a 'Translation' e.g. AK = nonnative in Alaska; HI = nonnative in Hawaii; L48 =
nonnative in the contiguus United States (CONUS); **Be sure to provide a translation for any unique combination of these values that
appears in your dataset) *added to BISON Data Schema FY18 (added and populated by Dev team during data ingest; no blank column
necessary in BISON-munged datasets)
iso_country_code (Controlled vocab) (DwC: country & countryCode, unless there is a separate country name field)
license (http://creativecommons.org/publicdomain/zero/1.0/legalcode) (DwC: license) *added to BISON Data Schema FY16/17 (added
and populated by Dev team during data ingest; no blank column necessary in BISON-munged datasets)