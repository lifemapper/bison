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
                * Question:  BISON provider, identified by???
            * Use ‘$’ delimiter in CSV output
            * Generate ScientificName/taxonKey list during data processing: 
        * Step 3: Create Name LUT after to occ record processing
            * CanonicalName: from GBIF parser + scientificName or taxonKey + API. 
        * Step 4: Process edited step2_occ CSV file
            * fill clean_provided_scientific_name from name LUT. 
            * Remove any temporary columns for final BISON 47 columns 
