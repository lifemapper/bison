
.. highlight:: rest

BISON data provider load 
=========================
.. contents::  


#. Process/sort the current-BISON-provider-records provided by Denver from the BISON 
   production system, to be used as input for BISON provider dataset processing.
   
   * Create a separate file for every resource_id encountered when reading the 
     large dataset, group records into these files:  BISON-provider-datasets-to-update
   
#. Download all available individual BISON provider datasets from Jira tickets.

#. Assemble metadata for all resources (by resource_id) pulled from Jira ticket,

   * new resources will not have a resource_id, so use the unique resource name 
     pulled from the ipt URL of new datasets.
   * add to metadata any existing resources from BISON-provider-datasets-to-update 
     with no Jira instructions.  These datasets will be reprocessed, 
     action = 'REWRITE'

#. Process files as indicated by Jira tickets, using the following 'action' to 
   determine the source input dataset :

   * ADD:
   
     * should not exist in BISON-provider-datasets-to-update
     
   * REPLACE:
   
     * Use the new file downloaded from Jira, ignore the dataset in BISON-provider-datasets-to-update
     
   * REPLACE_RENAME:
   
     * Use the new file downloaded from Jira, ignore the dataset in BISON-provider-datasets-to-update, 
       but replace field resource (and possibly resource_url)

   * RENAME:
   
     * Use the dataset in BISON-provider-datasets-to-update, but replace field
       resource (and possibly resource_url)

   * REWRITE:
   
     * Use the dataset in BISON-provider-datasets-to-update
       

#. Process all BISON-provider datasets

   * Step 1: Fill resource/provider constants, handle quotes
   * Step 2 (same as step 3 of GBIF processing):

     * fill ITIS fields: itis_tsn, valid_accepted_scientific_name, valid_accepted_tsn, itis_common_names, and if blank, kingdom
     * fill establishment means using itis_tsn or clean_provided_scientific_name
     * if missing, fill coordinates from county centroids IFF centroid != county

   * Step 3: georeference
         
     * calculate enclosing terrestrial (state, county, fips) or marine (eez, mrgid) polygons

#. Provide the updated-BISON-provider-records to Derek for ingest

