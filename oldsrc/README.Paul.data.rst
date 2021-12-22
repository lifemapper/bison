bison.tar.gz::
  Occurrence data from BISON providers.  CSV file created from Postgres table 
  "solr" which is used to populate SOLR index.
  
provider.csv::
  BISON file containing provider lookup values for providers contained in 
  "bisonProvider" table.  Data is used to simplify filter function in web app. 
  BISON provider ~= GBIF publisher/organization


resource.csv::
  BISON file containing resource lookup values for resources contained in 
  "bisonResource" table.  Data is used to simplify BISON API filter function. 
  BISON resource ~= GBIF dataset