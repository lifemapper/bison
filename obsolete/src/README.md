# Obsolete data processing

## Process BISON-provider and GBIF data for BISON

## Data Load by year
* [2018 Data Load](notes/dataload_2018.rst)
* [2019/2020 Data Load](notes/dataload_2019-2020.rst)

## Latest processing specifics
* [BISON-Provider Data Processing](notes/provider_dataset_process.rst)
* [GBIF Data Processing](notes/gbif_process.rst)
* [Common Data Processing](notes/provider_dataset_process.rst)****

## Processing Dependencies

[Data Load Run Dependencies](notes/develop_run_env.rst)

## Misc Notes

* [July 2019 Data load meeting](notes/mtgnotes_2019_07.rst)
* [Standup meeting decisions about KU processing](notes/standup_mtg_decisions.rst)

## 2019 Data from Paul

#### bison.tar.gz
Occurrence data from BISON providers. CSV file created from Postgres table "solr" which 
is used to populate SOLR index.

#### provider.csv

BISON file containing provider lookup values for providers contained in "bisonProvider" table.
Data is used to simplify filter function in web app.

BISON provider ~= GBIF publisher/organization

#### resource.csv

BISON file containing resource lookup values for resources contained in "bisonResource" table.
Data is used to simplify BISON API filter function.

BISON resource ~= GBIF dataset
