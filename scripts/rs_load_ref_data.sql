CREATE TABLE aiannh (
   shape    GEOMETRY,
   AIANNHCE VARCHAR(max),
   AIANNHNS VARCHAR(max),
   AFFGEOID VARCHAR(max),
   GEOID    VARCHAR(max),
   NAME     VARCHAR(max),
   NAMELSAD VARCHAR(max),
   LSAD     VARCHAR(max),
   ALAND    VARCHAR(max),
   AWATER   VARCHAR(max)
);

COPY aiannh FROM 's3://bison-321942852011-us-east-1/input_data/region/cb_2021_us_aiannh_500k.shp'
FORMAT SHAPEFILE
SIMPLIFY AUTO
IAM_role DEFAULT;

select * from aiannh limit 10

CREATE TABLE county (
   shape    GEOMETRY,
   STATEFP  VARCHAR(max),
   COUNTYFP VARCHAR(max),
   COUNTYNS VARCHAR(max),
   AFFGEOID VARCHAR(max),
   GEOID    VARCHAR(max),
   NAME     VARCHAR(max),
   NAMELSAD VARCHAR(max),
   STUSPS   VARCHAR(max),
   STATE_NAME   VARCHAR(max),
   LSAD     VARCHAR(max),
   ALAND    VARCHAR(max),
   AWATER   VARCHAR(max)
);

COPY county FROM 's3://bison-321942852011-us-east-1/input_data/region/cb_2021_us_county_500k.shp'
FORMAT SHAPEFILE
SIMPLIFY AUTO
IAM_role DEFAULT;

select * from county limit 10

-- First crawl data with Glue Crawler to get fields
CREATE TABLE riis (
    locality                VARCHAR(200),
    scientificname          VARCHAR(200),
    scientificnameauthorship    VARCHAR(200),
    vernacularname  	    VARCHAR(200),
    taxonrank               VARCHAR(200),
    introduced_or_invasive  VARCHAR(200),
    biocontrol              VARCHAR(200),
    associatedtaxa  	    VARCHAR(200),
    approximate_introduction_date  	VARCHAR(200),
    introdatenumber         BIGINT,
    other_names             VARCHAR(200),
    kingdom  	            VARCHAR(200),
    phylum  	            VARCHAR(200),
    class                 	VARCHAR(200),
    _order                   VARCHAR(200),
    family                  VARCHAR(200),
    taxonomicstatus  	    VARCHAR(200),
    itis_tsn	            BIGINT,
    gbif_taxonkey       	BIGINT,
    authority               VARCHAR(200),
    associatedreferences  	VARCHAR(200),
    acquisition_date  	    VARCHAR(200),
    modified  	            VARCHAR(200),
    update_remarks        	VARCHAR(200),
    occurrenceremarks  	    VARCHAR(200),
    occurrenceid  	        VARCHAR(200),
    gbif_res_taxonkey	    BIGINT,
    gbif_res_scientificname VARCHAR(200),
    lineno                  BIGINT
);

-- ERROR: Load into table 'riis' failed. Check 'sys_load_error_detail' system table for
-- details. [ErrorId: 1-656a6401-67bc80cb02ba7898069d8916]
COPY riis FROM 's3://bison-321942852011-us-east-1/input_data/riis/US-RIIS_MasterList_2021_annotated.csv'
FORMAT CSV
IAM_role DEFAULT;
