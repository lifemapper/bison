-- Load reference region shapefiles and US RIIS into Redshift
-- Note: You MUST update the annotated RIIS filename with the most recent version
--   containing the current date (first day of current month),
--   i.e. US-RIIS_MasterList_2021_annotated_2024_02_01.csv'
-- Load US Registry of Introduced and Invasive Species (US-RIIS)
-- First crawl data with Glue Crawler to get fields
-- Must use underscores in table name, no dash
-- -------------------------------------------------------------------------------------
-- US Registry of Introduced and Invasive Species (US-RIIS)
-- -------------------------------------------------------------------------------------
DROP TABLE riis_2024_02_01;
CREATE TABLE riis_2024_02_01 (
	locality	VARCHAR(max),
	scientificname	VARCHAR(max),
	scientificnameauthorship	VARCHAR(max),
	vernacularname	VARCHAR(max),
	taxonrank	VARCHAR(max),
	introduced_or_invasive	VARCHAR(max),
	biocontrol	VARCHAR(max),
	associatedtaxa	VARCHAR(max),
	approximate_introduction_date	VARCHAR(max),
	introdatenumber	VARCHAR(max),
	other_names	VARCHAR(max),
	kingdom	VARCHAR(max),
	phylum	VARCHAR(max),
	class	VARCHAR(max),
	_order	VARCHAR(max),
    family	VARCHAR(max),
	taxonomicstatus	VARCHAR(max),
	itis_tsn	VARCHAR(max),
	gbif_taxonkey	VARCHAR(max),
	authority	VARCHAR(max),
	associatedreferences	VARCHAR(max),
	acquisition_date	VARCHAR(max),
	modified	VARCHAR(max),
	update_remarks	VARCHAR(max),
	occurrenceremarks	VARCHAR(max),
	occurrenceid	VARCHAR(max),
	gbif_res_taxonkey	VARCHAR(max),
	gbif_res_scientificname	VARCHAR(max),
	lineno	VARCHAR(max)
);
-- ERROR: Load into table 'riis' failed. Check 'sys_load_error_detail' system table for
-- details. [ErrorId: 1-656a6401-67bc80cb02ba7898069d8916]
COPY riis_2024_02_01
FROM 's3://bison-321942852011-us-east-1/input_data/US-RIIS_MasterList_2021_annotated_2024_02_01.csv'
FORMAT CSV
IAM_role DEFAULT;

SELECT COUNT(*) FROM riis_2024_02_01;
SELECT * FROM riis_2024_02_01 LIMIT 10;

---- -------------------------------------------------------------------------------------
---- US Protected Areas Database (US-PAD)
---- -------------------------------------------------------------------------------------
--DROP TABLE pad;
--CREATE TABLE pad (
--);
--
--COPY pad FROM 's3://bison-321942852011-us-east-1/input_data/pad/PADUS3_0VectorAnalysisFile_ClipCensus.zip'
--FORMAT SHAPEFILE
--IAM_role DEFAULT;

-- -------------------------------------------------------------------------------------
-- American Indian, Alaskan Native, Native Hawaiian lands (AIANNH)
-- -------------------------------------------------------------------------------------
DROP TABLE aiannh;
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
COPY aiannh FROM 's3://bison-321942852011-us-east-1/input_data/cb_2021_us_aiannh_500k.shp'
FORMAT SHAPEFILE
--SIMPLIFY AUTO
IAM_role DEFAULT;

select * from aiannh limit 10

-- -------------------------------------------------------------------------------------
-- US Census County Boundaries
-- -------------------------------------------------------------------------------------
DROP TABLE county;
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
COPY county FROM 's3://bison-321942852011-us-east-1/input_data/cb_2021_us_county_500k.shp'
FORMAT SHAPEFILE
--SIMPLIFY AUTO
IAM_role DEFAULT;

select * from county limit 10

SHOW TABLES FROM SCHEMA dev.public;