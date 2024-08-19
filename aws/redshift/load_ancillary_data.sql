-- Load reference region shapefiles and US RIIS into Redshift
-- Note: You MUST update the annotated RIIS filename with the most recent version
--   containing the current date (first day of current month),
--   i.e. US-RIIS_MasterList_2021_annotated_2024_02_01.csv'
-- Load US Registry of Introduced and Invasive Species (US-RIIS)
-- Must use underscores in table name, no dash
-- -------------------------------------------------------------------------------------
-- US Registry of Introduced and Invasive Species (US-RIIS)
-- -------------------------------------------------------------------------------------
DROP TABLE riis_2024_02_01;
-- TODO: re-annotate these data and upload to S3
CREATE TABLE riis2021_2024_08_01 (
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
COPY riis2021_2024_08_01
FROM 's3://bison-321942852011-us-east-1/input_data/US-RIIS_MasterList_2021_annotated_2024-02-01.csv'
FORMAT CSV
IAM_role DEFAULT;

SELECT COUNT(*) FROM riis_2024_08_01;
SELECT * FROM riis_2024_08_01 LIMIT 10;

-- -------------------------------------------------------------------------------------
-- American Indian, Alaskan Native, Native Hawaiian lands (AIANNH)
-- Note: when updating to current census data, check the shapefile fields and match
--       them in the table
-- -------------------------------------------------------------------------------------
DROP TABLE aiannh;
CREATE TABLE aiannh2023 (
   shape    GEOMETRY,
   AIANNHCE VARCHAR(max),
   AIANNHNS VARCHAR(max),
   GEOIDFQ  VARCHAR(max),
   GEOID    VARCHAR(max),
   NAME     VARCHAR(max),
   NAMELSAD VARCHAR(max),
   LSAD     VARCHAR(max),
   MTFCC    VARCHAR(max),
   ALAND    VARCHAR(max),
   AWATER   VARCHAR(max)
);

COPY aiannh2023 FROM 's3://bison-321942852011-us-east-1/input_data/cb_2023_us_aiannh_500k.shp'
FORMAT SHAPEFILE
--SIMPLIFY AUTO
IAM_role DEFAULT;

select * from aiannh2023 limit 10


-- -------------------------------------------------------------------------------------
-- US Census County Boundaries
-- -------------------------------------------------------------------------------------
DROP TABLE county;
CREATE TABLE aiannh2023 (
   shape    GEOMETRY,
   AIANNHCE VARCHAR(max),
   AIANNHNS VARCHAR(max),
   GEOIDFQ  VARCHAR(max),
   GEOID    VARCHAR(max),
   NAME     VARCHAR(max),
   NAMELSAD VARCHAR(max),
   LSAD     VARCHAR(max),
   MTFCC    VARCHAR(max),
   ALAND    VARCHAR(max),
   AWATER   VARCHAR(max)
);

COPY aiannh2023 FROM 's3://bison-321942852011-us-east-1/input_data/cb_2023_us_aiannh_500k.shp'
FORMAT SHAPEFILE
--SIMPLIFY AUTO
IAM_role DEFAULT;

select * from aiannh2023 limit 10

---- -------------------------------------------------------------------------------------
---- US Protected Areas Database (US-PAD), only GAP status = 1
---- TODO: Fix if possible
---- Fails on RS load with: Compass I/O exception: Invalid hexadecimal character(s) found
---- -------------------------------------------------------------------------------------
DROP TABLE pad1;
CREATE TABLE pad1 (
   SHAPE     GEOMETRY,
   OBJECTID  INTEGER,
   Mang_Type VARCHAR(max),
   Mang_Name VARCHAR(max),
   Loc_Ds    VARCHAR(max),
   Unit_Nm   VARCHAR(max),
   GAP_Sts   VARCHAR(max),
   GIS_Acres VARCHAR(max)
);

COPY pad1 FROM 's3://bison-321942852011-us-east-1/input_data/pad_4.0_gap1_4326.shp'
FORMAT SHAPEFILE
SIMPLIFY AUTO
IAM_role DEFAULT;

SELECT query_id, start_time, line_number, column_name, column_type, error_message
    FROM sys_load_error_detail ORDER BY start_time DESC;

---- -------------------------------------------------------------------------------------
---- -------------------------------------------------------------------------------------

SHOW TABLES FROM SCHEMA dev.public;

---- -------------------------------------------------------------------------------------
---- -------------------------------------------------------------------------------------
