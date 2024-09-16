-- Load reference region shapefiles  into Redshift
-- Must use underscores in table names, no dash
-- -------------------------------------------------------------------------------------
-- American Indian, Alaskan Native, Native Hawaiian lands (AIANNH)
-- Note: when updating to current census data, check the shapefile fields and match
--       them in the table
-- -------------------------------------------------------------------------------------
DROP TABLE aiannh2023;
CREATE TABLE aiannh2023 (
   shape    GEOMETRY,
   AIANNHCE VARCHAR(4),
   AIANNHNS VARCHAR(8),
   GEOIDFQ  VARCHAR(13),
   GEOID    VARCHAR(4),
   NAME     VARCHAR(100),
   NAMELSAD VARCHAR(100),
   LSAD     VARCHAR(2),
   MTFCC    VARCHAR(5),
   ALAND    BIGINT,
   AWATER   BIGINT
);

COPY aiannh2023
FROM 's3://bison-321942852011-us-east-1/input/cb_2023_us_aiannh_500k.shp'
FORMAT SHAPEFILE
IAM_role DEFAULT;

select * from aiannh2023 limit 10


-- -------------------------------------------------------------------------------------
-- US Census County Boundaries
-- -------------------------------------------------------------------------------------
DROP TABLE county2023;
CREATE TABLE county2023 (
   shape      GEOMETRY,
   STATEFP    VARCHAR(2),
   COUNTYFP   VARCHAR(3),
   COUNTYNS   VARCHAR(8),
   GEOIDFQ    VARCHAR(14),
   GEOID      VARCHAR(5),
   NAME       VARCHAR(100),
   NAMELSAD   VARCHAR(100),
   STUSPS     VARCHAR(2),
   STATE_NAME VARCHAR(100),
   LSAD       VARCHAR(2),
   ALAND      INTEGER,
   AWATER     INTEGER
);

COPY county2023
FROM 's3://bison-321942852011-us-east-1/input/cb_2023_us_county_500k.shp'
FORMAT SHAPEFILE
IAM_role DEFAULT;

select * from county2023 limit 10

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

COPY pad1 FROM 's3://bison-321942852011-us-east-1/input/pad_4.0_gap1_4326.shp'
FORMAT SHAPEFILE
SIMPLIFY AUTO
IAM_role DEFAULT;


---- -------------------------------------------------------------------------------------
---- -------------------------------------------------------------------------------------

SHOW TABLES FROM SCHEMA dev.public;

---- -------------------------------------------------------------------------------------
---- -------------------------------------------------------------------------------------
