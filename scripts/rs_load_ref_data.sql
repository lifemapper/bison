CREATE TABLE aiannh (
   shape GEOMETRY,
   AIANNHCE VARCHAR(max),
   AIANNHNS VARCHAR(max),
   AFFGEOID VARCHAR(max),
   GEOID VARCHAR(max),
   NAME VARCHAR(max),
   NAMELSAD VARCHAR(max),
   LSAD VARCHAR(max),
   ALAND VARCHAR(max),
   AWATER VARCHAR(max)
);

COPY aiannh FROM 's3://bison-321942852011-us-east-1/input_data/region/cb_2021_us_aiannh_500k.shp'
FORMAT SHAPEFILE
SIMPLIFY AUTO
IAM_role DEFAULT;

select * from aiannh limit 10

CREATE TABLE county (
   shape GEOMETRY,
   STATEFP VARCHAR(max),
   COUNTYFP VARCHAR(max),
   COUNTYNS VARCHAR(max),
   AFFGEOID VARCHAR(max),
   GEOID VARCHAR(max),
   NAME VARCHAR(max),
   NAMELSAD VARCHAR(max),
   STUSPS VARCHAR(max),
   STATE_NAME VARCHAR(max),
   LSAD VARCHAR(max),
   ALAND VARCHAR(max),
   AWATER VARCHAR(max)
);

COPY county FROM 's3://bison-321942852011-us-east-1/input_data/region/cb_2021_us_county_500k.shp'
FORMAT SHAPEFILE
SIMPLIFY AUTO
IAM_role DEFAULT;

select * from county limit 10


-- SELECT gbifid, decimallatitude, decimallongitude FROM
--  'dev'.'redshift_spectrum'.'occurrence_parquet' limit 10;

---- small dataset does not contain all fields
--CREATE TABLE bison_very_small (
--	taxonrank	VARCHAR(max),
--	occurrenceid    VARCHAR(max),
--	kingdom	VARCHAR(max),
--	specieskey	INT,
--	coordinateprecision	DOUBLE PRECISION,
--	individualcount	INT,
--	lastinterpreted	TIMESTAMP,
-- 	datasetkey	VARCHAR(max),
--	taxonkey	INT,
--	_order	VARCHAR(max),
--	genus	VARCHAR(max),
--	eventdate	TIMESTAMP,
--	verbatimscientificname	VARCHAR(max),
--	dateidentified	TIMESTAMP,
--	countrycode	VARCHAR(max),
--	family	VARCHAR(max),
--	stateprovince	VARCHAR(max),
--	publishingorgkey	VARCHAR(max),
--	typestatus	SUPER,
--	year	INT,
--	coordinateuncertaintyinmeters	DOUBLE PRECISION,
--	verbatimscientificnameauthorship	VARCHAR(max),
--	issue	SUPER,
--	locality	VARCHAR(max),
--	basisofrecord	VARCHAR(max),
--	species	VARCHAR(max),
--	decimallongitude	DOUBLE PRECISION,
--	rightsholder	VARCHAR(max),
--	occurrencestatus	VARCHAR(max),
--	class	VARCHAR(max),
--	phylum	VARCHAR(max),
--	institutioncode	VARCHAR(max),
--	recordnumber	VARCHAR(max),
--	decimallatitude	DOUBLE PRECISION,
--	license	VARCHAR(max),
--	month	INT,
--	catalognumber	VARCHAR(max),
--    gbifid	VARCHAR(max),
--	collectioncode	VARCHAR(max),
--	day	INT,
--	scientificname	VARCHAR(max)
--);
--
----	census_state    VARCHAR(2),
----	census_county   VARCHAR(max),
----	riis_occurrence_id   VARCHAR(max),
----    riis_assessment   VARCHAR(20),
----    aiannh_name   VARCHAR(max),
----    aiannh_geoid   VARCHAR(max)
--
--COPY dev.public.gbif_very_small
--FROM 's3://bison-321942852011-us-east-1/raw_data/gbif_5k_2023-11-01.parquet'
--IAM_ROLE 'arn:aws:iam::321942852011:role/service-role/AmazonRedshift-CommandsAccessRole-20231129T105842'
--FORMAT AS PARQUET SERIALIZETOJSON

