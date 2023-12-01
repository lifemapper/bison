-- Create a BISON table with a subset of records and subset of fields
CREATE TABLE public.bison_subset_2023_11_01 AS
	SELECT
		gbifid, species, taxonrank, scientificname, countrycode, stateprovince,
		occurrencestatus, publishingorgkey, decimallatitude, decimallongitude, day,
		month, year, taxonkey, specieskey, basisofrecord,
		ST_Makepoint(decimallongitude, decimallatitude) as geom
	FROM dev.redshift_spectrum.occurrence_2023_11_01_parquet
	WHERE
	    countrycode = 'US' and occurrencestatus = 'PRESENT' and taxonrank IN
		('SPECIES', 'SUBSPECIES', 'FORM', 'INFRASPECIFIC_NAME', 'INFRASUBSPECIFIC_NAME');

SHOW TABLES FROM SCHEMA dev.public;

-- Append fields
ALTER TABLE public.bison_subset_2023_11_01
    ADD COLUMN census_state  VARCHAR(2)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2023_11_01
    ADD COLUMN census_county   VARCHAR(max)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2023_11_01
    ADD COLUMN riis_occurrence_id   VARCHAR(max)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2023_11_01
    ADD COLUMN riis_assessment   VARCHAR(20)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2023_11_01
    ADD COLUMN aiannh_name   VARCHAR(max)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2023_11_01
    ADD COLUMN aiannh_geoid   VARCHAR(max)
    DEFAULT NULL;

-- Temp table with state/county values
CREATE TABLE public.temp_cty_intersect_2023_11_01 AS
	SELECT subset.gbifid, county.STUSPS, county.NAME
	FROM county, public.bison_subset_2023_11_01 as subset
	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(county.shape, 4326));

-- Temp table with aiannh values
CREATE TABLE public.temp_aiannh_intersect_2023_11_01 AS
	SELECT subset.gbifid, aiannh.namelsad, aiannh.geoid
	FROM aiannh, public.bison_subset_2023_11_01 as subset
	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(aiannh.shape, 4326));

-- Intersected records
SELECT COUNT(*) FROM public.temp_cty_intersect_2023_11_01;
SELECT COUNT(*) FROM public.temp_aiannh_intersect_2023_11_01;

-- Add state/county values to dataset
UPDATE bison_subset_2023_11_01 AS subset
	SET census_state = temp.stusps, census_county = temp.name
	FROM temp_cty_intersect_2023_11_01 AS temp
	WHERE subset.gbifid = temp.gbifid;

-- Add aiannh values to dataset
UPDATE bison_subset_2023_11_01 AS subset
	SET aiannh_name = temp.namelsad, aiannh_geoid = temp.geoid
	FROM temp_aiannh_intersect_2023_11_01 AS temp
	WHERE subset.gbifid = temp.gbifid;

-- Add riis values to dataset
UPDATE bison_subset_2023_11_01 AS subset
	SET riis_occurrence_id = temp.stusps, riis_assessment = temp.name
	FROM temp_cty_intersect_2023_11_01 AS temp
	WHERE subset.gbifid = temp.gbifid;


---- Faster than creating a temp table and joining,
---- BUT this only keeps records that intersect, and does not use new fieldnames
--create table public.bison_annotate_2023_11_01 as
--	SELECT bison_subset_2023_11_01.*, county.STUSPS, county.NAME
--	FROM bison_subset_2023_11_01 AS subset, county
--	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(county.shape, 4326));
