-- Load reference region shapefiles and US RIIS into Redshift
-- Note: You MUST update the annotated RIIS filename with the most recent version
--   containing the current date (first day of current month),
--   i.e. US-RIIS_MasterList_2021_annotated_2024_02_01.csv'
-- Load US Registry of Introduced and Invasive Species (US-RIIS)
-- First crawl data with Glue Crawler to get fields
-- Must use underscores in table name, no dash

-- -------------------------------------------------------------------------------------
-- Add fields to BISON subset
-- -------------------------------------------------------------------------------------
-- Append fields for annotations to bison subset
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN aiannh_name   VARCHAR(200)
    DEFAULT NULL;
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN aiannh_geoid   VARCHAR(200)
    DEFAULT NULL;
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN census_state  VARCHAR(2)
    DEFAULT NULL;
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN census_county   VARCHAR(100)
    DEFAULT NULL;
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN riis_region   VARCHAR(3)
    DEFAULT NULL;
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN riis_occurrence_id   VARCHAR(50)
    DEFAULT NULL;
ALTER TABLE public.bison_2024_08_01
    ADD COLUMN riis_assessment   VARCHAR(20)
    DEFAULT NULL;

-- -------------------------------------------------------------------------------------
-- Geospatial intersect BISON subset with regions
-- -------------------------------------------------------------------------------------
-- TODO: can we fill annotation fields directly?
-- Create temp table with county values
CREATE TABLE public.tmp_bison_x_county AS
	SELECT bison.gbifid, county2023.stusps, county2023.namelsad
	FROM county2023, public.bison_2024_08_01 as bison
	WHERE ST_intersects(ST_SetSRID(bison.geom, 4326), ST_SetSRID(county2023.shape, 4326));

-- Add census values to BISON subset
UPDATE public.bison_2024_08_01 AS bison
	SET census_state = tmp.stusps, census_county = tmp.namelsad
	FROM tmp_bison_x_county AS tmp
	WHERE bison.gbifid = tmp.gbifid;

-- Create temp table with aiannh values
CREATE TABLE public.tmp_bison_x_aiannh AS
	SELECT bison.gbifid, aiannh2023.namelsad, aiannh2023.geoid
	FROM aiannh2023, public.bison_2024_08_01 as bison
	WHERE ST_intersects(ST_SetSRID(bison.geom, 4326), ST_SetSRID(aiannh2023.shape, 4326));
-- Add aiannh values to BISON subset
UPDATE public.bison_2024_08_01 AS bison
	SET aiannh_name = tmp.namelsad, aiannh_geoid = tmp.geoid
	FROM tmp_bison_x_aiannh AS tmp
	WHERE bison.gbifid = tmp.gbifid;

-- Verify counts
SELECT COUNT(*) FROM public.tmp_bison_x_county;
SELECT COUNT(*) FROM public.tmp_bison_x_aiannh;
SELECT COUNT(*) FROM public.bison_2024_08_01;

-- Cleanup temp tables
DROP TABLE public.tmp_bison_x_county;
DROP TABLE public.tmp_bison_x_aiannh;


-- -------------------------------------------------------------------------------------
-- Annotate BISON subset with RIIS status
-- -------------------------------------------------------------------------------------
-- Compute riis_region (AK, HI, L48) values for dataset
UPDATE public.bison_2024_08_01
	SET riis_region = census_state
	WHERE census_state IN ('AK', 'HI');
UPDATE public.bison_2024_08_01
	SET riis_region = 'L48'
	WHERE census_state IS NOT NULL AND census_state NOT IN ('AK', 'HI');

-- Annotate records with matching RIIS region + GBIF taxonkey
UPDATE public.bison_2024_08_01
	SET riis_occurrence_id = riis.occurrenceid,
	    riis_assessment = riis.degreeofestablishment
	FROM riisv2_2024_08_01 as riis
	WHERE riis.locality = riis_region
	  AND riis.gbif_res_taxonkey = taxonkey;

-- Annotate non-matching records to presumed native
UPDATE public.bison_2024_08_01
	SET riis_assessment = 'presumed_native'
	WHERE riis_region IS NOT NULL AND riis_occurrence_id IS NULL;


-- -------------------------------------------------------------------------------------
-- Misc Queries
-- -------------------------------------------------------------------------------------
-- Check some records
SELECT * FROM public.bison_2024_08_01 WHERE census_state IS NOT NULL LIMIT 10;
SELECT * FROM public.bison_2024_08_01
  WHERE census_state IS NOT NULL AND riis_occurrence_id IS NOT NULL LIMIT 10;

-- What percentage of records could be annotated with state and matches RIIS records
SELECT COUNT(*) FROM public.bison_2024_08_01;
SELECT COUNT(*) FROM public.bison_2024_08_01 WHERE census_state IS NOT NULL;
SELECT COUNT(*) FROM public.bison_2024_08_01
    WHERE census_state IS NOT NULL AND riis_occurrence_id IS NOT NULL;

-- -------------------------------------------------------------------------------------
-- Export annotated data records resolved to county/state
-- -------------------------------------------------------------------------------------
-- Write to S3 as tab-delimited CSV
-- Takes ~ minutes for ~100 million records
UNLOAD (
    'SELECT * FROM public.bison_2024_08_01 WHERE census_state IS NOT NULL')
    TO 's3://bison-321942852011-us-east-1/annotated_records/bison_2024_08_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    HEADER
    PARALLEL OFF;

-- Write to S3 as Parquet format
-- Parquet format with PARALLEL OFF appends <slice #>_part_<part #>.parquet to the name
--    prefix.  The first digits (xx) indicate the slice number.
-- Parquet does not allow geometry export, so delete column first (make sure
--    decimallongitude, decimallatitude fields exist in table).
ALTER TABLE public.bison_2024_08_01 DROP COLUMN geom;
UNLOAD (
    'SELECT * FROM public.bison_2024_08_01 WHERE census_state IS NOT NULL')
    TO 's3://bison-321942852011-us-east-1/annotated_records/bison_2024_08_01_'
    IAM_role DEFAULT
    FORMAT AS Parquet
    PARALLEL OFF;
