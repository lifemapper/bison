-- Annotate GBIF records with attributes from reference data

-------------------
-- Add fields
-------------------
-- Append fields for annotations to bison subset
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN census_state  VARCHAR(2)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN census_county   VARCHAR(100)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN riis_region   VARCHAR(3)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN riis_occurrence_id   VARCHAR(50)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN riis_assessment   VARCHAR(20)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN aiannh_name   VARCHAR(200)
    DEFAULT NULL;
ALTER TABLE public.bison_subset_2024_01_01
    ADD COLUMN aiannh_geoid   VARCHAR(200)
    DEFAULT NULL;

-------------------
-- Geospatial intersect
-------------------
-- Create temp tables with census/aiannh values
-- TODO: why aren't we filling annotation fields directly?
CREATE TABLE public.tmp_subset_x_census AS
	SELECT subset.gbifid, county.stusps, county.name
	FROM county, public.bison_subset_2024_01_01 as subset
	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(county.shape, 4326));
CREATE TABLE public.tmp_subset_x_aiannh AS
	SELECT subset.gbifid, aiannh.namelsad, aiannh.geoid
	FROM aiannh, public.bison_subset_2024_01_01 as subset
	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(aiannh.shape, 4326));

-- Verify counts
SELECT COUNT(*) FROM public.tmp_subset_x_census;
SELECT COUNT(*) FROM public.tmp_subset_x_aiannh;
SELECT COUNT(*) FROM public.bison_subset_2024_01_01;

-- Add census/aiannh values to big dataset
UPDATE public.bison_subset_2024_01_01 AS subset
	SET census_state = temp.stusps, census_county = temp.name
	FROM tmp_subset_x_census AS temp
	WHERE subset.gbifid = temp.gbifid;
UPDATE public.bison_subset_2024_01_01 AS subset
	SET aiannh_name = temp.namelsad, aiannh_geoid = temp.geoid
	FROM tmp_subset_x_aiannh AS temp
	WHERE subset.gbifid = temp.gbifid;
-- Cleanup temp tables
DROP TABLE public.tmp_subset_x_census;
DROP TABLE public.tmp_subset_x_aiannh;


-------------------
-- RIIS lookup
-------------------
-- Compute riis_region (AK, HI, L48) values for dataset
UPDATE public.bison_subset_2024_01_01
	SET riis_region = census_state
	WHERE census_state IN ('AK', 'HI');
UPDATE public.bison_subset_2024_01_01
	SET riis_region = 'L48'
	WHERE census_state IS NOT NULL AND census_state NOT IN ('AK', 'HI');

-- Annotate records with matching RIIS region + GBIF taxonkey
UPDATE public.bison_subset_2024_01_01
	SET riis_occurrence_id = riis.occurrenceid,
	    riis_assessment = riis.introduced_or_invasive
	FROM riis
	WHERE riis.locality = riis_region
	  AND riis.gbif_res_taxonkey = taxonkey;

-- Annotate non-matching records to presumed native
UPDATE public.bison_subset_2024_01_01
	SET riis_assessment = 'presumed_native'
	WHERE census_state IS NOT NULL AND riis_occurrence_id IS NULL;

-------------------
-- Misc Queries
-------------------
-- Check some records
SELECT * FROM public.bison_subset_2024_01_01 WHERE census_state IS NOT NULL LIMIT 10;
SELECT COUNT(*) FROM public.bison_subset_2024_01_01
    WHERE census_state IS NOT NULL AND riis_occurrence_id IS NULL;

-- What percentage of records could be annotated with state
SELECT COUNT(*) FROM public.bison_subset_2024_01_01;
SELECT COUNT(*) FROM public.bison_subset_2024_01_01 WHERE census_state IS NOT NULL;

-------------------
-- Export data
-------------------
-- Write to S3
UNLOAD (
    'SELECT * FROM public.bison_subset_2024_01_01 WHERE census_state IS NOT NULL')
    TO 's3://bison-321942852011-us-east-1/annotated_records/bison_2024_01_01_'
    IAM_role DEFAULT
    DELIMITER AS '\t'
    manifest
    HEADER;
