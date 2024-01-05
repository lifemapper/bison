-- Append fields to bison subset
ALTER TABLE public.bison_subset
    ADD COLUMN census_state  VARCHAR(2)
    DEFAULT NULL;
ALTER TABLE public.bison_subset
    ADD COLUMN census_county   VARCHAR(100)
    DEFAULT NULL;
ALTER TABLE public.bison_subset
    ADD COLUMN riis_region   VARCHAR(3)
    DEFAULT NULL;
ALTER TABLE public.bison_subset
    ADD COLUMN riis_occurrence_id   VARCHAR(50)
    DEFAULT NULL;
ALTER TABLE public.bison_subset
    ADD COLUMN riis_assessment   VARCHAR(20)
    DEFAULT NULL;
ALTER TABLE public.bison_subset
    ADD COLUMN aiannh_name   VARCHAR(200)
    DEFAULT NULL;
ALTER TABLE public.bison_subset
    ADD COLUMN aiannh_geoid   VARCHAR(200)
    DEFAULT NULL;

-- Create temp tables with census/aiannh values
CREATE TABLE public.tmp_subset_x_census AS
	SELECT subset.gbifid, county.stusps, county.name
	FROM county, public.bison_subset as subset
	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(county.shape, 4326));
CREATE TABLE public.tmp_subset_x_aiannh AS
	SELECT subset.gbifid, aiannh.namelsad, aiannh.geoid
	FROM aiannh, public.bison_subset as subset
	WHERE ST_intersects(ST_SetSRID(subset.geom, 4326), ST_SetSRID(aiannh.shape, 4326));

-- Check numbers
SELECT COUNT(*) FROM public.tmp_subset_x_census;
SELECT COUNT(*) FROM public.tmp_subset_x_aiannh;
SELECT COUNT(*) FROM public.bison_subset;

-- Add census/aiannh values to big dataset
UPDATE bison_subset AS subset
	SET census_state = temp.stusps, census_county = temp.name
	FROM tmp_subset_x_census AS temp
	WHERE subset.gbifid = temp.gbifid;
UPDATE bison_subset AS subset
	SET aiannh_name = temp.namelsad, aiannh_geoid = temp.geoid
	FROM tmp_subset_x_aiannh AS temp
	WHERE subset.gbifid = temp.gbifid;

-- Compute riis_region values for dataset
UPDATE bison_subset
	SET riis_region = census_state
	WHERE census_state IN ('AK', 'HI');
UPDATE bison_subset
	SET riis_region = 'L48'
	WHERE census_state IS NOT NULL AND census_state NOT IN ('AK', 'HI');
-- Match on RIIS region and GBIF taxonkey
UPDATE bison_subset
	SET riis_occurrence_id = riis.occurrenceid,
	    riis_assessment = riis.introduced_or_invasive
	FROM riis
	WHERE riis.locality = riis_region
	  AND riis.gbif_res_taxonkey = taxonkey;
-- Non-matching records are presumed native species
UPDATE bison_subset
	SET riis_assessment = 'presumed_native'
	WHERE census_state IS NOT NULL AND riis_occurrence_id IS NULL;

-- Check numbers
SELECT * FROM bison_subset WHERE census_state IS NOT NULL LIMIT 10;
SELECT COUNT(*) FROM bison_subset
    WHERE census_state IS NOT NULL AND riis_occurrence_id IS NULL;

SELECT COUNT(*) FROM bison_subset;
SELECT COUNT(*) FROM bison_subset WHERE census_state IS NOT NULL;

-- Write to S3
UNLOAD (
    'SELECT * FROM bison_subset WHERE census_state IS NOT NULL')
    TO 's3://bison-321942852011-us-east-1/annotated_records/bison_occ_'
    IAM_role DEFAULT
    DELIMITER AS '\t'
    manifest
    HEADER;
