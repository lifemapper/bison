-- Create lists and summaries of occurrences, species, and RIIS designation for each region

-- -------------------------------------------------------------------------------------
-- Create counts of occurrences and species by RIIS status for each region
-- -------------------------------------------------------------------------------------
CREATE TABLE public.county_counts_2024_02_01 AS
    SELECT DISTINCT census_county, census_state, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_subset_2024_02_01 WHERE census_state IS NOT NULL
    GROUP BY census_county, census_state, riis_assessment;
CREATE TABLE public.state_counts_2024_02_01 AS
    SELECT DISTINCT census_state, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_subset_2024_02_01 WHERE census_state IS NOT NULL
    GROUP BY census_state, riis_assessment;
CREATE TABLE public.aiannh_counts_2024_02_01 AS
    SELECT DISTINCT aiannh_name, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_subset_2024_02_01 WHERE census_state IS NOT NULL
    GROUP BY aiannh_name, riis_assessment;

-- Check counts
SELECT * from county_counts_2024_02_01 ORDER BY census_state, census_county, riis_assessment LIMIT 10;
SELECT * from state_counts_2024_02_01 ORDER BY census_state, riis_assessment LIMIT 10;
SELECT * from aiannh_counts_2024_02_01 ORDER BY aiannh_name, riis_assessment LIMIT 10;

-- -------------------------------------------------------------------------------------
-- Create lists of species for each region with counts
-- -------------------------------------------------------------------------------------
CREATE TABLE public.county_lists_2024_02_01 AS
    SELECT DISTINCT census_state, census_county, taxonkey, species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_subset_2024_02_01 WHERE census_state IS NOT NULL
    GROUP BY census_state, census_county, taxonkey, species, riis_assessment;
CREATE TABLE public.state_lists_2024_02_01 AS
    SELECT DISTINCT census_state, taxonkey, species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_subset_2024_02_01 WHERE census_state IS NOT NULL
    GROUP BY census_state, taxonkey, species, riis_assessment;
CREATE TABLE public.aiannh_lists_2024_02_01 AS
    SELECT DISTINCT aiannh_name, taxonkey, species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_subset_2024_02_01 WHERE census_state IS NOT NULL
    GROUP BY aiannh_name, taxonkey, species, riis_assessment;

-- Check counts
SELECT * from county_lists_2024_02_01 ORDER BY census_state, census_county, species LIMIT 10;
SELECT * from state_lists_2024_02_01 ORDER BY census_state, species LIMIT 10;
SELECT * from aiannh_lists_2024_02_01 ORDER BY aiannh_name, species LIMIT 10;

-- -------------------------------------------------------------------------------------
-- Write data summaries to S3 as CSV for data delivery
-- -------------------------------------------------------------------------------------
-- Option: add "PARTITION BY (region_field)" to separate regions into individual files;
--      if so, remove the trailing underscore from the target folder name

-- Count occurrences and species for RIIS assessment by region
UNLOAD (
    'SELECT * FROM county_counts_2024_02_01 ORDER BY census_state, census_county, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/out_data/county_counts_2024_02_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    manifest
    HEADER;
UNLOAD (
    'SELECT * FROM state_counts_2024_02_01 ORDER BY census_state, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/out_data/state_counts_2024_02_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    manifest
    HEADER;
UNLOAD (
    'SELECT * FROM aiannh_counts_2024_02_01 ORDER BY aiannh_name, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/out_data/aiannh_counts_2024_02_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    manifest
    HEADER;

-- List species with riis status, occurrence and species counts, for each region
UNLOAD (
    'SELECT * FROM county_lists_2024_02_01 ORDER BY census_state, census_county, species')
    TO 's3://bison-321942852011-us-east-1/out_data/county_lists_2024_02_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    manifest
    HEADER;
-- Also write as Parquet for easy DataFrame loading
UNLOAD (
    'SELECT * FROM county_lists_2024_02_01 ORDER BY census_state, census_county, species')
    TO 's3://bison-321942852011-us-east-1/out_data/county_lists_2024_02_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM state_lists_2024_02_01 ORDER BY census_state, species')
    TO 's3://bison-321942852011-us-east-1/out_data/state_lists_2024_02_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    manifest
    HEADER;
UNLOAD (
    'SELECT * FROM aiannh_lists_2024_02_01 ORDER BY aiannh_name, species')
    TO 's3://bison-321942852011-us-east-1/out_data/aiannh_lists_2024_02_01_'
    IAM_role DEFAULT
    CSV DELIMITER AS '\t'
    manifest
    HEADER;

-- Cleanup Redshift data summaries
DROP TABLE public.county_counts_2024_02_01;
DROP TABLE public.state_counts_2024_02_01;
DROP TABLE public.aiannh_counts_2024_02_01;
DROP TABLE public.county_lists_2024_02_01;
DROP TABLE public.state_lists_2024_02_01;
DROP TABLE public.aiannh_lists_2024_02_01;