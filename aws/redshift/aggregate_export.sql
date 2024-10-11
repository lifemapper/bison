-- Create lists and summaries of occurrences, species, and RIIS designation for each region

-- -------------------------------------------------------------------------------------
-- Create counts of occurrences and species by RIIS status for each region
-- -------------------------------------------------------------------------------------
DROP TABLE public.county_counts_2024_09_01;
DROP TABLE public.state_counts_2024_09_01;
DROP TABLE public.aiannh_counts_2024_09_01;
DROP TABLE public.county_x_riis_counts_2024_09_01;
DROP TABLE public.state_x_riis_counts_2024_09_01;
DROP TABLE public.aiannh_x_riis_counts_2024_09_01;

CREATE TABLE public.county_counts_2024_09_01 AS
    SELECT DISTINCT state_county,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY state_county;
CREATE TABLE public.county_x_riis_counts_2024_09_01 AS
    SELECT DISTINCT state_county, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY state_county, riis_assessment;

CREATE TABLE public.state_counts_2024_09_01 AS
    SELECT DISTINCT census_state,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY census_state;
CREATE TABLE public.state_x_riis_counts_2024_09_01 AS
    SELECT DISTINCT census_state, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY census_state, riis_assessment;

CREATE TABLE public.aiannh_counts_2024_09_01 AS
    SELECT DISTINCT aiannh_name,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY aiannh_name;
CREATE TABLE public.aiannh_x_riis_counts_2024_09_01 AS
    SELECT DISTINCT aiannh_name, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY aiannh_name, riis_assessment;

-- Check counts
SELECT * from county_counts_2024_09_01 ORDER BY state_county LIMIT 10;
SELECT * from state_counts_2024_09_01 ORDER BY census_state LIMIT 10;
SELECT * from aiannh_counts_2024_09_01 ORDER BY aiannh_name LIMIT 10;

SELECT * from county_x_riis_counts_2024_09_01 ORDER BY state_county, riis_assessment LIMIT 10;
SELECT * from state_x_riis_counts_2024_09_01 ORDER BY census_state, riis_assessment LIMIT 10;
SELECT * from aiannh_x_riis_counts_2024_09_01 ORDER BY aiannh_name, riis_assessment LIMIT 10;

-- -------------------------------------------------------------------------------------
-- Create lists of species for each region with counts
-- -------------------------------------------------------------------------------------
DROP TABLE public.county_x_species_list_2024_09_01;
DROP TABLE public.state_x_species_list_2024_09_01;
DROP TABLE public.aiannh_x_species_list_2024_09_01;

CREATE TABLE public.county_x_species_list_2024_09_01 AS
    SELECT DISTINCT state_county, taxonkey_species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY state_county, taxonkey_species, riis_assessment;
CREATE TABLE public.state_x_species_list_2024_09_01 AS
    SELECT DISTINCT census_state, taxonkey_species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY census_state, taxonkey_species, riis_assessment;
CREATE TABLE public.aiannh_x_species_list_2024_09_01 AS
    SELECT DISTINCT aiannh_name, taxonkey_species, riis_assessment,
        COUNT(*) AS occ_count
    FROM  bison_2024_09_01 WHERE census_state IS NOT NULL AND species IS NOT NULL
    GROUP BY aiannh_name, taxonkey_species, riis_assessment;

-- Check counts
SELECT * from county_x_species_list_2024_09_01 ORDER BY state_county, taxonkey_species, riis_assessment LIMIT 10;
SELECT * from state_x_species_list_2024_09_01 ORDER BY census_state, taxonkey_species, riis_assessment LIMIT 10;
SELECT * from aiannh_x_species_list_2024_09_01 ORDER BY aiannh_name, taxonkey_species, riis_assessment LIMIT 10;

-- -------------------------------------------------------------------------------------
-- Write data summaries to S3 as CSV for data delivery
-- -------------------------------------------------------------------------------------
-- Option: add "PARTITION BY (region_field)" to separate regions into individual files;
--      if so, remove the trailing underscore from the target folder name
-- Note: include "PARALLEL OFF" so it writes serially and does not write many very small
--      files.  Default max filesize is 6.2 GB, can change with option, for example,
--      "MAXFILESIZE 1 gb".
-- -------------------------------------------------------------------------------------
-- Write as parquet for data-frame loading
-- Parquet format with PARALLEL OFF appends 000.parquet to the name prefix.  000 is the
--    slice number.
-- -------------------------------------------------------------------------------------

-- List species with riis status, occurrence and species counts, for each region
UNLOAD (
    'SELECT * FROM county_x_species_list_2024_09_01 ORDER BY state_county, species')
    TO 's3://bison-321942852011-us-east-1/summary/county-x-species_list_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM state_x_species_list_2024_09_01 ORDER BY census_state, species')
    TO 's3://bison-321942852011-us-east-1/summary/state-x-species_list_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM aiannh_x_species_list_2024_09_01 ORDER BY aiannh_name, species')
    TO 's3://bison-321942852011-us-east-1/summary/aiannh-x-species_list_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;

UNLOAD (
    'SELECT * FROM county_x_riis_counts_2024_09_01 ORDER BY state_county, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/summary/county-x-riis_counts_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM county_counts_2024_09_01 ORDER BY state_county')
    TO 's3://bison-321942852011-us-east-1/summary/county_counts_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM state_x_riis_counts_2024_09_01 ORDER BY census_state, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/summary/state-x-riis_counts_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM state_counts_2024_09_01 ORDER BY census_state')
    TO 's3://bison-321942852011-us-east-1/summary/state_counts_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM aiannh_x_riis_counts_2024_09_01 ORDER BY aiannh_name, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/summary/aiannh-x-riis_counts_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;
UNLOAD (
    'SELECT * FROM aiannh_counts_2024_09_01 ORDER BY aiannh_name')
    TO 's3://bison-321942852011-us-east-1/summary/aiannh_counts_2024_09_01_'
    IAM_role DEFAULT
    FORMAT AS PARQUET
    PARALLEL OFF;

-- Cleanup Redshift data summaries
DROP TABLE public.county_counts_2024_09_01;
DROP TABLE public.state_counts_2024_09_01;
DROP TABLE public.aiannh_counts_2024_09_01;

DROP TABLE public.county_x_riis_counts_2024_09_01;
DROP TABLE public.state_x_riis_counts_2024_09_01;
DROP TABLE public.aiannh_x_riis_counts_2024_09_01;

DROP TABLE public.county_lists_2024_09_01;
DROP TABLE public.state_lists_2024_09_01;
DROP TABLE public.aiannh_lists_2024_09_01;
