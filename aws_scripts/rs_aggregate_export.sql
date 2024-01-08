-- Create aggregate counts of occurrences and species for each region

-- Create aggregate counts of RIIS status occurrences for each region
DROP TABLE public.county_counts;
CREATE TABLE public.county_counts AS
    SELECT DISTINCT census_county, census_state, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY census_county, census_state, riis_assessment;
SELECT * from county_counts ORDER BY census_state, census_county, riis_assessment LIMIT 10;

DROP TABLE public.state_counts;
CREATE TABLE public.state_counts AS
    SELECT DISTINCT census_state, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY census_state, riis_assessment;
SELECT * from state_counts ORDER BY census_state, riis_assessment LIMIT 10;

DROP TABLE public.aiannh_counts;
CREATE TABLE public.aiannh_counts AS
    SELECT DISTINCT aiannh_name, riis_assessment,
           COUNT(*) AS occ_count, COUNT(DISTINCT taxonkey) AS species_count
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY aiannh_name, riis_assessment;
SELECT * from aiannh_counts ORDER BY aiannh_name, riis_assessment LIMIT 10;

UNLOAD (
    'SELECT * FROM county_counts ORDER BY census_state, census_county, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/out_data/county_counts_'
    IAM_role DEFAULT
    PARALLEL OFF
    DELIMITER AS '\t'
    HEADER;
UNLOAD (
    'SELECT * FROM state_counts ORDER BY census_state, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/out_data/state_counts_'
    IAM_role DEFAULT
    PARALLEL OFF
    DELIMITER AS '\t'
    HEADER;
UNLOAD (
    'SELECT * FROM aiannh_counts ORDER BY aiannh_name, riis_assessment')
    TO 's3://bison-321942852011-us-east-1/out_data/aiannh_counts_'
    IAM_role DEFAULT
    PARALLEL OFF
    DELIMITER AS '\t'
    HEADER;

-- Create aggregate counts of species occurrences for each region
-- Create aggregate counts of species occurrences for each region
DROP TABLE public.county_lists;
CREATE TABLE public.county_lists AS
    SELECT DISTINCT census_state, census_county, taxonkey, scientificname, riis_assessment,
        COUNT(*) AS occ_count
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY census_state, census_county, taxonkey, scientificname, riis_assessment;
SELECT * from county_lists ORDER BY census_state, census_county, scientificname LIMIT 10;

DROP TABLE public.state_lists;
CREATE TABLE public.state_lists AS
    SELECT DISTINCT census_state, taxonkey, scientificname, riis_assessment,
        COUNT(*) AS occ_count
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY census_state, taxonkey, scientificname, riis_assessment;
SELECT * from county_lists ORDER BY census_state, scientificname LIMIT 10;

DROP TABLE public.aiannh_lists;
CREATE TABLE public.aiannh_lists AS
    SELECT DISTINCT aiannh_name, taxonkey, scientificname, riis_assessment,
        COUNT(*) AS occ_count
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY aiannh_name, taxonkey, scientificname, riis_assessment;
SELECT * from aiannh_lists ORDER BY aiannh_name, scientificname LIMIT 10;

UNLOAD (
    'SELECT * FROM county_lists ORDER BY census_state, census_county, scientificname')
    TO 's3://bison-321942852011-us-east-1/out_data/county_lists_'
    IAM_role DEFAULT
    PARALLEL OFF
    DELIMITER AS '\t'
    HEADER;

UNLOAD (
    'SELECT * FROM state_lists ORDER BY census_state, scientificname')
    TO 's3://bison-321942852011-us-east-1/out_data/state_lists_'
    IAM_role DEFAULT
    PARALLEL OFF
    DELIMITER AS '\t'
    HEADER;

UNLOAD (
    'SELECT * FROM aiannh_lists ORDER BY aiannh_name, scientificname')
    TO 's3://bison-321942852011-us-east-1/out_data/aiannh_lists_'
    IAM_role DEFAULT
    PARALLEL OFF
    DELIMITER AS '\t'
    HEADER;
