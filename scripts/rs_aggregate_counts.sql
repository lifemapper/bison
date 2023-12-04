CREATE TABLE public.county_counts AS
    SELECT DISTINCT census_county, census_state, riis_assessment, COUNT(*)
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY census_county, census_state, riis_assessment;

CREATE TABLE public.aiannh_counts AS
    SELECT DISTINCT aiannh_name, riis_assessment, COUNT(*)
    FROM bison_subset WHERE census_state IS NOT NULL
    GROUP BY aiannh_name, riis_assessment;
