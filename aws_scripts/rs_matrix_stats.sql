SELECT * FROM public.bison_subset_2024_01_01 WHERE taxonrank != 'SPECIES' LIMIT 10;

SELECT DISTINCT species FROM public.bison_subset_2024_01_01;
SELECT DISTINCT census_state, census_county FROM public.bison_subset_2024_01_01 WHERE census_state IS NOT NULL;

CREATE TABLE public.heatmatrix_2024_01_01 AS
  SELECT DISTINCT census_state, census_county FROM public.bison_subset_2024_01_01
      WHERE census_state IS NOT NULL;

-- SQL function to add column for each species

ALTER TABLE public.heatmatrix_2024_01_01
ADD COLUMN census_state  VARCHAR(2)
DEFAULT NULL;
