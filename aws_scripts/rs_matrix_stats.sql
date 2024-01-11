SELECT * FROM public.bison_subset_2024_01_01 WHERE taxonrank != 'SPECIES' LIMIT 10;

SELECT DISTINCT species FROM public.bison_subset_2024_01_01;
SELECT DISTINCT census_state, census_county FROM public.bison_subset_2024_01_01 WHERE census_state IS NOT NULL;

CREATE TABLE public.heatmatrix_2024_01_01 AS
    SELECT DISTINCT census_state, census_county FROM public.bison_subset_2024_01_01
        WHERE census_state IS NOT NULL;

SELECT MAX(LEN(census_state)) FROM public.bison_subset_2024_01_01

-- Stored Procedure to create table and add column for each species
CREATE OR REPLACE PROCEDURE create_heatmatrix_table() AS $$
DECLARE
    rec RECORD;
    cnty_cnt INT;
BEGIN
    SELECT INTO cnty_cnt MAX(LEN(census_state))
        FROM public.bison_subset_2024_01_01;
    -- Delete existing table
    DROP TABLE IF EXISTS public.heatmatrix_2024_01_01;
    -- Create table with rows defined by state/county
    EXECUTE  'CREATE TABLE public.heatmatrix_2024_01_01(' ||
             'census_state VARCHAR(2), census_county VARCHAR(' || cnty_cnt || ') ' ||
             'UNIQUE (census_state, census_county)';
    -- Add species columns
    FOR rec IN
        SELECT DISTINCT species FROM public.bison_subset_2024_01_01
    LOOP
        EXECUTE 'ALTER TABLE public.heatmatrix_2024_01_01 ADD COLUMN \"' ||
                rec.species || '\" INT DEFAULT 0;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL create_heatmatrix_table();


-- Stored Procedure to add rows for each county/state
CREATE OR REPLACE PROCEDURE fill_heatmatrix_table() AS $$
DECLARE
    rec RECORD;
    rec_count INT;
    query TEXT;
BEGIN
    -- Delete existing rows
    DELETE * FROM public.heatmatrix_2024_01_01;

    -- Add species columns
    FOR rec IN
        'SELECT * from county_counts_2024_01_01 ' ||
             'ORDER BY census_state, census_county, riis_assessment';
    LOOP
        SELECT COUNT(*) INTO rec_count FROM public.heatmatrix_2024_01_01
            WHERE census_state = rec.census_state AND census_county = rec.census_county;
        RAISE NOTICE 'inserting data for % % % %;',
            rec.census_state, rec.census_county, rec.species, rec.species, rec.occ_count;
        IF rec_count == 1 THEN
        EXECUTE 'UPDATE public.heatmatrix_2024_01_01 ADD COLUMN \"' || rec.species || '\" INT DEFAULT 0;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL create_heatmatrix_table();