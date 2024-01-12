SELECT * FROM bison_subset_2024_01_01 WHERE taxonrank != 'SPECIES' LIMIT 10;

SELECT DISTINCT species FROM bison_subset_2024_01_01;
SELECT DISTINCT census_state, census_county FROM bison_subset_2024_01_01
    WHERE census_state IS NOT NULL;

CREATE TABLE heatmatrix_2024_01_01 AS
    SELECT DISTINCT census_state, census_county FROM bison_subset_2024_01_01
        WHERE census_state IS NOT NULL;

SELECT MAX(LEN(census_state)) FROM bison_subset_2024_01_01

-- Stored Procedure to create table and add column for each species
CREATE OR REPLACE PROCEDURE create_heatmatrix_table() AS $$
DECLARE
    rec RECORD;
    cnty_cnt INT;
BEGIN
    -- Delete existing table
    DROP TABLE IF EXISTS heatmatrix_2024_01_01;
    -- Get length of county field
    SELECT INTO cnty_cnt MAX(LEN(census_county)) FROM bison_subset_2024_01_01;
    -- Create table with rows defined by state/county
    EXECUTE  'CREATE TABLE heatmatrix_2024_01_01 (' ||
        'census_state VARCHAR(2), census_county VARCHAR(' || cnty_cnt || '), ' ||
        'UNIQUE (census_state, census_county))';
    -- Add species columns
    FOR rec IN
        SELECT DISTINCT species FROM bison_subset_2024_01_01
    LOOP
        EXECUTE 'ALTER TABLE heatmatrix_2024_01_01 ADD COLUMN \"' ||
                rec.species || '\" INT DEFAULT 0;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL create_heatmatrix_table();


CREATE OR REPLACE PROCEDURE fill_heatmatrix_table() AS $$
DECLARE
    rec RECORD;
BEGIN
    -- Delete existing rows
    DELETE FROM heatmatrix_2024_01_01;

    FOR rec IN
        EXECUTE 'SELECT DISTINCT census_state, census_county ' ||
            'FROM bison_subset_2024_01_01 WHERE census_state IS NOT NULL'
    LOOP
        EXECUTE 'INSERT INTO heatmatrix_2024_01_01 (census_state, census_county) ' ||
            'VALUES (\'' || rec.census_state || '\', \'' || rec.census_county || \'')';
    END LOOP;


    -- -- Add species columns
    -- FOR rec IN
    --     EXECUTE 'SELECT * from county_lists_2024_01_01 ORDER BY census_state, census_county, riis_assessment LIMIT 10'
    -- LOOP
    --     -- Update existing record
    --     EXECUTE 'UPDATE heatmatrix_2024_01_01 SET \"' ||
    --         rec.species || '\" = ' || rec.occ_count ||
    --         ' WHERE census_state = \"' || rec.census_state || '\"' ||
    --         ' AND census_county = \"' || rec.census_county || '\"';
    -- END LOOP;
END;
$$ LANGUAGE plpgsql;


CALL fill_heatmatrix_table();