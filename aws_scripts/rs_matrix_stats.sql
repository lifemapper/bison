-- OBSOLETE
-- Limitation of 1600 columns in Redshift, this does not throw an error, just
-- fails to complete.
--
-- Create a heatmatrix, PAM, and compute statistics
-- -------------------------------------------------------------------
-- Misc queries
-- -------------------------------------------------------------------
SELECT * FROM bison_subset_2024_01_01 WHERE taxonrank != 'SPECIES' LIMIT 10;
SELECT DISTINCT census_state, census_county FROM county_lists_2024_01_01;

-- -------------------------------------------------------------------
-- Create heatmatrix with just Site (state, county)
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS heatmatrix_2024_01_01;
CREATE TABLE heatmatrix_2024_01_01 AS
    SELECT DISTINCT census_state, census_county FROM county_lists_2024_01_01;
SELECT * FROM heatmatrix_2024_01_01 LIMIT 20;

-- -------------------------------------------------------------------
-- Add species columns to heatmatrix
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE add_species_to_heatmatrix() AS $$
DECLARE
    rec RECORD;
    cmd TEXT;
BEGIN
    -- Add species columns
    FOR rec IN
        SELECT DISTINCT species FROM county_lists_2024_01_01
    LOOP
        EXECUTE 'ALTER TABLE heatmatrix_2024_01_01 ADD COLUMN \"' || rec.species || '\" INT DEFAULT 0';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
CALL add_species_to_heatmatrix();
SHOW COLUMNS FROM TABLE dev.public.heatmatrix_2024_01_01;


-- -------------------------------------------------------------------
-- Fill species columns for each county
-- -------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE fill_heatmatrix_table() AS $$
DECLARE
    rec1 RECORD;
    rec2 RECORD;
    site_qry TEXT;
    species_qry TEXT;
    cmd TEXT;
BEGIN
    site_qry := 'SELECT DISTINCT census_state, census_county FROM county_lists_2024_01_01 LIMIT 5';
    RAISE NOTICE 'site_qry: %', site_qry;
    FOR rec1 IN
        EXECUTE site_qry
    LOOP
        species_qry : = 'SELECT species, occ_count FROM county_lists_2024_01_01 ' ||
            'WHERE census_state = ' || rec1.census_state || ' AND census_county = ' ||
            rec1.census_county;
        RAISE NOTICE 'species_qry: %', species_qry;
        FOR rec2 IN
            EXECUTE species_qry
        LOOP
            cmd := 'UPDATE heatmatrix_2024_01_01 SET \"' || rec2.species || '\" = ' ||
                rec2.occ_count;
            RAISE NOTICE 'cmd: %', cmd;
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
