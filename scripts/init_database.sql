-- Instructions at:
--   https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html

--psql --host=bison-db-test.cqvncffkwz9t.us-east-1.rds.amazonaws.com bison_input admin


-- ----------------------------------------------------------------------------
\c postgres postgres
-- ----------------------------------------------------------------------------
--CREATE ROLE admin with CREATEROLE CREATEDB LOGIN PASSWORD %password%;
GRANT rds_superuser TO admin;
GRANT ALL ON DATABASE template1 TO admin WITH GRANT OPTION;

-- create user roles
CREATE ROLE reader NOINHERIT;
-- CREATE ROLE annotator with LOGIN INHERIT PASSWORD  %password%;
GRANT reader TO annotator;

:Tc[rVVAfP:1W)gS>+1cFQ7ysy+p
U5z%45msE98BkdWf
-- ----------------------------------------------------------------------------
\c template1 admin
-- ----------------------------------------------------------------------------
-- Create PostGIS extensions
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;
CREATE EXTENSION fuzzystrmatch;
CREATE EXTENSION postgis_tiger_geocoder;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION address_standardizer_data_us;

-- Test creation
SELECT n.nspname AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
  FROM pg_catalog.pg_namespace n
  WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
  ORDER BY 1;

-- Change owner
ALTER SCHEMA tiger OWNER TO admin;
ALTER SCHEMA tiger_data OWNER TO admin;
ALTER SCHEMA topology OWNER TO admin;

-- Test owner change
SELECT n.nspname AS "Name",
  pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
  FROM pg_catalog.pg_namespace n
  WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
  ORDER BY 1;

-- Transfer ownership of the PostGIS objects
CREATE FUNCTION exec(text) returns text
language plpgsql volatile AS $f$
BEGIN
  EXECUTE $1;
  RETURN $1;
END;
$f$;

SELECT exec('ALTER TABLE ' || quote_ident(s.nspname) || '.' || quote_ident(s.relname) || ' OWNER TO admin;')
  FROM (
    SELECT nspname, relname
    FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid)
    WHERE nspname in ('tiger','topology') AND
    relkind IN ('r','S','v') ORDER BY relkind = 'S')
s;

-- Test the tiger schema
SET search_path=public,tiger;

SELECT address, streetname, streettypeabbrev, zip
 FROM normalize_address('1 Devonshire Place, Boston, MA 02109') AS na;

--SELECT topology.createtopology('my_new_topo',26986,0.5);

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Continue in template1 as admin to create db
-- ----------------------------------------------------------------------------
CREATE DATABASE bison_input ENCODING='UTF8'
                    LC_COLLATE='en_US.UTF-8'
                    LC_CTYPE='en_US.UTF-8'
                    TEMPLATE=template1;

-- ----------------------------------------------------------------------------
\c bison_input
-- ----------------------------------------------------------------------------

CREATE SCHEMA lmb;
ALTER DATABASE "bison_input" SET search_path=lmb,public;
GRANT USAGE ON SCHEMA lmb TO admin;
