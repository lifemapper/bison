-- -------------------------------------------------------------------------------------
-- 1: Create role for interactions: bison_redshift_lambda_role (in docs/aws/roles.rst)
-- 2: Create/use default database (dev) and schema (public)
-- -------------------------------------------------------------------------------------
GRANT CREATE
    ON DATABASE dev
    TO IAMR:bison_redshift_lambda_role
SELECT has_database_privilege('IAMR:bison_redshift_lambda_role', 'dev', 'create');

GRANT ALL
	ON SCHEMA public
	TO IAMR:bison_redshift_lambda_role
SELECT has_schema_privilege('IAMR:bison_redshift_lambda_role','public','usage');

GRANT ALL
    ON ALL TABLES IN SCHEMA public
    TO IAMR:bison_redshift_lambda_role;

-- -------------------------------------------------------------------------------------
-- Setup external schema
-- -------------------------------------------------------------------------------------
-- Create a schema for mounting external data
-- This also creates a new external database "dev", though it appears in the console to
--    be the same "dev" database that contains the public schema.
DROP EXTERNAL SCHEMA redshift_spectrum;
CREATE EXTERNAL SCHEMA IF NOT EXISTS redshift_spectrum
    FROM data catalog
    DATABASE 'dev'
    IAM_ROLE default
    CREATE external database if NOT exists;
