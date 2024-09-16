-- Get last error message
SELECT query_id, start_time, line_number, column_name, column_type, error_message
    FROM sys_load_error_detail ORDER BY start_time DESC;

-- Count post-data load
SELECT COUNT(*) from dev.redshift_spectrum.occurrence_2024_09_01_parquet;
SELECT COUNT(*) FROM public.bison_2024_09_01;

-- List Redshift tables and creation times
SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
WHERE cls.relnamespace = ns.oid
  AND schemaname = 'public';


SELECT * FROM svv_all_schemas WHERE database_name = 'dev'
ORDER BY database_name, SCHEMA_NAME;

SELECT current_user;

GRANT CREATE
    ON DATABASE dev
    TO IAMR:bison_redshift_lambda_role
SELECT has_database_privilege('IAMR:bison_redshift_lambda_role', 'dev', 'create');

GRANT ALL
	ON SCHEMA public
	TO IAMR:bison_redshift_lambda_role
SELECT has_schema_privilege('IAMR:bison_redshift_lambda_role','public','usage');

SELECT has_table_privilege( 'IAMR:bison_redshift_lambda_role', 'aiannh_counts_2024_08_01', 'usage')

SELECT * FROM PG_USER_INFO;

SHOW DATABASES FROM DATA CATALOG IAM_ROLE default ;
SHOW SCHEMAS FROM DATABASE dev;
SHOW TABLES FROM SCHEMA dev.public LIKE '%_2024_08_01';
