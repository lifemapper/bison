SELECT gbif.*
FROM public.bison_subset_2023_12 AS subset,
     redshift_spectrum.occurrence_2023_12_parquet AS gbif
WHERE subset.census_state IS NULL AND subset.gbifid = gbif.gbifid
LIMIT 10;

SELECT count(*)
FROM public.bison_subset_2023_12 AS subset, redshift_spectrum.occurrence_2023_12_parquet AS gbif
WHERE subset.gbifid = gbif.gbifid
  AND subset.census_state IS NULL
  AND NOT (gbif.decimallatitude IS NULL OR gbif.decimallongitude IS NULL);


SELECT create_time FROM INFORMATION_SCHEMA.TABLES
WHERE table_catalog = 'dev'
  AND table_schema = 'public'
  AND table_name = 'bison_subset_2023_12';


SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE table_catalog = 'dev'
  AND table_schema = 'public';

SELECT * from pg_class_info a left join pg_namespace b on a.relnamespace=b.oid

SELECT * from pg_class_info LIMIT 10;
SELECT * from pg_namespace LIMIT 10;

select reloid as tableid,trim(nspname) as schemaname,trim(relname) as tablename,reldiststyle,releffectivediststyle,
CASE WHEN "reldiststyle" = 0 THEN 'EVEN'::text
     WHEN "reldiststyle" = 1 THEN 'KEY'::text
     WHEN "reldiststyle" = 8 THEN 'ALL'::text
     WHEN "releffectivediststyle" = 10 THEN 'AUTO(ALL)'::text
     WHEN "releffectivediststyle" = 11 THEN 'AUTO(EVEN)'::text
     WHEN "releffectivediststyle" = 12 THEN 'AUTO(KEY)'::text ELSE '<<UNKNOWN>>'::text END as diststyle,relcreationtime
from pg_class_info a left join pg_namespace b on a.relnamespace=b.oid
WHERE schemaname = 'public';

-- Get the table creation time
SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
WHERE cls.relnamespace = ns.oid
  AND schemaname = 'public'
  AND (tablename = 'bison_subset' OR tablename LIKE 'temp_%')
  AND relcreationtime > '2023-12-01';
