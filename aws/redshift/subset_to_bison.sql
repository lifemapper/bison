
-- Mount S3 GBIF Open Data Registry as an external table, then subset it for BISON

-------------------
-- Set variables
-------------------
-- TODO: Script the previous and current dataset date for table names and S3 location
-- TODO: Script to pull the IAM_Role from a variable

-- -------------------------------------------------------------------------------------
-- Mount GBIF
-- -------------------------------------------------------------------------------------
-- Create a schema for mounting external data
-- Throws error if pre-existing??
-- This also creates a new external database "dev", though it appears in the console to
--    be the same "dev" database that contains the public schema.
DROP EXTERNAL SCHEMA redshift_spectrum;
CREATE EXTERNAL SCHEMA IF NOT EXISTS redshift_spectrum
    FROM data catalog
    DATABASE 'dev'
    IAM_ROLE default
    CREATE external database if NOT exists;

'arn:aws:redshift-serverless:us-east-1:321942852011:namespace/75c14076-70c7-43c3-8a7e-53425e1eb43e'

GRANT ASSUMEROLE
       ON 'arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role'
       TO ROLE IAMR:bison_redshift_lambda_role
       FOR ALL;

---- If change IAM role, do this:
----GRANT USAGE TO redshift_spectrum to "IAMR:bison_subset_gbif_lambda-role-9i5qvpux";
--GRANT ALL ON ALL TABLES IN SCHEMA redshift_spectrum
--    TO ROLE 'arn:aws:iam::321942852011:role/service-role/bison_subset_gbif_lambda-role-9i5qvpux';
--GRANT ALL ON ALL TABLES IN SCHEMA redshift_spectrum
--    TO ROLE 'arn:aws:iam::321942852011:role/service-role/bison_redshift_lambda_role';


-- Mount a table of current GBIF ODR data in S3
-- An error indicating that the "dev" database does not exist, refers to the external
--    database, and may indicate that the role used by the command and/or namespace
--    differs from the role granted to the schema upon creation.
CREATE EXTERNAL TABLE redshift_spectrum.occurrence_2024_09_01_parquet (
    gbifid	VARCHAR(max),
    datasetkey	VARCHAR(max),
    occurrenceid	VARCHAR(max),
    kingdom	VARCHAR(max),
    phylum	VARCHAR(max),
	class	VARCHAR(max),
	_order	VARCHAR(max),
	family	VARCHAR(max),
	genus	VARCHAR(max),
	species	VARCHAR(max),
	infraspecificepithet	VARCHAR(max),
	taxonrank	VARCHAR(max),
	scientificname	VARCHAR(max),
	verbatimscientificname	VARCHAR(max),
	verbatimscientificnameauthorship	VARCHAR(max),
	countrycode	VARCHAR(max),
	locality	VARCHAR(max),
	stateprovince	VARCHAR(max),
	occurrencestatus	VARCHAR(max),
	individualcount     INT,
    publishingorgkey	VARCHAR(max),
	decimallatitude	DOUBLE PRECISION,
	decimallongitude	DOUBLE PRECISION,
	coordinateuncertaintyinmeters	DOUBLE PRECISION,
	coordinateprecision	DOUBLE PRECISION,
	elevation	DOUBLE PRECISION,
	elevationaccuracy	DOUBLE PRECISION,
	depth	DOUBLE PRECISION,
	depthaccuracy	DOUBLE PRECISION,
	eventdate	TIMESTAMP,
	day	INT,
	month	INT,
	year	INT,
	taxonkey	INT,
	specieskey	INT,
	basisofrecord	VARCHAR(max),
	institutioncode	VARCHAR(max),
	collectioncode	VARCHAR(max),
	catalognumber	VARCHAR(max),
	recordnumber	VARCHAR(max),
    identifiedby	SUPER,
	dateidentified	TIMESTAMP,
	license	VARCHAR(max),
	rightsholder	VARCHAR(max),
	recordedby	SUPER,
	typestatus	SUPER,
	establishmentmeans	VARCHAR(max),
	lastinterpreted	TIMESTAMP,
	mediatype	SUPER,
	issue    SUPER
)
    STORED AS PARQUET
    LOCATION 's3://gbif-open-data-us-east-1/occurrence/2024-09-01/occurrence.parquet/';


-- -------------------------------------------------------------------------------------
-- Subset for BISON
-- -------------------------------------------------------------------------------------
-- Drop previous table;
DROP TABLE IF EXISTS public.bison_2024_08_01;
-- Create a BISON table with a subset of records and subset of fields
-- TODO: This includes lat/lon, allowing final export to Parquet after deleting geom
CREATE TABLE public.bison_2024_09_01 AS
	SELECT
		gbifid, datasetkey, species, taxonrank, scientificname, countrycode, stateprovince,
		occurrencestatus, publishingorgkey, day, month, year, taxonkey, specieskey,
		basisofrecord, decimallongitude, decimallatitude,
		ST_Makepoint(decimallongitude, decimallatitude) as geom
	FROM redshift_spectrum.occurrence_2024_09_01_parquet
	WHERE decimallatitude IS NOT NULL
	  AND decimallongitude IS NOT NULL
	  AND countrycode = 'US'
	  AND occurrencestatus = 'PRESENT'
	  -- https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/Rank.html
	  AND taxonrank IN
		('SPECIES', 'SUBSPECIES', 'VARIETY', 'FORM', 'INFRASPECIFIC_NAME', 'INFRASUBSPECIFIC_NAME')
	  -- https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/BasisOfRecord.html
	  AND basisofrecord IN
	    ('HUMAN_OBSERVATION', 'OBSERVATION', 'OCCURRENCE', 'PRESERVED_SPECIMEN');

-- -------------------------------------------------------------------------------------
-- Unmount original GBIF data
-- -------------------------------------------------------------------------------------
DROP TABLE redshift_spectrum.occurrence_2024_09_01_parquet;
