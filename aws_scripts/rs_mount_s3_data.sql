-- Mount S3 GBIF Open Data Registry as an external table, then subset it for BISON

-- Create a schema for mounting external data
CREATE external schema redshift_spectrum
    FROM data catalog
    DATABASE dev
    IAM_ROLE 'arn:aws:iam::321942852011:role/service-role/AmazonRedshift-CommandsAccessRole-20231129T105842'
    CREATE external database if NOT exists;

-- Mount a table for subset of GBIF ODR data in S3
CREATE EXTERNAL TABLE redshift_spectrum.occurrence_2023_12_parquet (
    gbifid	VARCHAR(max),
    datasetkey	VARCHAR(max),
    occurrenceid	VARCHAR(max),
    kingdom	VARCHAR(max),
    phylum	VARCHAR(max),
	class	VARCHAR(max),
	order	VARCHAR(max),
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
    LOCATION 's3://gbif-open-data-us-east-1/occurrence/2023-12-01/occurrence.parquet/';

-- Create a BISON table with a subset of records and subset of fields

SELECT create_time FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'dev' AND table_name = 'public.bison_subset';

DROP TABLE public.bison_subset;
CREATE TABLE public.bison_subset AS
	SELECT
		gbifid, species, taxonrank, scientificname, countrycode, stateprovince,
		occurrencestatus, publishingorgkey, day, month, year, taxonkey, specieskey,
		basisofrecord,
		ST_Makepoint(decimallongitude, decimallatitude) as geom
	FROM redshift_spectrum.occurrence_2023_12_parquet
	WHERE countrycode = 'US'
	  AND decimallatitude IS NOT NULL
	  AND decimallongitude IS NOT NULL
	  AND occurrencestatus = 'PRESENT'
	  -- https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/Rank.html
	  AND taxonrank IN
		('SPECIES', 'SUBSPECIES', 'FORM', 'INFRASPECIFIC_NAME', 'INFRASUBSPECIFIC_NAME')
	  -- https://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/BasisOfRecord.html
	  AND basisofrecord IN
	    ('HUMAN_OBSERVATION', 'OBSERVATION', 'OCCURRENCE', 'PRESERVED_SPECIMEN');

SELECT COUNT(*) FROM public.bison_subset;

SELECT reloid AS tableid, nspname as schemaname, relname as tablename, relcreationtime
FROM pg_class_info cls LEFT JOIN pg_namespace ns ON cls.relnamespace=ns.oid
WHERE cls.relnamespace = ns.oid
  AND schemaname = 'public';

SELECT * FROM SVV_EXTERNAL_DATABASES WHERE
databasename = 'dev';

SELECT COUNT(*) from dev.redshift_spectrum.occurrence_2023_12_parquet;