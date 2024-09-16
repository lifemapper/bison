-- Load US RIIS into Redshift
-- Note: You MUST update the annotated RIIS filename with the most recent version
--   containing the current date (first day of current month),
--   i.e. US-RIIS_MasterList_2021_annotated_2024_02_01.csv'
-- Load US Registry of Introduced and Invasive Species (US-RIIS)
-- Must use underscores in table name, no dash
-- -------------------------------------------------------------------------------------
-- US Registry of Introduced and Invasive Species (US-RIIS)
-- -------------------------------------------------------------------------------------
-- DROP TABLE riis_2024_02_01;
CREATE TABLE riisv2_2024_08_01 (
	locality	VARCHAR(max),
	scientificname	VARCHAR(max),
	scientificnameauthorship	VARCHAR(max),
	vernacularname	VARCHAR(max),
	taxonrank	VARCHAR(max),
	establishmentmeans	VARCHAR(max),
	degreeofestablishment	VARCHAR(max),
	ishybrid	VARCHAR(max),
	pathway	VARCHAR(max),
	habitat	VARCHAR(max),
	biocontrol	VARCHAR(max),
	associatedtaxa	VARCHAR(max),
	eventremarks	VARCHAR(max),
	introdatenumber	VARCHAR(max),
	taxonremarks	VARCHAR(max),
	kingdom	VARCHAR(max),
	phylum	VARCHAR(max),
	class	VARCHAR(max),
	_order	VARCHAR(max),
    family	VARCHAR(max),
	taxonomicstatus	VARCHAR(max),
	itis_tsn	VARCHAR(max),
	gbif_taxonkey	VARCHAR(max),
	taxonid	VARCHAR(max),
	authority	VARCHAR(max),
	weblink	VARCHAR(max),
	associatedreferences	VARCHAR(max),
	eventdate	VARCHAR(max),
	modified	VARCHAR(max),
	update_remarks	VARCHAR(max),
	occurrenceremarks	VARCHAR(max),
	occurrenceid	VARCHAR(max),
	gbif_res_taxonkey	VARCHAR(max),
	gbif_res_scientificname	VARCHAR(max),
	lineno	VARCHAR(max)
);

COPY riisv2_2024_08_01
FROM 's3://bison-321942852011-us-east-1/input/USRIISv2_MasterList_annotated_2024_08_01.csv'
FORMAT CSV
IAM_role DEFAULT;

SELECT COUNT(*) FROM riisv2_2024_08_01;
SELECT * FROM riisv2_2024_08_01 LIMIT 10;
