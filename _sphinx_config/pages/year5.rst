=============================================
AWS strategy
=============================================

Data
---------------------------
* on us-east-1
* Find bucket, specify_athena

gbif_extract: 303237553
gbif_parquet_extract: 301669806

Use python libs **awscli** and **boto3** to connect with AWS

* query (Norway only):

  https://www.gbif.org/occurrence/download?basis_of_record=PRESERVED_SPECIMEN&basis_of_record=FOSSIL_SPECIMEN&basis_of_record=OCCURRENCE&country=NO&occurrence_status=present

* DwCA 9 GB data (2 GB zipped)
* 5,293,875 records
* download: https://www.gbif.org/occurrence/download/0098682-230530130749713


Setup
---------------------------
* install aws-cli on local dev machine
* Subset GBIF Open Data Registry to Bison S3 bucket, serverless,
  glue_bison_subset_gbif.py
* Handle input geospatial data
  * add shapefiles to S3 as zipfiles
  * add RIIS data to S3
  * create RDS, PostgreSQL
* create EC2 for test/debug connection
  * update/upgrade apt, install stuff
  * add aws config and credentials
* Populate RDS
    * add postgis to postgres:
      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html
    * insert census boundaries, native lands, PAD (scripts/populate_rds.py

* Redshift?

Workflow
---------------------------

* download GBIF data (~350 GB)

  * directly to EC2 instance using wget or script

* upload to S3

  * put-object with AWS CLI v2
    https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/put-object.html
  * AWS Python SDK put_object using Boto3
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html#

* pyspark
