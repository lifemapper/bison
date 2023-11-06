==================
Process Data Year 5, 2023-2024
==================

Data ingestion and processing will be executed on Amazon Web Services (AWS), utilizing
several AWS tools.  Other data inputs will be placed in AWS resources, such as RDS or
S3, for easy access by AWS tools.  In order to minimize costs, we will experiment with
different data storage and processing strategies - they each have speed and cost pros
and cons.

Current decisions
---------------------------
* Region: us-east-1
* Inputs: same as , `Data Inputs <history/yr4_inputs.rst>`_

GBIF Data Ingestion
---------------------------
* **Option 1:** Subset GBIF Open Data Registry to Bison S3 bucket, serverless, script
  `glue_bison_subset_gbif.py <../../scripts/glue_bison_subset_gbif.py>`_
* **Option 2:** Query GBIF portal manually, then initiate an EC2 Spot instance to
  download and subset it, saving it to S3. The script that downloads, subsets, and
  uploads data from the EC2 Spot instance is installed on the EC2
  instance on creation, `user_data_for_ec2spot.py
  <../../scripts/user_data_for_ec2spot.py>`_.  The script that builds and instantiates
  the EC2 Spot instance is: `gbif_to_s3.py <../../scripts/gbif_to_s3.py>`_ .

Reference Data
-----------------
Reference data consists of US-RIIS data and geospatial data for intersections.
Reference data will reside on AWS S3, and will be updated manually when new versions
becomes available.  These data are uploaded to S3 manually.

As part of a workflow, a process will add the reference data in S3 to a database in RDS.
The database must first be created with the SQL script
`init_database.sql <../../scripts/init_database.sql>`_.  This script will initialize a
PostgreSQL database in an existing RDS instance, and add PostGIS extensions for
geospatial data and operations.

A subsequent part of a workflow will add the data to RDS with the script
`populate_rds.py <../../scripts/populate_rds.py>`_.  This handles both standard CSV
data (RIIS) and geospatial data (census, AIANNH, PAD).

Setup
---------------------------
* install aws-cli on local dev machine
* Add reference data to S3 manually
* create EC2 for test/debug connection
  * update/upgrade apt, install stuff
  * add aws config and credentials
* Populate RDS
    * add postgis to postgres:
      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html
    * insert RIIS
    * insert geospatial data: census boundaries, native lands, PAD

* Redshift?

Experiment
....................
* Find bucket, specify_athena

gbif_extract: 303237553
gbif_parquet_extract: 301669806

Use python libs **awscli** and **boto3** to connect with AWS

* query (Norway only):

  https://www.gbif.org/occurrence/download?basis_of_record=PRESERVED_SPECIMEN&basis_of_record=FOSSIL_SPECIMEN&basis_of_record=OCCURRENCE&country=NO&occurrence_status=present

* DwCA 9 GB data (2 GB zipped)
* 5,293,875 records
* download: https://www.gbif.org/occurrence/download/0098682-230530130749713



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
