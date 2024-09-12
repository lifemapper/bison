Roles, Policies, Trust Relationships
=========================================

.. _bison_redshift_s3_role:

bison_redshift_s3_role
------------------------------

* Trusted entity type: AWS Service
* for Redshift - Customizable

  * TODO: change to Redshift - Scheduler when automated

* Policies:

  * AmazonRedshiftAllCommandsFullAccess (AWS managed)
  * AmazonRedshiftDataFullAccess (AWS managed)
  * AmazonRedshiftFullAccess (AWS managed)
  * bison_s3_policy (read public/GBIF S3 data and read/write bison S3 data)
  * redshift_glue_policy.json (for Redshift interactions)

  * AmazonS3FullAccess (AWS managed)

* Trust policy:

  *

bison_redshift_lambda_role
------------------------

Attach to BISON lambda functions

  * AmazonRedshiftAllCommandsFullAccess (AWS managed)
  * AmazonRedshiftDataFullAccess (AWS managed)
  * AmazonRedshiftFullAccess (AWS managed)
  * bison_lambda_log_policy (write CloudWatch logs to bison log groups)
    TODO: add new log group for each lambda function
  * bison_s3_policy (read public/GBIF S3 data and read/write bison S3 data)

.. _bison_ec2_s3_role:

bison_ec2_s3_role
------------------------------

* Trusted entity type: AWS Service
* for S3
* Includes policies:

  * bison_s3_policy.json (read public/GBIF S3 data and read/write bison S3 data)
  * SecretsManagerReadWrite (AWS managed)

* Trust relationship:

  * ec2_s3_role_trust_policy.json edit trust policy for both ec2 and s3
