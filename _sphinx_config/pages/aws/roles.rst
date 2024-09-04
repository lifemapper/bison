Roles, Policies, Trust Relationships
=========================================

.. _bison_redshift_s3_role:

bison_redshift_s3_role
------------------------------

* Trusted entity type: AWS Service
* for Redshift - Customizable

  * TODO: change to Redshift - Scheduler when automated

* Policies:

  * bison_s3_policy (read public/GBIF S3 data and read/write bison S3 data)
  * redshift_glue_policy.json (for Redshift interactions)
  * AmazonRedshiftAllCommandsFullAccess (AWS managed)
  * AmazonS3FullAccess (AWS managed)

* Trust policy:

  *

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
