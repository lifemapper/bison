Roles, Policies, Trust Relationships
=========================================

.. _bison_redshift_s3_role:

bison_redshift_s3_role
------------------------------

Attach to BISON namespace (Redshift)
* Regular role
* Trust relationships

  * service: "redshift.amazonaws.com"

* Policies:

  * AmazonRedshiftAllCommandsFullAccess (AWS managed)
  * AmazonRedshiftDataFullAccess (AWS managed)
  * AmazonRedshiftFullAccess (AWS managed)
  * bison_invoke_lambda_policy (invoke lambda functions starting with `bison`)
  * bison_lambda_log_policy (write CloudWatch logs to log groups starting with `bison`)
  * bison_s3_policy (read public/GBIF S3 data and read/write S3 data in bison bucket)
  * redshift_glue_policy.json (for Redshift interactions)

  * AmazonS3FullAccess (AWS managed)

* for Redshift - Customizable

  * TODO: change to Redshift - Scheduler when automated

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:us-east-1:321942852011:function:bison_s0_test_schedule:*",
                "arn:aws:lambda:us-east-1:321942852011:function:bison_s0_test_schedule"
            ]
        }
    ]
}

bison_redshift_lambda_role
------------------------

* Service role
* Trust relationships:
  ../../aws/bison_redshift_lambda_role_trusted_relationships.json

* Policies:

  * same as bison_redshift_s3_role

* In Redshift, GRANT permissions to database::

    GRANT CREATE
        ON DATABASE dev
        TO IAMR:bison_redshift_lambda_role

* Attached to BISON lambda functions
* Attach to BISON namespace (Redshift)



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
