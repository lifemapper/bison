Create lambda function to initiate processing
------------------------------------------------
* Create a lambda function for execution when the trigger condition is activated,
  aws/events/bison_find_current_gbif_lambda.py

  * This trigger condition is a file deposited in the BISON bucket

    * TODO: change to the first of the month

  * The lambda function will delete the new file, and test the existence of
    GBIF data for the current month

    * TODO: change to mount GBIF data in Redshift, subset, unmount

Edit the execution role for lambda function
--------------------------------------------
* Under Configuration/Permissions see the Execution role Role name
  (bison_find_current_gbif_lambda-role-fb05ks88) automatically created for this function
* Open in a new window and under Permissions policies, Add permissions

  * bison_s3_policy
  * redshift_glue_policy

Create trigger to initiate lambda function
------------------------------------------------

* Check for existence of new GBIF data
* Use a blueprint, python, "Get S3 Object"
* Function name: bison_find_current_gbif_lambda
* S3 trigger:

    * Bucket: arn:aws:s3:::gbif-open-data-us-east-1

* Create a rule in EventBridge to use as the trigger

  * Event source : AWS events or EventBridge partner events
  * Sample event, "S3 Object Created", aws/events/test_trigger_event.json
  * Creation method: Use pattern form
  * Event pattern

    * Event Source: AWS services
    * AWS service: S3
    * Event type: Object-Level API Call via CloudTrail
    * Event Type Specifications

      * Specific operation(s): GetObject
      * Specific bucket(s) by name: arn:aws:s3:::bison-321942852011-us-east-1

  * Select target(s)

    * AWS service


AWS lambda function that queries Redshift
--------------------------------------------

https://repost.aws/knowledge-center/redshift-lambda-function-queries

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data/client/execute_statement.html

* Connect to a serverless workgroup (bison), namespace (bison), database name (dev)

* When connecting to a serverless workgroup, specify the workgroup name and database
  name. The database user name is derived from the IAM identity. For example,
  arn:iam::123456789012:user:foo has the database user name IAM:foo. Also, permission
  to call the redshift-serverless:GetCredentials operation is required.
* need redshift:GetClusterCredentialsWithIAM permission for temporary authentication
  with a role
