Create lambda function to initiate processing
--------------------------------------------
* Create a lambda function for execution when the trigger condition is activated,
  i.e.  aws/lambda/bison_s0_test_task_lambda.py
* This trigger condition can be either a schedule (i.e. midnight on the second day of
  every month) or a rule (i.e. file matching xxx* deposited in an S3 bucket)

Edit the execution role for lambda function
--------------------------------------------
* Under Configuration/Permissions set the Execution role to the Workflow role
  (bison_redshift_lambda_role)



Lambda to query Redshift
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

Lambda to start EC2 for task
--------------------------------------------

Lambda functions must be single-function tasks that run in less than 15 minutes.
For complex or long-running tasks we start an EC2 instance containing bison code
and execute it in a docker container.

For each task, the lambda function should create a Spot EC2 instance with a template
containing userdata that will either 1) pull the Github repo, then build the docker
image, or 2) pull a docker image directly.

Annotating the RIIS records with GBIF accepted taxa takes about 1 hour and uses
multiple bison modules.