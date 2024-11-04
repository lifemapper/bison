Create lambda function to initiate processing
--------------------------------------------

Lambda functions can execute tasks in a workflow.  They can be initiated when a trigger
condition occurs, set (separately) in the Amazon EventBridge as either Rules or
Schedules.

Lambda functions can only execute for up to 15 minutes at a time, but can launch longer
running processes in another service, such as launch an EC2 instance to run a
BISON workflow tasks.

Lambda functions log directly to AWS Cloudwatch - make sure the role executing
the lambda function has Create/Write permissions to AWS Cloudwatch.

Edit the Configuration for lambda function
--------------------------------------------

* General: Timeout to an appropriate value for this task.
* Permissions: Execution role to the Workflow role (bison_redshift_lambda_role)
* Asynchronous Invokation: Set Maximum age of event and Retry attempts.
* By default, Lambda functions produce a Cloudwatch Logs stream.

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
