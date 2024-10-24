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

EC2/Docker setup
....................

* The EC2 Spot instance should have the following settings::

  Name: bison_spot_task
  Application and OS Images: Ubuntu
  AMI: Ubuntu Server 24.04 LTS
  Architecture: 64-bit ARM
  Instance type: t4g.nano (maybe)
  Key pair: bison-task-key
  Network settings/Select existing security group: launch-wizard-1
  Configure storage: 8 Gb gp3 (default)
  Advanced Details:
    IAM instance profile: bison_ec2_s3_role
    Purchasing option: Spot instances

* Pull the template from the first instance and use it to create each new instance.
* The EC2 instance should have the following in the userdata::

    #!/bin/bash
    sudo apt update
    sudo apt install docker.io
    sudo apt install docker-compose-v2
    git clone https://github.com/lifemapper/bison.git
    cd bison
    sudo docker compose -f <compose.test_task.yml> up

* For each task compose.test_task.yml must be replaced with the appropriate compose file.
* On EC2 instance startup, the userdata script will execute
* The compose file sets an environment variable (TASK_APP) containing a python module
  to be executed from the Dockerfile.
* Tasks should deposit outputs and logfiles into S3.
* After completion, the docker container will stop.
* A final command should prune the system and volumes, to ensure the next execution
  runs the most current code.
* **TODO**: once the workflow is stable, create an image and download it in userdata script.
  That will also eliminate the build time, and allow a new EC2 spot instance for every
  task.

Lambda setup
....................

Triggering execution
-------------------------
The first step may be executed on a schedule, such as the second day of the month (since
GBIF data is deposited on the first day of the month).

Upon successful completion, the deposition of successful output into S3 can trigger
following steps.
