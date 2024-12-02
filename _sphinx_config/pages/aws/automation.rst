Workflow Automation
#####################################

Lambda Functions For Step Execution
=====================================

Overview
----------
Lambda functions:

* may run code using AWS data or resources.
* can run for 15 minutes or less
* must contain only a single function

Therefore, long-running processes and computations that require a complex programming
are less suitable for Lambda.

BISON Steps
----------------
The first and last steps of the BISON workflow both use BISON code modules and run for
longer than the Lambda 15 minute limit.  For each of these steps, the lambda function
launches an EC2 instance to build a Docker container to execute the process. In
future iterations, we will download a pre-built Docker image.

Other BISON steps run processes reading, writing, or analyzing data on S3 and Redshift.

Lambda-launched EC2 tasks
------------------------------
EC2 instances launched by BISON workflow lambda functions use an EC2 Launch Template.
There are multiple versions of the launch template, one for each task.  Each task
version contains a userdata script that executes the Docker compose command  with
a compose file specific to that task.  Compose files are at the root of this repository.
Userdata scripts are in **aws/userdata/** directory of this repo.  Task scripts used in
the compose files are in the **bison/task** directory.

Initiate Steps with AWS Eventbridge Schedule
==============================

The first step will be executed on a schedule, such as the second day of the month
(GBIF data is deposited on the first day of the month).

Each step after the first is also executed on a schedule, scheduled some appropriate
amount of time after expected completion of the previous step.  Steps with a
dependency on previous outputs will first check for the existence of required inputs,
failing immediately if inputs are not present.

For each step, create an AWS EventBridge Schedule

* Detail: name matching the lambda function it will execute, recurring, cron expression
  for the 2nd day of the month, and a time adequate for the previous step to complete.
* target: All APIs, AWS Lambda Invoke, choose lambda function to execute
* Execution role: the same role given to the lambda function, allowing access to
  redshift, S3, EC2, Cloudwatch, etc (bison_redshift_lambda_role)
* Retry policy: 1 day, 5 times


The BISON Workflow
=====================================

For the BISON workflow the first step is to annotate RIIS records with GBIF accepted
taxa, a process that takes 30-60 minutes to resolve the approximately 15K names in RIIS.

The final step is to build 2d matrices, species by region, from the data, and compute
biogeographic statistics on them.  This process requires more complex code which is
present in the BISON codebase.

All steps, including both Lambda functions, and EC2 instances launched by Lambda, log
statements to Cloudwatch logs.  Lambda functions log to a Log Group named identically
to the lambda function name.  Docker logs from the EC2 instance log to the 'bison_task'
LogGroup, with the stream named the same as the task name.

Code for all lambda steps is in **bison/aws/lambda/**.

Preconditions:
--------------------

In S3:

* In the BISON bucket, create sub folders:

  * input
  * log
  * output
  * summary

* Update the **AWS constants** in common/constants.py with the correct account number,
  bucket name, region,
Load ancillary data and most recent RIIS data into an accessible S3 bucket,

Step 1: Annotate RIIS with GBIF accepted taxa
--------------------------------------------------

This ensures that we can match RIIS records with the GBIF records that we
will annotate with RIIS determination.  This process requires sending the scientific
name in the RIIS record to the GBIF 'species' API, to find the accepted name,
`acceptedScientificName` (and GBIF identifier, `acceptedTaxonKey`).

This script runs first in the workflow to ensure that the name resolution is current
with the GBIF data being processed.

The lambda function will make sure the data to be created does not already exist
in S3, execute if needed, return if it does not.

The EventBridge Schedule name and Lambda name is `bison_s1_annotate_riis`.  The
schedule cron expression is `0 0 2 * ? *` (min, hour, day of month, month, day of week, year)

Step 2: Load ancillary data from S3 into Redshift
--------------------------------------------------

TODO: Create trigger rule
==========================

Initiate lambda function based on outputs of previous step:

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
