Workflow Automation
#####################################

Lambda Functions For Workflow Steps
=====================================

Overview
----------
Lambda functions:

* can run for 15 minutes or less
* must contain only a single function

Therefore, long-running processes and computations that require a complex programming
are less suitable for Lambda.  The alternative we use for this workflow is to use
Lambda to launch an EC2 instance to complete the processing.

For the BISON workflow the first step is to annotate RIIS records with GBIF accepted
taxa, a process that takes 30-60 minutes to resolve the approximately 15K names in RIIS.

The final step is to build 2d matrices, species by region, from the data, and compute
biogeographic statistics on them.  This process requires more complex code which is
present in the BISON codebase.

In both cases, we install the code onto the newly launched EC2 instance, and build a
Docker container to install all dependencies and run the code.

In future iterations, we will download a pre-built Docker image.

More detailed setup instructions in lambda


Initiate Workflow on a Schedule
------------------------------------------------

Step 1: Annotate RIIS with GBIF accepted taxa
......................................

This ensures that we can match RIIS records with the GBIF records that we
will annotate with RIIS determination.  This process requires sending the scientific
name in the RIIS record to the GBIF 'species' API, to find the accepted name,
`acceptedScientificName` (and GBIF identifier, `acceptedTaxonKey`).

* Create an AWS EventBridge Schedule

* Create a lambda function for execution when the trigger condition is activated, in
  this case, the time/date in the schedule.
  aws/lambda/bison_s0_annotate_riis_lambda.py

  * The lambda function will make sure the data to be created does not already exist
    in S3, execute if needed, return if it does not.



Triggering execution
-------------------------
The first step will be executed on a schedule, such as the second day of the month
(GBIF data is deposited on the first day of the month).

Scheduled execution (Temporary): Each step after the first, is also executed on a
schedule, roughly estimating completion of the previous step.  These steps with a
dependency on previous outputs will first check for the existence of required inputs,
failing immediately if inputs are not present.

Automatic execution (TODO):  The successful deposition of output of the first
(scheduled) and all following steps into S3 or Redshift triggers subsequent steps.

Both automatic and scheduled execution will require examining the logs to ensure
successful completion.


TODO: Create rule to initiate lambda function based on previous step
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
