AWS Batch
***************

1. Getting started

   * https://docs.aws.amazon.com/batch/latest/userguide/Batch_GetStarted.html
   * All these steps are already completed, up to and including install aws_cli

2. Create a VPC.

   * The default one is fine for now, it is vpc-09d4c9a0524b17382

3. Run AWS Batch first-run wizard to

   * set up roles

     Batch role (not yet created):
     arn:aws:iam::321942852011:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch

   * create a compute environment (EC2)

     * https://docs.aws.amazon.com/batch/latest/userguide/getting-started-ec2.html
     * https://us-east-1.console.aws.amazon.com/batch/home?region=us-east-1#wizard
       * choose EC2, r7g.medium AMI

   * Create Batch resources
   * Select orchestration type (EC2)
   * Create a compute environment
   * Create a job queue
   * Create a job definition
   * Create a job
   * Review and create

   * create a job definition, and
   * create a job queue

4. Docker image

   * docker pull ghcr.io/osgeo/gdal:alpine-small-latest

Workflow:
***************

Prep:
-----
* Create a Docker image from osgeo/gdal with
  * python dependencies
  * bison code
  * reference data
* Save on S3 or Github

Input data acquisition:
-----------------------
* Create EC2 spot image

  * download data from GBIF
    Test bison dataset from 9/25/2023:  0006607-230918134249559
  * copy to S3

Input data prep:
----------------
* Create a Step Workflow to:

  * crawl for metadata
  * copy to a Glue database
  * add annotation fields
  * count records and identify subsets

Batch
------
* Create AWS Batch Compute Environment

  * name
  * with EC2 orchestration
  * Create AWSServiceRoleForBatch, and InstanceRole

* Create AWS Batch Job Queue

  * Choose name, priority
  * Choose Batch Compute Environment from prev step

* Create AWS Batch Job Definition

  * Job definition name
  * Job role
  * Container image: Specify the URL of your Docker image stored in an S3 bucket or a
    public container registry.
  * Command: Define the command that should be executed within the container.

* Submit AWS Batch Job

