#######################
AWS workflow experiments
#######################

***************
AWS Batch
***************

Preparation
------------

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
-------------

Prep:
............
* Create a Docker image from osgeo/gdal with
  * python dependencies
  * bison code
  * reference data
* Save on S3 or Github

Input data acquisition:
............
* Create EC2 spot image

  * download data from GBIF
    Test bison dataset from 9/25/2023:  0006607-230918134249559
  * copy to S3

Input data prep:
............
* Create a Step Workflow to:

  * crawl for metadata
  * copy to a Glue database
  * add annotation fields
  * count records and identify subsets

Batch
............
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

***************************
Glue - Interactive Development
***************************

`AWS Glue Studio with Jupyter
<https://docs.aws.amazon.com/glue/latest/dg/create-notebook-job.html>`_

`Local development with Jupyter
<https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html>`_


Problem/Solution
--------------------
Interactive Samples fail with error (File Not Found) for public GBIF data

* Create database in AWS Glue for metadata about project inputs

  * In the DB, create table for each data input, using Glue Crawler

Problem/Solution
--------------------

Interactive Data Preview fails for public and private data

* Use AWS Glue DataBrew to visually examine data

  * First add dataset to Glue Data Catalog
        "A table is the metadata definition that represents your data, including its
        schema. A table can be used as a source or target in a job definition."
  * Next add dataset to Glue DataBrew

Problem/Solution
--------------------
AWS Glue DataBrew add dataset, create connection to RDS, shows no tables in
bison-metadata database.

DataBrew for visual representation of data, not examination


***************************
BISON AWS data/tools
***************************

* Amazon RDS, PostgreSQL, bison-db-test

    * Create JDBC connection from Crawler, then change to Amazon RDS to bison-test-db/%

* AWS Glue Data Catalog

  * bison-metadata (glue) Database, populated by
  * AWS Glue Crawler, crawls data to create tables of metadata/schema
    * GBIF Crawler to crawl GBIF Open Data Registry 11-2023 --> gbif-odr-occurrence_parquet table
    * Does Glue Crawler only access S3?

* To connect to RDS, add Glue/Data Catalog/Connection

    * endpoint: bison-db-test.cqvncffkwz9t.us-east-1.rds.amazonaws.com
    * dbname: bison_db_test
    * connection url: jdbc:postgresql://bison-db-test.cqvncffkwz9t.us-east-1.rds.amazonaws.com:5432/bison_db_test

    * "InvalidInputException: Unable to resolve any valid connection"
      Docs point to error logs in /aws-glue/testconnection/output, but this does not exist
      check https://repost.aws/knowledge-center/glue-test-connection-failed

    * Added RDS database VPC, 1 subnet, all 3 security groups
    * Result: InvalidInputException:
        At least one security group must open all ingress ports.To limit traffic, the
        source security group in your inbound rule can be restricted to the same
        security group



        Add policy to my user:
        https://docs.aws.amazon.com/glue/latest/dg/configure-iam-for-glue.html
        Added policy according to instructions in Step 3, verbatim -
        Error:

Permissions Solution:
--------------------
Add AdministratorAccess  to Role, the audit the calls later to identify
minimum permissions needed.