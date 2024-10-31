AWS Resource Setup
###################

Security
********************

Create policies and roles
===========================================================


The :ref:`_bison_redshift_lambda_role` allows access to the bison Redshift
namespace/workgroup, lambda functions, EventBridge Scheduler, and S3 data.
The Trusted Relationships on this policy allow each to

The :ref:`_bison_redshift_lambda_role_trusted_relationships policy allow

The :ref:`_bison_ec2_s3_role` allows an EC2 instance to access the public S3 data and
the bison S3 bucket.  Its trust relationship grants AssumeRole to ec2 and s3 services.
This role will be assigned to an EC2 instance that will initiate
computations and compute matrices.

The :ref:`_bison_redshift_s3_role` allows Redshift to access public S3 data and
the bison S3 bucket, and allows Redshift to perform glue functions. Its trust
relationship grants AssumeRole to redshift service.

Make sure that the same role granted to the namespace is used for creating an external
schema and lambda functions.  When mounting external data as a redshift table to the
external schema, you may encounter an error indicating that the "dev" database does not
exist.  This refers to the external database, and may indicate that the role used by the
command and/or namespace differs from the role granted to the schema upon creation.

Create a Security Group for the region
===========================================================

* Test this group!
* Create a security group for the project/region

  * inbound rules allow:

    * Custom TCP, port 8000
    * Custom TCP, port 8080
    * HTTPS, port 80
    * HTTPS, port 443
    * SSH, port 22

  * Consider restricting SSH to campus

* or use launch-wizard-1 security group (created by some EC2 instance creation in 2023)

  * inbound rules IPv4:

    * Custom TCP 8000
    * Custom TCP 8080
    * SSH 22
    * HTTP 80
    * HTTPS 443

  * outbound rules IPv4, IPv6:

    * All traffic all ports

Redshift Namespace and Workgroup
===========================================================

Namespace and Workgroup
------------------------------

A namespace is storage-related, with database objects and users.  A workspace is
a collection of compute resources such as security groups and other properties and
limitations.
https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-workgroup-namespace.html

External Schema
------------------------
The command below creates an external schema, redshift_spectrum, and also creates a
**new** external database "dev".  It appears in the console to be the same "dev"
database that contains the public schema, but it is separate.  Also note the IAM role
used to create the schema must match the role attached to the namespace::

    CREATE external schema redshift_spectrum
        FROM data catalog
        DATABASE dev
        IAM_ROLE 'arn:aws:iam::321942852011:role/bison_redshift_s3_role'
        CREATE external database if NOT exists;
