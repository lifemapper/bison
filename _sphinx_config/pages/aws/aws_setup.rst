AWS Resource Setup
********************

Create policies and roles
===========================================================

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

EC2 instance creation
===========================================================

Create (Console)
--------------------------------
* Future - create and save an AMI or template for consistent reproduction
* via Console, without launch template:

  * Ubuntu Server 24.04 LTS, SSD Volume Type (free tier eligible), Arm architecture
  * Instance type t4g.micro (1gb RAM, 2 vCPU)
  * Security Group: launch-wizard-1
  * 15 Gb General Purpose SSD (gp3)
  * Modify `IAM instance profile` - to role created for s3 access (bison_ec2_s3_role)
  * Use the security group created for this region (currently launch-wizard-1)
  * Assign your key pair to this instance

    * If you do not have a keypair, create one for SSH access (tied to region) on initial
      EC2 launch
    * One chance only: Download the private key (.pem file for Linux and OSX) to local
      machine
    * Set file permissions to 400

  * Launch
  * Test by SSH-ing to the instance with the Public IPv4 DNS address, with efault user
    (for ubuntu instance) `ubuntu`::

    ssh  -i .ssh/<aws_keyname>.pem  ubuntu@<ec2-xxx-xxx-xxx-xxx.compute-x.amazonaws.com>


Install software on EC2
===========================================================

Baseline
------------
* update apt
* install apache for getting/managing certificates
* install certbot for Let's Encrypt certificates
* install docker for BISON deployment::

    sudo apt update
    sudo apt install apache2 certbot plocate unzip
    sudo apt install docker.io
    sudo apt install docker-compose-v2

AWS Client tools
--------------------

* Use instructions to install the awscli package (Linux):
  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html.
* Make sure to use the instructions with the right architecture (x86 vs Arm)
* Test by listing the contents of bison bucket (permission from role bison_ec2_s3_role)::

  aws s3 ls s3://bison-321942852011-us-east-1/input/

SSL certificates
------------------

* Create an SSL certificate on the EC2 instance.
* For testing/development, use self-signed certificates because Cerbot will not create
  certificates for an AWS EC2 Public IPv4 DNS, or an IP address.

  * Edit the docker-compose.yml file under `nginx` service (which intercepts all web
    requests) in `volumes` to bind-mount the directory containing self-signed
    certificates to /etc/letsencrypt::

    services:
    ...
      nginx:
      ...
      volumes:
        - "/home/ubuntu/certificates:/etc/letsencrypt:ro"

BISON code
---------------------

* Download the BISON code repository::

  git clone https://github.com/lifemapper/bison.git

* Edit the .env.conf (Docker environment variables) and nginx.conf (webserver address)
  files with the FQDN of the server being deployed. For development/testing EC2 servers,
  use the Public IPv4 DNS for the EC2 instance.

Launch BISON docker instances
-----------------------------------
