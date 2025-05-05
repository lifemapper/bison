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
  * (no?) Use the bison-ec2-role for this instance
  * Assign your key pair to this instance

    * If you do not have a keypair, create one for SSH access (tied to region) on initial
      EC2 launch
    * One chance only: Download the private key (.pem file for Linux and OSX) to local
      machine
    * Set file permissions to 400

  * Launch
  * Test by SSH-ing to the instance with the Public IPv4 DNS address, with default user
    (for ubuntu instance) `ubuntu`::

    ssh  -i .ssh/<aws_keyname>.pem  ubuntu@<ec2-xxx-xxx-xxx-xxx.compute-x.amazonaws.com>

Create an SSH key for Github clone
-----------------------------------------------

* Generate an SSH key::

    ssh-keygen -t ed25519 -C "bison@whereever"

* Add the public key to your Github profile,
  https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent


Install software
---------------------------

* Update apt and install unzip::

    sudo apt update
    sudo apt install unzip

* AWS Client tools

    * Use instructions to install the awscli package (Linux):
      https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html.
    * Make sure to use the instructions with the right architecture (x86 vs Arm)
    * Test by listing the contents of bison bucket (permission from role bison_ec2_s3_role)::

        aws s3 ls s3://bison-321942852011-us-east-1/input/

* install docker for BISON application deployment::

    sudo apt install docker.io
    sudo apt install docker-compose-v2

* BISON code (for building docker image during development/testing)

    * Download the BISON code repository::

      git clone https://github.com/lifemapper/bison.git

    * Edit the .env.conf (Docker environment variables) and nginx.conf (webserver address)
      files with the FQDN of the server being deployed. For development/testing EC2 servers,
      use the Public IPv4 DNS for the EC2 instance.

for API deployment
----------------------------------
SSL certificates
...................

* install apache for getting/managing certificates
* install certbot for Let's Encrypt certificates::

    sudo apt install apache2 certbot plocate

* Create an SSL certificate on the EC2 instance.
* For testing/development, use self-signed certificates because Cerbot will not create
  certificates for an AWS EC2 Public IPv4 DNS, or an IP address.

  * Edit the compose.yml file under `nginx` service (which intercepts all web
    requests) in `volumes` to bind-mount the directory containing self-signed
    certificates to /etc/letsencrypt::

    services:
    ...
      nginx:
      ...
      volumes:
        - "/home/ubuntu/certificates:/etc/letsencrypt:ro"

Configure for AWS access
--------------------

In the home directory, create the directory and file .aws/config, with the following
content::

    [default]
    region = us-east-1
    output = json
    duration_seconds = 43200
    credential_source = Ec2InstanceMetadata


EC2 for Workflow Tasks
---------------------------------

Credentials
..............

EC2 must be set up with a role for temporary credentials to enable applications to
retrieve those credentials for AWS permissions to other services (i.e. S3).
By default, the instance allows IMDSv1 or IMDSv2, though making v2 required is recommended.

TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"` \
&& curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/iam/security-credentials/s3access

Using IMDSv2, first get a token::

    TOKEN=`curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"`

Then get top level metadata::

    curl -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/

To set up config to use/assume a role:
https://docs.aws.amazon.com/sdkref/latest/guide/feature-assume-role-credentials.html

More info:

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html

Hop Limit for AWS communication
................................

* Extend the hop limit for getting metadata about permissions to 2
  host --> dockercontainer --> metadata
  https://specifydev.slack.com/archives/DQSAVMMHN/p1717706137817839

* SSH to the ec2 instance, then run ::

    aws ec2 modify-instance-metadata-options \
        --instance-id i-082e751b94e476987 \
        --http-put-response-hop-limit 2 \
        --http-endpoint enabled

* or in console, add metadata tag/value HttpPutResponseHopLimit/2

EC2/Docker setup
....................

* Create the first EC2 Launch Template as a "one-time" Spot instance, no hibernation

* The Launch template should have the following settings::

  Name: bison_spot_task
  Application and OS Images: Ubuntu
  AMI: Ubuntu 24.04 LTS
  Architecture: 64-bit ARM
  Instance type: t4g.micro
  Key pair: bison-task-key
  Network settings/Select existing security group: launch-wizard-1
  Configure storage: 8 Gb gp3 (default)
    Details - encrypted
  Advanced Details:
    IAM instance profile: bison_ec2_s3_role
    Shutdown behavior: Terminate
    Cloudwatch monitoring: Enable
    Purchasing option: Spot instances
    Request type: One-time

* Use the launch template to create a version for each task.
* The launch template task versions must have the task name in the description, and
  have the following script in the userdata::

    #!/bin/bash
    sudo apt-get -y update
    sudo apt-get -y install docker.io
    sudo apt-get -y install docker-compose-v2
    git clone https://github.com/lifemapper/bison.git
    cd bison
    sudo docker compose -f compose.test_task.yml up
    sudo shutdown -h now


* For each task **compose.test_task.yml** must be replaced with the appropriate compose file.
* On EC2 instance startup, the userdata script will execute
* The compose file sets an environment variable (TASK_APP) containing a python module
  to be executed from the Dockerfile.
* Tasks should deposit outputs and logfiles into S3.
* After completion, the docker container will stop automatically and the EC2 instance
  will stop because of the shutdown command in the final line of the userdata script.
* **TODO**: once the workflow is stable, to eliminate Docker build time, create a Docker
  image and download it in userdata script.
