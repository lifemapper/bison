AWS Setup
####################

Security
**********************

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

Create an Policies for the EC2/S3 interaction
===========================================================

* Create a Policy for EC2 instance access to S3

  * bison_s3_policy
  * allowing s3:ListBucket, s3:GetBucket, s3:GetObject, and s3:object-lambda
  * to the Bison bucket and input, output, log, and summary subfolders.
  * Make sure to add each subfolder - permissions are not recursive.

* Create a Policy for Redshift / S3 access interaction


  2. Trusted entity type = AWS service, Use Case = S3.

  3. Add permissions

    * specnet_S3bucket_FullAccess

  4. Save and name role (specnet_ec2_role)
