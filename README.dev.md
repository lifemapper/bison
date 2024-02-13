### Pre-commit

* Instructions in [.pre-commit-config.yaml](.pre-commit-config.yaml)
* When running a commit (and the pre-commit hooks), if files are modified, make sure to
  restage them, then run commit again to ensure that changes are saved.

### Local Testing

* Include execution of tests in pre-commit hooks, example in
  [Specify7](https://github.com/specify/specify7/blob/production/.pre-commit-config.yaml)

* Create test file with first 100K records + header

```commandline
head -n 10001 occurrence.txt > gbif_2023-01-26_10k.csv
```
# AWS prep

## Local machine for testing

* Create a virtual environment, activate
* Pip install requirements*.txt files
* Install boto3 and botocore for local access
* Ensure AWS key is in ~/.ssh/ directory
* Create  ~/.aws/config file

```commandline
[default]
region = <region>
output = json
glue_role_arn = <service_role>
```

Create ~/.aws/credentials file

```commandline
[default]
aws_access_key_id = <access_key_id>
aws_secret_access_key = <secret_access_key>
```

### AWS Errors

#### InvalidSignatureException

In AWS consoles, EC2, Glue, ... :

  "... InvalidSignatureException (status: 403): Signature not yet current:... "
* Time is off on local computer.
* KU blocks NTP packages, use time.ku.edu

```commandline
$ sudo ntpdate ntp.ubuntu.com
 7 Feb 13:17:37 ntpdate[1351628]: no server suitable for synchronization found
$ sudo ntpdate time.ku.edu
 7 Feb 13:14:06 ntpdate[1351650]: step time server 129.237.12.233 offset -308.215932 sec

```
