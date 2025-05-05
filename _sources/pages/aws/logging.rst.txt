Logging to AWS Cloudwatch
#####################################

Workflow steps executed in Lambda Functions
=====================================

Lambda functions log directly to AWS Cloudwatch - make sure the role executing
the lambda function has Create/Write permissions to AWS Cloudwatch.  All print
statements will go to the log.

Logs will be sent to a Log Group, prefixed by '/aws/lambda/' followed by the name of
the lambda function, such as **/aws/lambda/bison_s4_intersect_bison**.  A new stream
under that log group will be created for every run of the function.

Workflow steps executed in EC2 instance
========================================

EC2 instances which execute steps will redirect Docker logs to an AWS Cloudwatch Log
Group created for this purpose, **bison_task**.  A stream will be created for each task,
with logging statements for all runs in the same stream.

This logging is configured in the Docker compose file (example below) for each task.
Use the task name defined in the EC2 Launch Template version for the task.

AWS permissions
=====================================

Add CloudWatchFullAccess to the role used to launch the EC2 instance and execute the
task.

AWS Cloudwatch
=================

* Create a log group (ex: bison_task)
* Under the log group, create a log stream (ex: annotate_riis)


In Docker compose File
=====================================

compose.annotate_riis.yml::

    logging:
      driver: awslogs
      options:
        awslogs-region: us-east-1

        awslogs-group: bison_task_calc_stats
        awslogs-stream: calc_stats

TODO: Create a new log group for each lambda- initiated EC2 workflow task, with a new
stream (named with timestamp) for each execution::

        awslogs-group: bison_task
        awslogs-stream: annotate_riis


