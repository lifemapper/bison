Create lambda function to initiate processing
------------------------------------------------

* Check for existence of new GBIF data
* Use a blueprint, python, "Get S3 Object"
* Function name: bison_find_current_gbif_lambda
* S3 trigger:

    * Bucket: arn:aws:s3:::gbif-open-data-us-east-1
