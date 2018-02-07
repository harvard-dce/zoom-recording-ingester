[![Build Status](https://travis-ci.org/harvard-dce/zoom-recording-ingester.svg?branch=master)](https://travis-ci.org/harvard-dce/zoom-recording-ingester)

# Zoom Video Ingester

A set of AWS services for downloading and ingesting Zoom meeting videos into Opencast

## Initial Setup

Python 3 is required.

1. `pip install -r requirements.txt`
1. copy `example.env` to `.env` and update as necessary
1. run `invoke create-code-bucket` to ensure the s3 bucket for packaged lambda
   code exists
1. run `invoke package` to build and upload the lambda function code to s3
1. run `invoke create` to build the CloudFormation stack

## Opsworks cluster VPC setup

To allow the `zoom-uploader` function to access the Opencast admin node the Lambda
function must be configured for VPC network access.

1. In the "Network" settings for the uploader lambda function choose the VPC of 
the target opsworks cluster
1. In the "Subnets" dropdown that appears choose both the public and private subnets
1. In the "Security Groups" dropdown choose the "Common" security group
1. Click "Save" to update the function

## Zoom Webhook Setup

Enter API endpoint at https://developer.zoom.us/me/.
Enable recording completed push notifications at https://zoom.us/account/setting?tab=recording.

## Stack-related commands

This project uses the `invoke` python library to provide a simple task cli. Run `invoke -l`
to see a list of available commands.

The current list of commands includes:

##### `invoke create-code-bucket`

Must be run once per setup. Will create an s3 bucket to which the packaged
lambda code will be uploaded. Does nothing if the bucket already exists. The
name of the bucket comes from `LAMBDA_CODE_BUCKET` in `.env`


##### `invoke package`

This packages and uploads the lambda function code to the s3 code bucket. You 
should only have to run this once prior to initial stack creation.

You can also package individual functions with:

* `invoke package-webhook`
* `invoke package-downloader`

##### `invoke create`

Build the Cloudformation stack. 

##### `invoke update`

Apply template changes to the stack.

##### `invoke delete`

Delete the stack.

##### `invoke update-webhook` & `invoke update-downloader`

Run this to release new versions of the function code.

These commands will respectively package their function, upload to s3 and
register the updated code with the AWS Lambda function resource.

## Testing

The lambda python functions each have associated unittests. To run them manually
execute:

`invoke test`

Alternatively you can run `tox`. 
