[![Build Status](https://travis-ci.org/harvard-dce/zoom-recording-ingester.svg?branch=master)](https://travis-ci.org/harvard-dce/zoom-recording-ingester)

# Zoom Video Ingester

A set of AWS services for downloading and ingesting Zoom meeting videos into Opencast

## Things you will need

* python 3
* the python `virtualenv` package
* an Opsworks Opencast cluster, including:
    * the base url of the admin node
    * the user/pass combo of the Opencast API system account user
    * the ID of the Opsworks cluster's VPC, e.g. "vpc-123abcd"
* A Zoom account API key and secret
* A Zoom account login user/pass combo
* An email address to recieve alerts and other notifications

    
## Initial Setup

Python 3 is required.

1. Make a python virtualenv and activate it: `virtualenv venv && source venv/bin/activate`
1. Install the dependencies: `pip install -r requirements.txt`
1. Copy `example.env` to `.env` and update as necessary. See inline comments for an explanation of each setting
1. Run `invoke test` to confirm the installation
1. (Optional) run `invoke -l` to see a list of all available tasks + descriptions
1. Run `invoke create-code-bucket` to ensure the s3 bucket for packaged lambda code exists
1. Run `invoke stack.create` to build the CloudFormation stack
1. Populate the Zoom meeting schedule database. See the *Schedule DB* section below for more details.
    1. Export the DCE Zoom schedule google spreadsheet to a CSV file
    1. Run `invoke schedule.import -c [csv file] -s [semester] -y [year]`

That's it. Your Zoom Ingester is deployed and operational. To see a summary of the 
state of the CloudFormation stack and the Lambda functions run `invoke stack.status`.

## Lambda Versions, Release Alias & Initial Code Release

Lambda functions employ the concepts of "versions" and "aliases". Each time you push new
code to a Lambda function it updates a special version signifier, `$LATEST`. If you wish
to assign an actual version number to what is referenced by `$LATEST` you "publish" a 
new version. Versions are immutable and version numbers always increment by 1. 

Aliases allow us to control which versions of the Lambda functions are invoked by the system.
The Zoom Ingester uses a single alias defined by the `.env` variable, `LAMBDA_RELEASE_ALIAS` (default "live"), 
to designate the currently released versions of each function. A new version of each
function can be published independent of the alias as many times as you like. It is only
when the release alias is updated to point to one of the new versions that the behavior
of the ingester will change.

When you first build the stack using the above "Initial Setup" steps, the version of the Lambda functions
code will be whatever was current in your cloned repo. The Lambda function versions will
be set to "1" and the release aliases ("live") will be pointing to this same version.
At this point you may wish to re-release a specific tag or branch of the function code.
In a production environment this should be done via the CodeBuild project, like so:

    invoke codebuild -r release-v1.0.0
    
This command will trigger CodeBuild to package and release the function code from the github
repo identified by the "release-v1.0.0" tag. Each function will have a new Lambda version "2"
published and the release alias will be updated to point to this version.

## Zoom Webhook Setup

Once the Zoom Ingester pipeline is operational you can configure your Zoom account to
send completed recording notifications to it via the Zoom Webhook settings.

1. Get your ingester's webhook endpoint URL. You can find it in the `invoke stack.status` output
or by browsing to the release stage of your API Gateway REST api.
1. Enter the API endpoint in the settings at https://developer.zoom.us/me/.
1. Enable recording completed push notifications at https://zoom.us/account/setting?tab=recording.

## Schedule DB

Incoming Zoom recordings are ingested to an Opencast series based on two pieces of
information:

1. The Zoom meeting number. AKA the Zoom series id.
1. The time the recording was made

The Zoom Ingester pipeline includes a DynamoDB table that stores information about
when Zoom classes are held. This is because the same Zoom series id can be used by
different courses. To determine the correct Opencast series that the recording should
be ingested to we need to also know what time the meeting occurred. 

The current authority for Zoom meeting schedules is a google spreadsheet. To populate 
our DynamoDB from the spread sheet data we have to export the spreadsheet to CSV and then
import to DynamoDB using the `invoke schedule.import` task.

If a lookup to the DynamoDB schedule data does not find a mapping the uploader function will
log a message to that effect and return. During testing/development, this can be overridden
by setting the `DEFAULT_SERIES_ID` in the lambda function's environment. Just set that
to whatever test series you want to use and all unmapped meetings will be ingested to that series.

## Task descriptions

This project uses the `invoke` python library to provide a simple task cli. Run `invoke -l`
to see a list of available commands. The descriptions below are listed in the likely order
you would run them and/or their importance.

##### `invoke create-code-bucket`

Must be run once per setup. Will create an s3 bucket to which the packaged
lambda code will be uploaded. Does nothing if the bucket already exists. The
name of the bucket comes from `LAMBDA_CODE_BUCKET` in `.env`. Packaged function
zip files are **not** namespaced, so beware using the same code bucket for multiple
ingester stacks.

##### `invoke stack.create`

Does the following:

1. Packages each function and uploads the zip files to your s3 code bucket
1. Builds the CloudFormation stack defined in `template.yml.
1. Releases an intitial version "1" of each Lambda function

Use `stack.update` to modify an existing stack.

##### `invoke stack.status`

This will output some tables of information about the current state of the 
CloudFormation stack and the Lambda functions.

##### `invoke codebuild --revision [tag or branch]`

Execute the CodeBuild project. This is the command that should be used to deploy
and release new versions of the pipeline functions in a production environment.

`--revision` is a required argument.

The build steps that CodeBuild will perform are defined in `buildspec.yml`.

##### `invoke stack.update`

Apply `template.yml` changes to the stack.

##### `invoke stack.delete`

Delete the stack.

##### `invoke debug.{on,off}`

Enable/disable debug logging in the Lambda functions. This task adds or modifies
a `DEBUG` evnironment variable in the Lambda function(s) settings.

## Development

1. Create a dev/test stack by setting your `.env` `STACK_NAME` and `LAMBDA_CODE_BUCKET`
   to unique values, e.g. "myname-zoom-ingester"
1. Follow the usual stack creation steps outlined at the top
1. Make changes
1. Run `invoke deploy --do-release` to push changes to your Lambda functions
1. Run `invoke exec.webhook [options]` to initiate the pipeline. See below for options.
1. Repeat

##### `invoke exec.webhook`

This task will manually invoke the webhook endpoint with a payload constructed from
the arguments you provide. The arguments are:

* uuid - the uuid of the meeting instance
* host_id - the Zoom host identifier

These are positional arguments, so an example command looks something like:

`invoke exec.webhook V27UZBYGRRGPVbZZTDUPyA== Lpf1XegVTWu6CVGWtSfc-Q`

There is also a `--status=[recording status]` option with a default value of
"RECORDING_MEETING_COMPLETED", but this is only useful in cases where you would be
testing correct behavior by the webhook function.

## Testing

The lambda python functions each have associated unittests. To run them manually
execute:

`invoke test`

Alternatively you can run `tox`. 
