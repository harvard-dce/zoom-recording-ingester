[![Build Status](https://travis-ci.org/harvard-dce/zoom-recording-ingester.svg?branch=master)](https://travis-ci.org/harvard-dce/zoom-recording-ingester)

# Zoom Video Ingester

A set of AWS services for downloading and ingesting Zoom meeting videos into Opencast

## Overview

The Zoom Ingester (a.k.a., "Zoom Ingester Pipeline", a.k.a., "ZIP") is Harvard DCE's mechanism for moving Zoom
recordings out of the Zoom service and into our own video management and delivery system, Opencast. It allows DCE
to deliver recorded Zoom class meetings and lectures alongside our other, non-Zoom video content. 

When deployed, the pipeline will have an API endpoint that must be registered in your Zoom account as a receiver of
completed recording events. When Zoom has completed the processing of a recorded meeting video it will send a "webhook"
notification to the pipeline's endpoint. From there the recording metadata will be passed along through a series of queues 
and Lambda functions before finally being ingested into Opencast. Along the way, the actual recording files will be
fetched from Zoom and stored in S3. Alternatively, from the Opencast admin interface, a user can kick off an "on-demand"
ingestion by entering the identifier of a Zoom recording and the corresponding Opencast series into which it should be 
ingested. The On-Demand ingest function then fetches the recording metadata from the Zoom API and emulates a standard
webhook.

Info on Zoom's API and webhook functionality can be found at:
  * [developer portal](https://marketplace.zoom.us/docs/guides)
  * [webhooks](https://marketplace.zoom.us/docs/api-reference/webhook-reference)


## Pipeline flow diagram

![zoom ingester pipeline diagram](docs/Zoom%20Ingester%20Pipeline.png)


* [Setup](#setup)

	* [CloudFormation stack](#create%20a%20cloudformation%20stack) 

	* [Zoom webhook notifications](Setup%20Zoom%20webhook%20notifications(Optional))

* [Development](#development)

	* [Schedule DB](#schedule%20db)

	* [Invoke task descriptions](#invoke%20task%20descriptions)
	
	* [Dependency Changes](#dependency%20changes)

	* [Lambda Versions, Release Alias & Initial Code Release](#lambda%20versions,%20release%20alias%20&%20initial%20code%20release)

* [Testing](#testing)

* [Release Process](#release-process)

<!-- toc -->

## Setup

### Things you will need

* python 3.8
* the python `virtualenv` package
* an Opsworks Opencast cluster, including:
    * the base url of the admin node
    * the user/pass combo of the Opencast API system account user
    * OC database password
* A Zoom account API key and secret
* Email of Zoom account with privileges to download recordings
* An email address to receive alerts and other notifications

### Create a CloudFormation stack

#### local environment setup

1. Make a python virtualenv and activate it however you normally do those things, e.g.: `virtualenv venv && source venv/bin/activate`
1. Python dependencies are handled via `pip-tools` so you need to install that first: `pip install pip-tools`
1. Install the dependencies by running `pip-sync`
1. Copy `example.env` to `.env` and update as necessary. See inline comments for an explanation of each setting
1. Run `invoke test` to confirm the installation
1. (Optional) run `invoke -l` to see a list of all available tasks + descriptions

#### deployment

1. Make sure your s3 bucket for packaged lambda code exists. The
name of the bucket comes from `LAMBDA_CODE_BUCKET` in `.env`. 
1. Run `invoke stack.create` to build the CloudFormation stack
1. Run `invoke generate-resource-policy` and paste the output into the API Gateway's "Resource Policy" field in the web console.
1. Enable CORS on the ingest endpoint:
    1. Navigate to the `/ingest` POST method in the api gateway UI
    1. In the "Actions" menu choose "Enable CORS"
    1. In the "Access-Control-Allow-Headers" field enter the following value, including the "'" marks:
       `'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept-Language,X-Requested-With'`
    1. Click "Enable" and confirm
    1. The follow-up screen will show the changes being applied. The last one will show a red X instead of a green checkmark.
       Apparently this is fine for our purposes and doesn't cause problems, so safe to ignore.
1. (Optional) Populate the Zoom meeting schedule database. See the *Schedule DB* section below for more details.
    1. Export the DCE Zoom schedule google spreadsheet to a CSV file
    1. Run `invoke schedule.import-csv [filepath]`

That's it. Your Zoom Ingester is deployed and operational. To see a summary of the
state of the CloudFormation stack and the Lambda functions run `invoke stack.status`.

### Set up Zoom webhook notifications (Optional)


Once the Zoom Ingester pipeline is operational you can configure your Zoom account to
send completed recording notifications to it via the Zoom Webhook settings.

1. Get your ingester's webhook endpoint URL. You can find it in the `invoke stack.status` output
or by browsing to the release stage of your API Gateway REST api.
1. Go to [marketplace.zoom.us](marketplace.zoom.us) and log in. Under "Develop" select "Build App."
1. Give your app a name. Turn off "Intend to publish this app on Zoom Marketplace."
Choose app type "Webhook only app."
1. Click "Create." Fill out the rest of the required information,
and enter the API endpoint under "Event Subscription."
1. Make sure to subscribe to "All recordings have completed" events.
1. Activate the app when desired. (For development it's recommended that you only leave the notifications active while you're actively testing.)

## Development

1. Create a dev/test stack by setting your `.env` `STACK_NAME` to a unique value.
1. Follow the usual stack creation steps outlined at the top.
1. Make changes.
1. Run `invoke deploy.all --do-release` to push changes to your Lambda functions.
Alternatively, to save time, if you are only editing one function, run `invoke deploy.[function name] --do-release`.
1. Run `invoke exec.webhook [options]` to initiate the pipeline. See below for options.
1. Repeat.

##### `invoke exec.webhook [uuid]`

Options: `--oc-series-id=XX`

This task will recreate the webhook notification for the recording identified by
`uuid` and manually invoke the `/new_recording` api endpoint.

##### `invoke exec.pipeline [uuid]`

Options: `--oc-series-id=XX`

Similar to `exec.webhook` except that this also triggers the downloader and
uploader functions to run and reports success or error for reach.

##### `invoke exec.on_demand [uuid]`

Options: `--oc-series-id=XXX --allow-multiple-ingests`

This task will manually invoke the `/ingest` endpoint. This is the endpoint used
by the Opencast "Zoom+" tool. Specify an opencast series id with `--oc-series-id=XX`.
Allow multiple ingests of the same recordiing (for testing purposes) with `--allow-multiple-ingests`.


### Schedule DB

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
import to DynamoDB using the `invoke schedule.import-csv [filepath]` task.

If a lookup to the DynamoDB schedule data does not find a mapping the uploader function will
log a message to that effect and return. During testing/development, this can be overridden
by setting the `DEFAULT_SERIES_ID` in the lambda function's environment. Just set that
to whatever test series you want to use and all unmapped meetings will be ingested to that series.

### Invoke task descriptions

This project uses the `invoke` python library to provide a simple task cli. Run `invoke -l`
to see a list of available commands. The descriptions below are listed in the likely order
you would run them and/or their importance.

##### `invoke stack.create`

Does the following:

1. Packages each function and uploads the zip files to your s3 code bucket
1. Builds the CloudFormation stack defined in `template.yml.
1. Releases an initial version "1" of each Lambda function

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
a `DEBUG` environment variable in the Lambda function(s) settings.

##### `invoke update-requirements`

Does a bulk `pip-compile` upgrade of all base and function requirements.


### Dependency Changes

Dependencies for the project as a whole and the individual functions are managed using
the `pip-tools` command, `pip-compile`. Top-level dependencies are listed in a `.in` file
which is then compiled to a "locked" `.txt` version like so:

`pip-compile -o requirements.txt requirements.in`

Both the `.in` and `.txt` files are version-controlled, so the initial compile was
only necessary once. Now we only have to run `pip-compile` in a couple of situations:

* when upgrading a particular package.
* to update the project's base requirements list if a dependency for a specific function is changed

In the first case you run `pip-compile -P [package-name] [source file]` where `source_file` is the `.in` file getting the update.

Following that you must run `pip-compile` in the project root to pull the function-specific change(s) into the main project list.

Finally, run `pip-sync` to ensure the packages are updated in your virtualenv .

## Testing

The lambda python functions each have associated unittests. To run them manually
execute:

`invoke test`

Alternatively you can run `tox`.

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

## Release Process

### Step 1: Update master

Merge all changes for release into master.  
Checkout master branch, git pull.

### Step 2: Test release in dev stack

First check that the codebuild runs with the new changes on a dev stack:

If there are new functions you must package and ensure the code is in s3:

    invoke package -u

then

	invoke stack.update
	invoke codebuild --revision=master

Make sure codebuild completes successfully.

### Step 3: Tag management

First update your tags:

    git tag -l | xargs git tag -d
    git fetch --tags    


Then tag release:

    git tag release-vX.X.X
    git push --tags

### Step 4: Release to production stack

Make sure you are working on the correct zoom ingester stack, double check environment variables. Then:

If there are new functions you must package and ensure the code is in s3:

    invoke package -u
    
then

	invoke stack.update
	invoke codebuild --revision=release-vX.X.X
