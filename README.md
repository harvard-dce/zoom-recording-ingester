[![Build Status](https://github.com/harvard-dce/zoom-recording-ingester/actions/workflows/checks.yml/badge.svg)](https://github.com/harvard-dce/zoom-recording-ingester/actions/workflows/checks.yml)

# Zoom Video Ingester

A set of AWS services for downloading and ingesting Zoom meeting videos into Opencast

## Overview

The Zoom Ingester (a.k.a., "Zoom Ingester Pipeline", a.k.a., "ZIP") is Harvard DCE's mechanism for moving Zoom
recordings out of the Zoom service and into our own video management and delivery system, Opencast. It allows DCE
to deliver recorded Zoom class meetings and lectures alongside our other, non-Zoom video content.

When deployed, the pipeline will have an API endpoint that must be registered in your Zoom account as a receiver of completed recording events. When Zoom has completed the processing of a recorded meeting video it will send a "webhook" notification to the pipeline's endpoint. From there the recording metadata will be passed along through a series of queues and Lambda functions before finally being ingested into Opencast. Along the way, the actual recording files will be fetched from Zoom and stored in S3.

Alternatively, from the Opencast admin interface, a user can kick off an "on-demand" ingestion by entering the identifier of a Zoom recording and the corresponding Opencast series into which it should be ingested. The On-Demand ingest function then fetches the recording metadata from the Zoom API and emulates a standard webhook.

Info on Zoom's API and webhook functionality can be found at:

* [developer portal](https://marketplace.zoom.us/docs/guides)
* [webhooks](https://marketplace.zoom.us/docs/api-reference/webhook-reference)

## Pipeline flow diagram

![zoom ingester pipeline diagram](docs/zoom-ingester-pipeline.png)

* [Setup](#setup)

* [Development](#development)

* [Endpoints](#endpoints)

* [Testing](#testing)

* [Release Process](#release-process)

<!-- toc -->

## Setup

### Things you will need

##### Python stuff

* you should use python 3.12
* the python `virtualenv` package
* AWS CLI installed and configured <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html>

##### node/cdk stuff

* nodejs v16+ along with npm
* you need docker installed for CDK to build the function container image

Note, as of v4 you no longer need the aws-cdk toolkit installed as a pre-requisite. We will install that as part of the setup detailed below.

##### other stuff

* an Opsworks Opencast cluster, including:
  * the base url of the admin node
  * the user/pass combo of the Opencast API system account user
* A Zoom account API key and secret
* Email of Zoom account with privileges to download recordings
* An email address to receive alerts and other notifications

### Create a CloudFormation stack

#### local environment setup

1. Make sure to have run `aws configure` at some point so that you
   at least have one set of credentials in an `~/.aws/configure` file.
2. Make a python virtualenv and activate it however you normally do those things, e.g.: `virtualenv venv && source venv/bin/activate`
3. Python dependencies are handled via `pip-tools` so you need to install that first: `pip install pip-tools`
4. Install the dependencies by running `pip-sync requirements/dev.txt`.
5. Install the nodejs aws-cdk CLI locally by running `npm install` in the project root
6. Copy `example.env` to `.env` and update as necessary. See inline comments for an explanation of each setting.
7. If you have more than one set of AWS credentials configured you can set `AWS_PROFILE` in your `.env` file. Otherwise
   you'll need to remember to set in your shell session prior to any `invoke` commands.
8. Run `invoke test` to confirm the installation.
9. (Optional) run `invoke -l` to see a list of all available tasks + descriptions.
10. (Optional) If you plan to make contributions. Install the pre-commit checks with `pip install pre-commit && pre-commit install`.

#### deployment

1. Run `invoke stack.deploy` to build the CloudFormation stack.
2. (Optional for dev) Populate the Zoom meeting schedule database. See the *Schedule DB* section below for more details.
   1. Export the DCE Zoom schedule google spreadsheet to a CSV file.
   2. Run `invoke schedule.import-csv [filepath]`.

That's it. Your Zoom Ingester is deployed and operational. To see a summary of the
state of the CloudFormation stack and the Lambda functions run `invoke stack.status`.

#### A note about `.env` management

If you are working on a zip deployment that is potentially shared with other developers you should ensure that the values in the `.env` file are stored and kept up-to-date in a parameter store entry. DCE's Parameter store entry names use the format `/zip-configs/<zip-name>` and should be created as SecureString values.

### Google Sheets integration setup

#### Google Sheets API Auth

1. Fill in Google Sheets environment variables `GSHEETS_DOC_ID` and `GSHEETS_SHEET_NAMES`

Since these credentials are shared within an AWS account, the following setup
only needs to be done once per AWS account:

1. Set up a Google API service account and download the `service_account.json` credentials file.
2. Store the credentials file in SSM using `invoke schedule.save-creds [-f credentials-filename]`

#### Finding the ZIP stack API endpoint

The ZIP API endpoint is available in the output of `invoke stack.deploy` and `invoke stack.status` under `Outputs`.

#### Setting up the schedule update trigger from Google Sheets

1. Share the Google Sheet with your service account
2. From the Google Sheet, under Tools > Script Editor, create a script.
   The following script is an example which creates a menu item in the
   Google Sheet that triggers the schedule update function.

```
function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('ZIP')
      .addItem('Update ZIP Schedule', 'updateZoomIngester')
      .addToUi();
}

function updateZoomIngester() {
  var url = "[your stack endpoint]";
  var options = {
  'method' : 'POST',
  };
  var response = UrlFetchApp.fetch(url, options);
  Logger.log(response);
  SpreadsheetApp
    .getActiveSpreadsheet()
    .toast(response.getContentText(), "Schedule Updated", 3);
}

```

### Slack integration setup (Optional)

1. Go to <https://api.slack.com/apps>, log in if necessary, go to "Build" then "Your Apps", and click "Create New App".

2. Select "From scratch", give the app a name, and pick the Slack workspace to develop your app in. Then click "Create App" to create the app.

3. You should now be on the "Settings" > "Basic Information" page for your app. Open "Add features and functionality".

4. Click on "Interactive Components", then toggle "Interactivity" on. Paste the slack endpoint url into the "Request URL" filed. (This can be found in the CDK stack outputs and should end with `/slack`.) Save changes.

5. Go to "Slash Commands" for the app and click "Create New Command". Enter `/zip` for the Command or an alternative of your choice. Paste the slack endpoint into the "Request URL" field and enter a short description and then save the new command.

6. Go to "Permissions". Scroll down to "Scopes". Add the OAuth Scope "usergroups:read". (The "commands" scope should already be there.)

7. Install the app in the workspace.

8. Add the following environment variables to your `.env` file:

   * On the Permissions page. Copy/paste the "Bot User OAuth Token" into your .env file `SLACK_API_TOKEN`.
   * In your apps "Basic Information", under "App Credentials", show the "Signing Secret", and copy/paste this into the ZIP .env file `SLACK_SIGNING_SECRET`.
   * Set `SLACK_ZIP_CHANNEL` to the name of the Slack channel in which you would like to allow usage of the Slack integration.
   * Set `SLACK_ALLOWED_GROUPS` to a comma delimited list of Slack groups whose members will be allowed to use the integration.

9. Run `invoke stack.deploy` to release new values of the environment variables.

### Setup Zoom webhook notifications (Optional)

Once the Zoom Ingester pipeline is deployed you can configure your Zoom account to
send completed recording and other event notifications to it via the Zoom Webhook settings.

1. Get your ingester's webhook endpoint URL. You can find it in the `invoke stack.status` output
   or by browsing to the release stage of your API Gateway REST api.
2. Go to [marketplace.zoom.us](marketplace.zoom.us) and log in. Look for the "Develop" menu at the top-right, and select "Build App."
3. Choose app type "Webhook Only"
4. Give your app a name.
5. Fill in the appropriate developer contact info. Enter "Harvard University" for the company name. Click "Continue".
6. Copy the Secret Token value. This goes in your `.env` file as `WEBHOOK_VALIDATION_SECRET_TOKEN`.
7. Run `invoke stack.deploy` to deploy the new `WEBHOOK_VALIDATION_SECRET_TOKEN` value to the webhook lambda function. This is necessary for
   app endpoint validation step coming up.
8. Toggle "Event Subscriptions" to enabled, then click "+ Add Event Subscription"
9. Give the subscription a name, e.g. "recording events", and paste your webhook endpoint URL in the space provided.
10. Click the "Validate" button underneath where you pasted the webhook endpoint URL. If validation does not succeed...
    1. double-check you got the correct secret token value
    2. check the webhook lambda function in the AWS console to confirm the environment variable
11. Click "+ Add Events" and subscritbe to the following events:

    For automatic ingests:.

    * Recording - "All recordings have completed".

    For status updates:

    * Recording - "Recording Started"
    * Recording - "Recording Paused"
    * Recording - "Recording Resumed"
    * Recording - "Recording Stopped"
    * Meeting - "End Meeting"
    * Webinar - "End Webinar"
12. Activate the app when desired. **For development it's recommended that you only leave the notifications active while you're actively testing.**

## Endpoints

The easiest way to find a listing of the endpoints for your stack is to run `invoke stack.status` and look in the **Outputs**. Identify the endpoints by their `ExportName`.

### Webhook Receiving Endpoint

**Description:** Receives webhook notifications from Zoom for ingests or status updates and receives on-demand ingest requests forwarded from the on-demand endpoint.

**Endpoint:** `POST /new_recording`
**ExportName** : `<stack-name>-webhook-url`

**Accepted Zoom webhook notifications for status updates:**

* "recording.started"
* "recording.stopped"
* "recording.paused"
* "recording.resumed"
* "meeting.ended"

**Accepted Zoom webhook notifications for ingests:**

* "recording.completed"

### On-Demand Endpoint (requests from Opencast only)

**Description:** Initiate a new on demand ingests from Opencast.

**Endpoint:** `POST /ingest`
**ExportName:**`<stack-name>-ingest-url`

**Request Body Schema**

| Parameter      | Required?     | Type          | Description   |
| -------------  | ------------- |-------------  |-------------  |
| uuid           | Yes           | string        | Either the recording uuid or the link to the recording files. Recording files link example: `https://zoom.us/recording/management/detail?meeting_id=ajXp112QmuoKj4854875%3D%3D`  |
| oc\_series\_id  | No            | string        | Opencast series id to ingest this recording to. Default: Recording only ingested if it matches the existing ZIP schedule.   |
| oc\_workflow  | No            | string        | Override the default Opencast workflow. |
| allow\_multiple\_ingests  | No  | boolean       | Whether to allow the same Zoom recording to be ingested multiple times into Opencast. Default false. |
| ingest\_all\_mp4  | No  | boolean       | Whether to ingest (and archive) all mp4 files. Default false. |

**Request Body Example**

```
{
    "uuid": "ajXp112QmuoKj4854875==",
    "oc_series_id": "20210299999",
    "oc_workflow": "workflow_name",
    "allow_multiple_ingests": false,
    "ingest_all_mp4": false
}
```

### Schedule Update Endpoint

**Description:** Update the ZIP schedule.

**Endpoint:** `POST /schedule_update`
**ExportName:** `<stack-name>-schedule-url`
**Parameters:** No parameters. Retrieves schedule from stack associated google sheet.

### Status Endpoint (requests from Opencast only)

**Description:** Check the status of a recording.

**Endpoint:** `GET /status`
**ExportName:** `<stack-name>-status-url`

**Request Path Parameters**

Provide only one of the following:

`meeting_id` - A Zoom meeting id.
`seconds` - Retrieve status' updated within the last X seconds.

**Request Examples**

Retrieve all status' updated within the last 10 seconds:
`GET https://<your-stack-endpoint-url>/status?seconds=10`

Retrieve current status of all recordings with Zoom meeting id 86168921331:
`GET https://<your-stack-endpoint-url>/status?meeting_id=86168921331`

## Development

### Development Guide

1. Create a dev/test stack by setting your `.env` `STACK_NAME` to a unique value. If using ECS deployment, also set STACK\_TYPE=ecs.
1. Follow the usual stack creation steps outlined at the top.
1. Make changes.
1. Run `invoke stack.diff` to inspect the changes
1. Run `invoke stack.deploy` to apply the changes. Alternatively you can run `invoke stack.changeset` to generate a cloudformation changeset which you will then need to inspect and execute in the web console.
1. Run `invoke exec.webhook [options]` to initiate the pipeline. See below for options.
1. Repeat.

##### `invoke exec.webhook [uuid]`

Options: `--oc-series-id=XX`

This task will recreate the webhook notification for the recording identified by
`uuid` and manually invoke the `/new_recording` api endpoint.

##### `invoke exec.pipeline [uuid]`

Options: `--oc-series-id=XX`

Similar to `exec.webhook` except that this also triggers the downloader and
uploader functions to run and reports success or error for each.

##### `invoke exec.on_demand [uuid]`

Options: `--oc-series-id=XX --oc_workflow=XX --allow-multiple-ingests --ingest-all-mp4`

This task will manually invoke the `/ingest` endpoint. This is the endpoint used by the Opencast "+Zoom" tool.

`--oc-series-id=XX` - Specify an opencast series id.

`--oc_workflow=XX` - Select an Opencast workflow other than the default.

`--allow-multiple-ingests` - Allow multiple ingests of the same recordiing (for testing purposes).

`--ingest-all-mp4` - Ingest (for archival purposes) all mp4 files associated with the requested recording

### Schedule DB

Incoming Zoom recordings are ingested to an Opencast series based on two pieces of
information:

1. The Zoom meeting number. AKA the Zoom series id.
2. The time the recording was made

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

##### `invoke stack.deploy`

Execute the initial deploy or apply changes to an existing CloudFormation stack.

**Notes**:

* When running this command for a stack for the first time you will be presented with a
  confirmation prompt to approve some of provisioning operations or changes, typical those realted
  to security and/or permissions
* The output from this command can be a bit verbose. You will see real-time updates from
  Cloudformation as resource creation is initiated and completed.

##### `invoke stack.status`

This will output some tables of information about the current state of the
CloudFormation stack and the Lambda functions.

##### `invoke stack.diff`

View a diff of CloudFormation changes to the stack.

##### `invoke stack.changeset`

Like `stack.deploy` except changes are captured in a CloudFormation changeset and execution of the update is deferred and must be done manually, most likely through the CloudFormation web console. Use this instead of `stack.deploy` if you want to be more cautious with the deployment. There are times when every change an update is going to make is not represented in the diff output of `stack.diff`. A changeset allows you to inspect what's going to change in more detail. A changeset can also be discarded if it contains changes that are unwanted or incorrect for some reason.

`stack.changeset` only works with stacks that have already been created with `stack.deploy`.

##### `invoke stack.delete`

Delete the stack.

##### `invoke debug.{on,off}`

Enable/disable debug logging in the Lambda functions. This task adds or modifies
a `DEBUG` environment variable in the Lambda function(s) settings.

##### `invoke update-requirements`

Does a bulk `pip-compile` upgrade of all base and function requirements.

## Dependency Changes

Dependencies for the project as a whole and the individual functions are managed using
the `pip-tools` command, `pip-compile`. Top-level dependencies are listed in a `.in` file
which is then compiled to a "locked" `.txt` version.

There are four different contexts that require dependencies to be pip-compiled:

* the functions themsevles
* the base context, i.e., running the `invoke` tasks for packaging, deployment and stack updates
* the tox unittesting context
* the development context (when you're working on and testing the code)

When updating or adding a package you should run pip-compile on affected requirements files in this order:

1. `functions/requirements.in`
2. `requirements/base.in`
3. `requirements/tox.in`
4. `requirements/dev.in`

Running pip-compile on a `.in` file will generate a corresponding `.txt` file which "locks" the dependent package versions.
Both the `.in` and `.txt` files should be committed to version control.

The main situation in which this becomes necessary is when you need to update a particular package due to
vulnerability. For example, if the **google-auth** package needed to be updated you would run:

`pip-compile -P google-auth functions/requirements.in`

Afterwards you would need to also `pip-compile` the remaning three "downstream" requirements files (in order) since they
use the `-r` flag to import the `functions/requirements.txt` file.

Finally, you'll want to run `pip-sync requirements/dev.txt` to ensure the packages are updated in your virtualenv.

### aws-cdk-lib & constructs package updates

**Important**: when updating the versions of `aws-cdk-lib` and `constructs` you must also update the version of `aws-cdk` specified in the `package.json`.

## Testing

The lambda python functions each have associated unittests. To run them manually
execute:

`invoke test`

Alternatively you can just run `tox` directly.

## Production Release Process

### Step 1: Update master

All changes should be merged into the main branch

### Step 2: Update CHANGELOG.md

Any changes detailed in the `[unreleased]` section should be moved to a new `[vX.X.X]` section.

Commit the change with a message like `release vX.X.X prep`

### Step 3: Tag management

```
git tag release-vX.X.X
git push --tags
```

### Step 4: Update .env

Sync your local `.env` file with the appropriate zip config param.
```
aws ssm get-parameter --name /zip-configs/prod --with-decription --output text --query Parameter.Value > .env
```

### Step 5: Release to production stack

Make sure you are working on the correct zoom ingester stack, double check environment variables. Then:

Run `invoke stack.diff` to verify the changes
Run `invoke stack.deploy` to deploy the changes
