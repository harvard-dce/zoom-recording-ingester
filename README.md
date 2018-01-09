[![Build Status](https://travis-ci.org/harvard-dce/zoom-recording-ingester.svg?branch=master)](https://travis-ci.org/harvard-dce/zoom-recording-ingester)

# Zoom Video Ingester

A set of AWS services for downloading and ingesting Zoom meeting videos into Opencast

## Setup

Python 3 is required.

1. `pip install requirements/dev.txt`
1. copy `example.env` to `.env` and update as necessary

## Commands

This project uses the `invoke` python library to provide a simple task cli. Run `invoke -l`
to see a list of available commands.

The current list of commands includes:

##### `invoke create-code-bucket`

Must be run once per setup. Will create an s3 bucket to which the packaged
lambda code will be uploaded. Does nothing if the bucket already exists. The
name of the bucket comes from `LAMBDA_CODE_BUCKET` in `.env`

##### `invoke create`

Build the Cloudformation stack. Default stack name is "zoom-ingester". Set
`STACK_NAME` in `.env` for something different.

##### `invoke update`

Apply template changes to the stack.

##### `invoke delete`

Delete the stack.
