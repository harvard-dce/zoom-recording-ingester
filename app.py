#!/usr/bin/env python3

from aws_cdk import core
from os.path import join, dirname
from dotenv import load_dotenv
from os import getenv as env

from zoom_recording_ingester_cdk.zoom_recording_ingester_cdk_stack \
    import ZoomRecordingIngesterCdkStack

load_dotenv(join(dirname(__file__), '.env'))

CDK_DEFAULT_ACCOUNT = env("CDK_DEFAULT_ACCOUNT")
CDK_DEFAULT_REGION = env("CDK_DEFAULT_REGION")

STACK_NAME = env("STACK_NAME")
if not STACK_NAME:
    raise Exception("Missing environment variable STACK_NAME")

app = core.App()
ZoomRecordingIngesterCdkStack(
    app,
    STACK_NAME,
    env=core.Environment(
        account=CDK_DEFAULT_ACCOUNT,
        region=CDK_DEFAULT_REGION,
    )
)

app.synth()
