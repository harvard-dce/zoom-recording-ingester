#!/usr/bin/env python3

import boto3
from aws_cdk import core
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path('..') / '.env')

from .helpers import (
    getenv,
    vpc_components,
    oc_base_url,
    zoom_admin_id,
    aws_account_id
)
from .stack import ZipStack

STACK_NAME = getenv("STACK_NAME")
AWS_REGION = getenv("AWS_REGION", required=False) or \
             getenv("AWS_DEFAULT_REGION", required=False) or \
             "us-east-1"

oc_vpc_id, oc_security_group_id =  vpc_components()

stack_props = {
    "lambda_release_alias": getenv("LAMBDA_RELEASE_ALIAS"),
    "lambda_code_bucket": getenv("LAMBDA_CODE_BUCKET"),
    "notification_email": getenv("NOTIFICATION_EMAIL"),
    "zoom_api_key": getenv("ZOOM_API_KEY"),
    "zoom_api_secret": getenv("ZOOM_API_SECRET"),
    "local_time_zone": getenv("LOCAL_TIME_ZONE"),
    "default_series_id": getenv("DEFAULT_SERIES_ID", required=False),
    "download_message_per_invocation": getenv("DOWNLOAD_MESSAGES_PER_INVOCATION"),
    "opencast_api_user": getenv("OPENCAST_API_USER"),
    "opencast_api_password": getenv("OPENCAST_API_PASSWORD"),
    "default_publisher": getenv("DEFAULT_PUBLISHER"),
    "override_publisher": getenv("OVERRIDE_PUBLISHER", required=False),
    "override_contributor": getenv("OVERRIDE_CONTRIBUTOR", required=False),
    "oc_workflow": getenv("OC_WORKFLOW"),
    "oc_flavor": getenv("OC_FLAVOR"),
    "oc_track_upload_max": getenv("OC_TRACK_UPLOAD_MAX"),
    "downloader_event_rate": 2,
    "uploader_event_rate": 2,
    "oc_vpc_id": oc_vpc_id,
    "oc_security_group_id": oc_security_group_id,
    "oc_base_url": oc_base_url(),
    "zoom_admin_id": zoom_admin_id(),
    "project_git_url": "https://github.com/harvard-dce/zoom-recording-ingester.git",
}

app = core.App()
stack = ZipStack(
    app,
    STACK_NAME,
    **stack_props,
    env=core.Environment(
        account=aws_account_id(),
        region=AWS_REGION
    )
)

app.synth()



