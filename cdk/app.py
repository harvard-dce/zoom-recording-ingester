#!/usr/bin/env python3

from aws_cdk import core
from os.path import join, dirname

from dotenv import load_dotenv
load_dotenv(join(dirname(__file__), '.env'))

from .helpers import getenv, vpc_components, oc_base_url, zoom_admin_id
from .stack import ZipStack

STACK_NAME = getenv("STACK_NAME")

oc_vpc_id, oc_security_group_id =  vpc_components()

stack_props = {
    "lambda_code_bucket": getenv("LAMBDA_CODE_BUCKET"),
    "notification_email": getenv("NOTIFICATION_EMAIL"),
    "zoom_api_key": getenv("ZOOM_API_KEY"),
    "zoom_api_secret": getenv("ZOOM_API_KEY"),
    "local_time_zone": getenv("ZOOM_API_KEY"),
    "default_series_id": getenv("ZOOM_API_KEY", required=False),
    "download_message_per_invocation": getenv("ZOOM_API_KEY"),
    "opencast_api_user": getenv("ZOOM_API_KEY"),
    "opencast_api_password": getenv("ZOOM_API_KEY"),
    "default_publisher": getenv("ZOOM_API_KEY"),
    "override_publisher": getenv("ZOOM_API_KEY"),
    "override_contributor": getenv("ZOOM_API_KEY"),
    "oc_workflow": getenv("ZOOM_API_KEY"),
    "oc_flavor": getenv("ZOOM_API_KEY"),
    "oc_track_upload_max": getenv("ZOOM_API_KEY"),
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
    **stack_props
)

app.synth()



