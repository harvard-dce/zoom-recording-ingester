#!/usr/bin/env python3

from aws_cdk import App, Environment
from pathlib import Path
from .helpers import (
    getenv,
    vpc_components,
    oc_base_url,
    oc_db_url,
    aws_account_id,
    stack_tags,
)
from .stack import ZipStack

from dotenv import load_dotenv

dotenv_path = Path("..") / ".env"
load_dotenv(dotenv_path, override=True)


STACK_NAME = getenv("STACK_NAME")
AWS_PROFILE = getenv("AWS_PROFILE", required=False)
AWS_REGION = (
    getenv("AWS_REGION", required=False)
    or getenv("AWS_DEFAULT_REGION", required=False)
    or "us-east-1"
)

oc_vpc_id, oc_security_group_id = vpc_components()
ingest_allowed_ips = getenv("INGEST_ALLOWED_IPS").split(",")

default_publisher = getenv("DEFAULT_PUBLISHER", required=False)
if not default_publisher:
    default_publisher = getenv("NOTIFICATION_EMAIL")

APIGEE_KEY = getenv("APIGEE_KEY", required=False)
ZOOM_API_KEY = getenv("ZOOM_API_KEY", required=False)
ZOOM_API_SECRET = getenv("ZOOM_API_SECRET", required=False)
if not APIGEE_KEY and not (ZOOM_API_KEY and ZOOM_API_SECRET):
    raise Exception(
        (
            "Missing api credentials. "
            "Must have APIGEE_KEY or ZOOM_API_KEY and ZOOM_API_SECRET"
        )
    )

stack_props = {
    "lambda_code_bucket": getenv("LAMBDA_CODE_BUCKET"),
    "notification_email": getenv("NOTIFICATION_EMAIL"),
    "zoom_api_base_url": getenv("ZOOM_API_BASE_URL"),
    "zoom_api_key": ZOOM_API_KEY,
    "zoom_api_secret": ZOOM_API_SECRET,
    "apigee_key": APIGEE_KEY,
    "buffer_minutes": getenv("BUFFER_MINUTES"),
    "local_time_zone": getenv("LOCAL_TIME_ZONE"),
    "default_series_id": getenv("DEFAULT_SERIES_ID", required=False),
    "download_message_per_invocation": getenv(
        "DOWNLOAD_MESSAGES_PER_INVOCATION"
    ),
    "opencast_api_user": getenv("OPENCAST_API_USER"),
    "opencast_api_password": getenv("OPENCAST_API_PASSWORD"),
    "default_publisher": default_publisher,
    "override_publisher": getenv("OVERRIDE_PUBLISHER", required=False),
    "override_contributor": getenv("OVERRIDE_CONTRIBUTOR", required=False),
    "oc_cluster_name": getenv("OC_CLUSTER_NAME"),
    "default_oc_workflow": getenv("DEFAULT_OC_WORKFLOW"),
    "oc_track_upload_max": getenv("OC_TRACK_UPLOAD_MAX"),
    "downloader_event_rate": 2,
    "uploader_event_rate": 2,
    "ingest_allowed_ips": ingest_allowed_ips,
    "oc_vpc_id": oc_vpc_id,
    "oc_security_group_id": oc_security_group_id,
    "oc_base_url": oc_base_url(),
    "oc_db_url": oc_db_url(),
    "project_git_url": (
        "https://github.com/harvard-dce/zoom-recording-ingester.git"
    ),
    "gsheets_doc_id": getenv("GSHEETS_DOC_ID"),
    "gsheets_sheet_names": getenv("GSHEETS_SHEET_NAMES"),
    "slack_signing_secret": getenv("SLACK_SIGNING_SECRET", required=False),
    "slack_zip_channel": getenv("SLACK_ZIP_CHANNEL", required=False),
    "slack_api_token": getenv("SLACK_API_TOKEN", required=False),
    "slack_allowed_groups": getenv("SLACK_ALLOWED_GROUPS", required=False),
}

app = App()

# warn if we weren't exec'd via the invoke tasks
if app.node.try_get_context("VIA_INVOKE") != "true":
    print(
        "\033[93m"
        + "WARNING: executing `cdk` commands directly is not recommended"
        + "\033[0m"
    )

stack = ZipStack(
    app,
    STACK_NAME,
    **stack_props,
    env=Environment(account=aws_account_id(), region=AWS_REGION),
    tags=stack_tags()
)

app.synth()
