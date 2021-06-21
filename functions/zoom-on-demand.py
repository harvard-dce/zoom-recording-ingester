import json
import requests
from os import getenv as env
from urllib.parse import urlparse, parse_qs, quote
from utils import (
    zoom_api_request,
    setup_logging,
    PipelineStatus,
    set_pipeline_status,
)
from uuid import uuid4

import logging

logger = logging.getLogger()

# This is the API Gateway endpoint url of the webhook
# We use this + the requests library as a way to trigger the webhook
# vs invoking the function directly via boto3 because the manner in which
# the API Gateway -> Lambda proxy integration is done is not replicable
# using boto3's lambda invoke() method.
WEBHOOK_ENDPOINT_URL = env("WEBHOOK_ENDPOINT_URL")


def resp(status_code, msg=""):
    logger.info(f"returning {status_code} response: '{msg}'")
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(
            {
                "message": msg,
            }
        ),
    }


@setup_logging
def handler(event, context):
    """
    This function acts as a relay to the traditional zoom webhook. The webhook
    function is called by Zoom on a "recording.completed" event, along with
    data about the recordings. Here we fetch the recording data from the zoom
    API.
    The response to that API call is (mostly) identical to the payload zoom
    sends to the webhook, so we can simply pass it along in our own webhook
    request, using a "on.demand.ingest" event type and including the series id
    as "on_demand_series_id".
    """

    logger.info(event)

    if "body" not in event or event["body"] is None:
        return resp(400, "Bad data. No body found in event.")

    try:
        body = json.loads(event["body"])
        logger.info({"on-demand request": body})
    except json.JSONDecodeError:
        return resp(400, "Webhook notification body is not valid json.")

    if "uuid" not in body:
        return resp(
            400,
            "Missing recording uuid field in webhook notification body.",
        )

    recording_uuid = body["uuid"]
    if recording_uuid.startswith("https"):
        # it's a link; parse the uuid from the query string
        parsed_url = urlparse(recording_uuid)
        query_params = parse_qs(parsed_url.query)
        if "meeting_id" not in query_params:
            return resp(
                404,
                "Zoom URL is malformed or missing 'meeting_id' param.",
            )
        recording_uuid = query_params["meeting_id"][0]

    logger.info(f"Got recording uuid: '{recording_uuid}'")

    try:
        try:
            # zoom api can break if uuid is not double urlencoded
            double_urlencoded_uuid = quote(
                quote(recording_uuid, safe=""),
                safe="",
            )
            zoom_endpoint = (
                f"meetings/{double_urlencoded_uuid}/recordings"
                "?include_fields=download_access_token&ttl=3600"
            )
            r = zoom_api_request(zoom_endpoint)
            recording_data = r.json()
        except requests.HTTPError as e:
            # return a 404 if there's no such meeting
            if e.response.status_code == 404:
                return resp(
                    404, f"No zoom recording with id '{recording_uuid}'"
                )
            else:
                raise
    # otherwise return a 500 on any other errors (bad json, bad request, etc)
    except Exception as e:
        return resp(
            500,
            f"Something went wrong querying the zoom api: {str(e)}",
        )

    if (
        "recording_files" not in recording_data
        or not recording_data["recording_files"]
    ):
        return resp(
            503,
            "Zoom api response contained no recording files for "
            f"{recording_uuid}",
        )

    # verify that all the recording files are actually "completed"
    not_completed = sum(
        1
        for x in recording_data["recording_files"]
        if x.get("status") and x.get("status") != "completed"
    )

    if not_completed > 0:
        return resp(
            503,
            "Not all recorded files have status 'completed'",
        )

    webhook_data = {
        "event": "on.demand.ingest",
        "payload": {"object": recording_data},
        "download_token": recording_data["download_access_token"],
    }

    # series id is an optional param. if not present the download function will
    # attempt to determine the series id by matching the recording times
    # against it's known schedule as usual.
    if "oc_series_id" in body and body["oc_series_id"]:
        webhook_data["payload"]["on_demand_series_id"] = body["oc_series_id"]

    if "allow_multiple_ingests" in body:
        webhook_data["payload"]["allow_multiple_ingests"] = body[
            "allow_multiple_ingests"
        ]

    zip_id = f"on-demand-{str(uuid4())}"
    webhook_data["payload"]["zip_id"] = zip_id

    logger.info({"webhook_data": webhook_data})
    try:
        r = requests.post(
            WEBHOOK_ENDPOINT_URL,
            data=json.dumps(webhook_data),
            headers={
                "Content-type": "application/json",
            },
        )
        r.raise_for_status()
        if r.status_code == 204:
            raise Exception("Webhook returned 204: ingest not accepted")
    except Exception as e:
        err_msg = str(e)
        logger.exception(
            f"Something went wrong calling the webhook: {err_msg}"
        )
        return resp(500, err_msg)

    set_pipeline_status(
        zip_id,
        PipelineStatus.ON_DEMAND_RECEIVED,
        meeting_id=webhook_data["payload"]["object"]["id"],
        recording_id=recording_uuid,
        recording_start_time=webhook_data["payload"]["object"]["start_time"],
        topic=webhook_data["payload"]["object"]["topic"],
        origin="on_demand",
        oc_series_id=body.get("oc_series_id"),
    )
    return resp(200, "Ingest accepted")
