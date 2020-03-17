import json
import requests
from os import getenv as env
from urllib.parse import urlparse, parse_qs
from common import setup_logging, zoom_api_request

import logging
logger = logging.getLogger()

# This is the API Gateway endpoint url of the webhook
# We use this + the requests library as a way to trigger the webhook
# vs invoking the function directly via boto3 because the manner in which
# the API Gateway -> Lambda proxy integration is done is not replicable
# using boto3's lambda invoke() method.
WEBHOOK_ENDPOINT_URL = env("WEBHOOK_ENDPOINT_URL")


def resp(status_code, msg=""):
    logger.info("returning {} response: '{}'".format(status_code, msg))
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({
            "message": msg
        })
    }


@setup_logging
def handler(event, context):
    """
    This function acts as a relay to the traditional zoom webhook. The webhook
    function is called by Zoom on a "recording.completed" event, along with data
    about the recordings. Here we fetch the recording data from the zoom API.
    The response to that API call is (mostly) identical to the payload zoom
    sends to the webhook, so we can simply pass it along in our own webhook
    request, using a "on.demand.ingest" event type and including the series id
    as "on_demand_series_id".
    """

    logger.info(event)

    if "body" not in event:
        return resp(400, "Bad data. No body found in event.")

    try:
        body = json.loads(event["body"])
        logger.info({"on-demand request": body})
    except json.JSONDecodeError:
        return resp(400, "Webhook notification body is not valid json.")

    if "uuid" not in body:
        return resp(400, "Missing recording uuid field in webhook notification "
                        "body.")

    uuid = body["uuid"]
    if uuid.startswith("https"):
        # it's a link; parse the uuid from the query string
        parsed_url = urlparse(uuid)
        query_params = parse_qs(parsed_url.query)
        if "meeting_id" not in query_params:
            return resp(400, "Bad url: missing 'meeting_id' param.")
        uuid = query_params["meeting_id"][0]

    try:
        zoom_endpoint = "/meetings/{}/recordings".format(uuid)
        logger.info("zoom api request to {}".format(zoom_endpoint))
        r = zoom_api_request(zoom_endpoint)
        recording_data = r.json()
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return resp(404, "No zoom recording with id '{}'"
                        .format(uuid))
        else:
            return resp(500, "Something went wrong querying the zoom api: {}"
                        .format(str(e)))

    # verify that all the recording files are actually "completed"
    not_completed = sum(
        1 for x in recording_data["recording_files"]
        if x.get("status") and x.get("status") != "completed"
    )

    if not_completed > 0:
        return resp(503, "Not all recorded files have status 'completed'")

    webhook_data = {
        "event": "on.demand.ingest",
        "payload": {
            "object": recording_data
        }
    }

    # series id is an optional param. if not present the download function will
    # attempt to determine the series id by matching the recording times against
    # it's known schedule as usual.
    if "series_id" in body:
        webhook_data["payload"]["on_demand_series_id"] = body["series_id"]

    logger.info("posting on-demand request: {}".format(webhook_data))
    try:
        r = requests.post(WEBHOOK_ENDPOINT_URL,
                          data=json.dumps(webhook_data),
                          headers={"Content-type": "application/json"}
                          )
        r.raise_for_status()
        if r.status_code == 204:
            raise Exception("Webhook returned 204: ingest not accepted")
    except Exception as e:
        err_msg = str(e)
        logger.exception(
            "Something went wrong calling the webook: {}".format(err_msg)
        )
        return resp(500, err_msg);

    return resp(200, "Ingest accepted")

