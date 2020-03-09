import json
from os import getenv as env
from common import setup_logging, TIMESTAMP_FORMAT
from datetime import datetime
from pytz import timezone
import boto3

import logging
logger = logging.getLogger()

DOWNLOAD_QUEUE_NAME = env("DOWNLOAD_QUEUE_NAME")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
DEFAULT_MESSAGE_DELAY = 300
INGEST_EVENT_TYPES = [
    "recording.completed",
    "on.demand.ingest"
]


class BadWebhookData(Exception):
    pass


def resp_204(msg):
    logger.info("http 204 response: {}".format(msg))
    return {
        "statusCode": 204,
        "headers": {},
        "body": ""  # 204 = no content
    }


def resp_400(msg):
    logger.error("http 400 response: {}".format(msg))
    return {
        "statusCode": 400,
        "headers": {},
        "body": msg
    }


@setup_logging
def handler(event, context):
    """
    This function accepts the incoming POST relay from the API Gateway endpoint
    that serves as the Zoom webhook endpoint. It checks for the appropriate
    event status type, fetches info about the meeting host, and then passes
    responsibility on to the downloader function via a queue.
    """

    if "body" not in event:
        return resp_400("Bad data. No body found in event.")

    try:
        body = json.loads(event["body"])
        logger.info({"webhook_notification": body})
    except json.JSONDecodeError:
        return resp_400("Webhook notification body is not valid json.")

    zoom_event = body.get("event")
    if zoom_event is None:
        return resp_400("Request has no event type?")
    elif zoom_event not in INGEST_EVENT_TYPES:
        return resp_204(
            "Handling not implemented for event '{}'".format(zoom_event)
        )
    logger.info("Processing event type: {}".format(zoom_event))

    if "payload" not in body:
        return resp_400("Missing payload field in webhook notification body.")
    payload = body["payload"]

    try:
        validate_payload(payload)
    except BadWebhookData as e:
        return resp_400("Bad data: {}".format(str(e)))

    sqs_message = construct_sqs_message(payload, context)
    logger.info({"SQS message to downloader": sqs_message})

    if "delay_seconds" in payload:
        logger.debug("Override default message delay.")
        send_sqs_message(sqs_message, delay=payload["delay_seconds"])
    else:
        send_sqs_message(sqs_message)

    return {
        "statusCode": 200,
        "headers": {},
        "body": "Success"
    }


def validate_payload(payload):
    required_payload_fields = [
        "object"
    ]
    required_object_fields = [
        "id",  # zoom series id
        "uuid",  # unique id of the meeting instance,
        "host_id",
        "topic",
        "start_time",
        "duration",  # duration in minutes
        "recording_files"
    ]
    required_file_fields = [
        "id",  # unique id for the file
        "recording_start",
        "recording_end",
        "download_url",
        "file_type",
        "recording_type"
    ]

    try:
        for field in required_payload_fields:
            if field not in payload.keys():
                raise BadWebhookData(
                    "Missing required payload field '{}'. Keys found: {}"
                    .format(field, payload.keys()))

        obj = payload["object"]
        for field in required_object_fields:
            if field not in obj.keys():
                raise BadWebhookData(
                    "Missing required object field '{}'. Keys found: {}"
                    .format(field, obj.keys()))

        files = obj["recording_files"]
        for file in files:
            if "file_type" not in file:
                raise BadWebhookData("Missing required file field 'file_type'")
            if file["file_type"].lower() != "mp4":
                continue
            for field in required_file_fields:
                if field not in file.keys():
                    raise BadWebhookData(
                        "Missing required file field '{}'".format(field))
            if "status" in file and file["status"].lower() != "completed":
                raise BadWebhookData(
                    "File with incomplete status {}".format(file["status"])
                )

    except Exception as e:
        raise BadWebhookData("Unrecognized payload format. {}".format(e))


def construct_sqs_message(payload, context):
    now = datetime.strftime(
                timezone(LOCAL_TIME_ZONE).localize(datetime.today()),
                TIMESTAMP_FORMAT)

    recording_files = []
    for file in payload["object"]["recording_files"]:
        if file["file_type"].lower() == "mp4":
            recording_files.append({
                "recording_id": file["id"],
                "recording_start": file["recording_start"],
                "recording_end": file["recording_end"],
                "download_url": file["download_url"],
                "file_type": file["file_type"],
                "recording_type": file["recording_type"]
            })

    sqs_message = {
        "uuid": payload["object"]["uuid"],
        "zoom_series_id": payload["object"]["id"],
        "topic": payload["object"]["topic"],
        "start_time": payload["object"]["start_time"],
        "duration": payload["object"]["duration"],
        "host_id": payload["object"]["host_id"],
        "recording_files": recording_files,
        "correlation_id": context.aws_request_id,
        "received_time": now
    }

    if "on_demand_series_id" in payload:
        sqs_message["on_demand_series_id"] = payload["on_demand_series_id"]

    return sqs_message


def send_sqs_message(message, delay=DEFAULT_MESSAGE_DELAY):

    logger.debug("SQS sending start...")
    sqs = boto3.resource("sqs")

    try:
        download_queue = sqs.get_queue_by_name(QueueName=DOWNLOAD_QUEUE_NAME)

        message_sent = download_queue.send_message(
            MessageBody=json.dumps(message),
            DelaySeconds=delay
        )

    except Exception as e:
        logger.error("Error when sending SQS message for meeting uuid {} :{}"
                     .format(message["uuid"], e))
        raise

    logger.debug({"Message sent": message_sent})
