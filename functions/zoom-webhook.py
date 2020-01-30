import json
from urllib.parse import parse_qsl
from os import getenv as env
from common import setup_logging, api_request
from datetime import datetime
from pytz import timezone
import boto3

import logging
logger = logging.getLogger()

DOWNLOAD_QUEUE_NAME = env('DOWNLOAD_QUEUE_NAME')
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
ZOOM_API_BASE_URL = env('ZOOM_API_BASE_URL')
DEFAULT_MESSAGE_DELAY = 300
PARALLEL_ENDPOINT = env('PARALLEL_ENDPOINT')
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class BadWebhookData(Exception):
    pass


def resp_204(msg):
    logger.info("http 204 response: {}".format(msg))
    return {
        'statusCode': 204,
        'headers': {},
        'body': ""  # 204 = no content
    }


def resp_400(msg):
    logger.error("http 400 response: {}".format(msg))
    return {
        'statusCode': 400,
        'headers': {},
        'body': msg
    }


@setup_logging
def handler(event, context):
    """
    This function accepts the incoming POST relay from the API Gateway endpoint that
    serves as the Zoom webhook endpoint. It checks for the appropriate event status
    type, fetches info about the meeting host, and then passes responsibility on
    to the downloader function via a queue.
    """

    if 'body' not in event:
        return resp_400("bad data: no body in event")

    try:
        payload = parse_payload(event['body'])
        logger.info({'payload': payload})
    except BadWebhookData as e:
        return resp_400("bad webhook payload data: {}".format(str(e)))

    if payload["status"] != "RECORDING_MEETING_COMPLETED":
        return resp_204(
            "Handling not implemented for status '{}'"
            .format(payload["status"])
        )

    now = datetime.strftime(
                timezone(LOCAL_TIME_ZONE).localize(datetime.today()),
                TIMESTAMP_FORMAT)

    recording_files = []
    for file in payload["object"]["recording_files"]:
        recording_files.append({
            "recording_id": file["id"],
            "recording_start": file["recording_start"],
            "recording_end": file["recording_end"],
            "download_url": file["download_url"],
            "file_type": file["file_type"],
            "status": file["status"]
        })

    host = host_name(payload["object"]["host_id"])

    sqs_message = {
        "uuid": payload["uuid"],
        "zoom_series_id": payload["object"]["id"],
        "topic": payload["object"]["topic"],
        "start_time": payload["object"]["start_time"],
        "duration": payload["object"]["duration"],
        "host_name": host,
        "recording_files": recording_files,
        "correlation_id": context.aws_request_id,
        "received_time": now
    }

    if 'delay_seconds' in payload:
        logger.debug("Override default message delay.")
        send_sqs_message(sqs_message, delay=payload['delay_seconds'])
    else:
        send_sqs_message(sqs_message)

    return {
        'statusCode': 200,
        'headers': {},
        'body': "Success"
    }


def parse_payload(event_body):

    required_payload_fields = [
        "object",
        "uuid",  # unique id of the meeting instance
        "status"
    ]
    required_object_fields = [
        "id",  # zoom series id
        "host_id",
        "topic",
        "start_time",
        "duration",  # duration in minutes
        "recording_files"
    ]
    requried_file_fields = [
        "id",  # unique id for the file
        "recording_start",
        "recording_end",
        "download_url",
        "file_type",
        "recording_type",
        "status"
    ]

    try:
        body = json.loads(event_body)
        if "payload" not in body:
            raise BadWebhookData(
                "No payload found in event body."
            )
        payload = body["payload"]
        logger.info("Got payload {}".format(payload))

        for field in required_payload_fields:
            if field not in payload.keys():
                raise BadWebhookData(
                    "Missing required payload field {}.\n Keys found: {}"
                    .format(field, payload.keys()))

        obj = payload['object']
        for field in required_object_fields:
            if field not in obj.keys():
                raise BadWebhookData(
                    "Missing required object field {}.\n Keys found: {}"
                    .format(field, obj.keys()))

        files = obj["recording_files"]
        for file in files:
            for field in requried_file_fields:
                if field not in file.keys():
                    raise BadWebhookData(
                        "Missing required file field {}".format(field))

    except Exception as e:
        raise BadWebhookData("Unrecognized payload format. {}".format(e))

    return payload


def host_name(host_id):
    resp = api_request(
        ZOOM_API_KEY,
        ZOOM_API_SECRET,
        "{}users/{}".format(ZOOM_API_BASE_URL, host_id)
    )
    logger.info(resp)
    name = "{} {}".format(resp["first_name"], resp["last_name"])
    return name


def send_sqs_message(message, delay=DEFAULT_MESSAGE_DELAY):

    logger.debug("SQS sending start...")
    sqs = boto3.resource('sqs')

    try:
        download_queue = sqs.get_queue_by_name(QueueName=DOWNLOAD_QUEUE_NAME)

        message_sent = download_queue.send_message(
            MessageBody=json.dumps(message),
            DelaySeconds=delay
        )

    except Exception as e:
        logger.error("Error when sending SQS message for meeting uuid {} :{}"
                     .format(message['uuid'], e))
        raise

    logger.debug({"Message sent": message_sent})
