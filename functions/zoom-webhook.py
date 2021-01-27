import json
from os import getenv as env
import common
from datetime import datetime, timedelta
from pytz import timezone
import boto3

import logging
logger = logging.getLogger()

DOWNLOAD_QUEUE_NAME = env("DOWNLOAD_QUEUE_NAME")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
DEFAULT_MESSAGE_DELAY = 300


class BadWebhookData(Exception):
    pass


class NoMp4Files(Exception):
    pass


def resp_204(msg):
    """
    For requests from the zoom service, we return a 204 even in cases where
    the recording is rejected (e.g., no mp4 files) because anything else will
    be considered a retry-able error by Zoom
    """
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


INGEST_EVENT_TYPES = {
    # event type            no mp4 files response callback
    "recording.completed":  resp_204,
    "on.demand.ingest":     resp_400
}


@common.setup_logging
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

    if "on_demand_request_id" in payload:
        origin = "on_demand"
        correlation_id = payload["on_demand_request_id"]
    else:
        origin = "webhook_notification"
        correlation_id = context.aws_request_id

    try:
        validate_payload(payload)
        common.set_pipeline_status(
            correlation_id,
            common.PipelineStatus.WEBHOOK_RECEIVED,
            meeting_id=payload["object"]["id"],
            recording_id=payload["object"]["uuid"],
            recording_start_time=payload["object"]["start_time"],
            topic=payload["object"]["topic"],
            origin=origin
        )
    except BadWebhookData as e:
        common.set_pipeline_status(
            correlation_id,
            common.PipelineStatus.WEBHOOK_FAILED,
            reason="bad webhook data",
            origin=origin
        )
        return resp_400(f"Bad data: {str(e)}")
    except NoMp4Files as e:
        common.set_pipeline_status(
            correlation_id,
            common.PipelineStatus.WEBHOOK_FAILED,
            reason="no mp4 files",
            origin=origin
        )
        resp_callback = INGEST_EVENT_TYPES[zoom_event]
        return resp_callback(str(e))

    sqs_message = construct_sqs_message(payload, correlation_id, zoom_event)
    logger.info({"sqs_message": sqs_message})

    if zoom_event == "on.demand.ingest":
        delay = 0
    else:
        delay = DEFAULT_MESSAGE_DELAY
    send_sqs_message(sqs_message, delay)
    common.set_pipeline_status(
        correlation_id,
        common.PipelineStatus.SENT_TO_DOWNLOADER
    )

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

        # make sure there's some mp4 files in here somewhere
        mp4_files = any(x["file_type"].lower() == "mp4" for x in files)
        if not mp4_files:
            raise NoMp4Files("No mp4 files in recording data")

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

    except NoMp4Files:
        # let these bubble up as we handle them differently depending
        # on who the caller is
        raise
    except Exception as e:
        raise BadWebhookData("Unrecognized payload format. {}".format(e))


def construct_sqs_message(payload, correlation_id, zoom_event):
    now = datetime.strftime(
                timezone(LOCAL_TIME_ZONE).localize(datetime.today()),
                common.TIMESTAMP_FORMAT)

    if "allow_multiple_ingests" in payload:
        allow_multiple_ingests = payload["allow_multiple_ingests"]
    else:
        allow_multiple_ingests = False

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
        "allow_multiple_ingests": allow_multiple_ingests,
        "correlation_id": correlation_id,
        "received_time": now
    }

    if "on_demand_series_id" in payload:
        sqs_message["on_demand_series_id"] = payload["on_demand_series_id"]

    # not used in downloader or uploader but useful for cloudwatch dashboard
    if "total_size" in payload["object"]:
        sqs_message["zoom_total_size_bytes"] = payload["object"]["total_size"]

    if zoom_event == "recording.completed":
        zoom_processing_mins = estimated_processing_mins(
            sqs_message["start_time"],
            sqs_message["duration"]
        )
        sqs_message["zoom_processing_minutes"] = zoom_processing_mins

    return sqs_message


def estimated_processing_mins(start_ts, duration_in_minutes):
    rec_start = datetime.strptime(start_ts, common.TIMESTAMP_FORMAT)
    rec_end = rec_start + timedelta(minutes=duration_in_minutes)
    processing_time = datetime.utcnow() - rec_end
    return processing_time.total_seconds() // 60


def send_sqs_message(message, delay):
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
