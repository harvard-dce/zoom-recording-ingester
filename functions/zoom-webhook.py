import json
import hmac
from os import getenv as env
from datetime import datetime, timedelta
from pytz import timezone
import boto3
from utils import (
    setup_logging,
    TIMESTAMP_FORMAT,
    ZoomStatus,
    PipelineStatus,
    set_pipeline_status,
    record_exists,
    set_recording_events,
)

import logging

logger = logging.getLogger()

DOWNLOAD_QUEUE_NAME = env("DOWNLOAD_QUEUE_NAME")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
WEBHOOK_VALIDATION_SECRET_TOKEN = env("WEBHOOK_VALIDATION_SECRET_TOKEN")
DEFAULT_MESSAGE_DELAY = 300
ZOOM_WEBINAR_TYPES = [5, 6, 9]
# ZIP-74: now ingesting chat files
FILE_TYPES = ["mp4", "chat"]


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
    logger.info(f"http 204 response: {msg}")
    return {"statusCode": 204, "headers": {}, "body": ""}  # 204 = no content


def resp_400(msg):
    logger.error(f"http 400 response: {msg}")
    return {
        "statusCode": 400,
        "headers": {},
        "body": msg,
    }


STATUS_EVENT_TYPES = [
    "recording.started",
    "recording.resumed",
    "recording.paused",
    "recording.stopped",
    "meeting.ended",
    "webinar.ended",
]


INGEST_EVENT_TYPES = {
    # event type            no mp4 files response callback
    "recording.completed": resp_204,
    "on.demand.ingest": resp_400,
}

VALID_EVENT_TYPES = (
    STATUS_EVENT_TYPES + list(INGEST_EVENT_TYPES) + ["endpoint.url_validation"]
)


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
    if not zoom_event:
        return resp_400("Request has no event type?")
    elif zoom_event not in VALID_EVENT_TYPES:
        return resp_204(f"Handling not implemented for event '{zoom_event}'")

    logger.info(f"Processing event type: {zoom_event}")

    if "payload" not in body:
        return resp_400("Missing payload field in webhook notification body.")

    payload = body["payload"]

    # Yearly webhook endpoint validation event. As in this should only happen once
    # per year per ZIP instance. We simply hash the provided token with our secret
    # token value, then respond with both the original and hashed version.
    if zoom_event == "endpoint.url_validation":
        logger.info("performing webhook validation")

        if not WEBHOOK_VALIDATION_SECRET_TOKEN:
            return resp_204("Validation not enabled for this endpoint")

        plain_token = payload["plainToken"]
        encrypted_token = hmac.new(
            WEBHOOK_VALIDATION_SECRET_TOKEN.encode("utf-8"),
            plain_token.encode("utf-8"),
            "sha256",
        ).hexdigest()

        response_json = {
            "plainToken": plain_token,
            "encryptedToken": encrypted_token,
        }
        logger.info(response_json)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps(response_json),
        }

    if zoom_event == "on.demand.ingest":
        origin = "on_demand"
        zip_id = payload["zip_id"]
    else:
        if "object" not in payload or "uuid" not in payload["object"]:
            return resp_400("Bad data: missing uuid")
        origin = "webhook_notification"
        zip_id = f"auto-ingest-{payload['object']['uuid']}"

    download_token = body.get("download_token")
    if zoom_event in INGEST_EVENT_TYPES and not download_token:
        return resp_400("Missing download token")

    try:
        validate_payload(payload, zoom_event)
        status = event_status(zoom_event, zip_id)
        if not status:
            return resp_204("Recording status ignored.")

        # Set recording.paused/resumed event times in separate table
        set_recording_events(
            zoom_uuid=payload["object"]["uuid"],
            zoom_event=zoom_event,
            zoom_event_timestamp=body.get("event_ts"),
        )

        set_pipeline_status(
            zip_id,
            status,
            meeting_id=payload["object"]["id"],
            recording_id=payload["object"]["uuid"],
            recording_start_time=payload["object"]["start_time"],
            topic=payload["object"]["topic"],
            origin=origin,
        )

        if zoom_event not in INGEST_EVENT_TYPES:
            return resp_204("Recording status updated.")

        validate_recording_files(payload["object"]["recording_files"])

    except BadWebhookData as e:
        if zoom_event not in INGEST_EVENT_TYPES:
            return resp_204(f"Ignore {zoom_event}. {e}")

        set_pipeline_status(
            zip_id,
            PipelineStatus.WEBHOOK_FAILED,
            reason="Bad webhook data",
            origin=origin,
        )
        return resp_400(f"Bad data: {str(e)}")
    except NoMp4Files as e:
        set_pipeline_status(
            zip_id,
            PipelineStatus.IGNORED,
            reason="No mp4 files",
            origin=origin,
        )
        resp_callback = INGEST_EVENT_TYPES[zoom_event]
        return resp_callback(str(e))

    sqs_message = construct_sqs_message(
        payload,
        zip_id,
        zoom_event,
        download_token,
    )
    logger.info({"sqs_message": sqs_message})

    if zoom_event == "on.demand.ingest":
        delay = 0
    else:
        delay = DEFAULT_MESSAGE_DELAY
    send_sqs_message(sqs_message, delay)
    set_pipeline_status(zip_id, PipelineStatus.SENT_TO_DOWNLOADER)

    return {
        "statusCode": 200,
        "headers": {},
        "body": "Success",
    }


def event_status(zoom_event, zip_id):
    if zoom_event in INGEST_EVENT_TYPES:
        return PipelineStatus.WEBHOOK_RECEIVED
    if zoom_event == "recording.started" or zoom_event == "recording.resumed":
        return ZoomStatus.RECORDING_IN_PROGRESS
    elif zoom_event == "recording.paused":
        return ZoomStatus.RECORDING_PAUSED
    elif zoom_event == "recording.stopped":
        return ZoomStatus.RECORDING_STOPPED
    elif zoom_event == "meeting.ended" or zoom_event == "webinar.ended":
        if record_exists(zip_id):
            return ZoomStatus.RECORDING_PROCESSING

    return None


def validate_payload(payload, zoom_event):
    required_payload_fields = ["object"]

    required_object_fields = [
        "id",  # zoom series id
        "uuid",  # unique id of the meeting instance
        "topic",
        "start_time",
    ]

    if zoom_event in INGEST_EVENT_TYPES:
        required_object_fields.extend(
            ["host_id", "duration", "recording_files"]
        )

    try:
        for field in required_payload_fields:
            if field not in payload.keys():
                raise BadWebhookData(
                    f"Missing required payload field '{field}'. "
                    f"Keys found: {payload.keys()}"
                )

        obj = payload["object"]
        for field in required_object_fields:
            if field not in obj.keys():
                raise BadWebhookData(
                    f"Missing required object field '{field}'. "
                    f"Keys found: {obj.keys()}"
                )
    except Exception as e:
        raise BadWebhookData("Unrecognized payload format. {}".format(e))


def validate_recording_files(files):
    required_file_fields = [
        "id",  # unique id for the file
        "recording_start",
        "recording_end",
        "download_url",
        "file_type",
        "recording_type",
    ]

    try:
        # make sure there's some mp4 files in here somewhere
        mp4_files = any(x["file_type"].lower() == "mp4" for x in files)
        if not mp4_files:
            raise NoMp4Files("No mp4 files in recording data")

        for f in files:
            if "file_type" not in f:
                raise BadWebhookData("Missing required file field 'file_type'")
            if f["file_type"].lower() not in FILE_TYPES:
                continue
            for field in required_file_fields:
                if field not in f.keys():
                    raise BadWebhookData(
                        f"Missing required file field '{field}'"
                    )
            if "status" in f and f["status"].lower() != "completed":
                raise BadWebhookData(
                    f"File with incomplete status {f['status']}"
                )

    except NoMp4Files:
        # let these bubble up as we handle them differently depending
        # on who the caller is
        raise
    except Exception as e:
        raise BadWebhookData(f"Unrecognized payload format. {str(e)}")


def construct_sqs_message(payload, zip_id, zoom_event, download_token):
    now = datetime.strftime(
        timezone(LOCAL_TIME_ZONE).localize(datetime.today()),
        TIMESTAMP_FORMAT,
    )

    recording_files = []
    for file in payload["object"]["recording_files"]:
        if file["file_type"].lower() in FILE_TYPES:
            recording_files.append(
                {
                    "recording_id": file["id"],
                    "recording_start": file["recording_start"],
                    "recording_end": file["recording_end"],
                    "download_url": file["download_url"],
                    "file_type": file["file_type"],
                    "recording_type": file["recording_type"],
                }
            )

    sqs_message = {
        "uuid": payload["object"]["uuid"],
        "zoom_series_id": payload["object"]["id"],
        "topic": payload["object"]["topic"],
        "start_time": payload["object"]["start_time"],
        "duration": payload["object"]["duration"],
        "host_id": payload["object"]["host_id"],
        "recording_files": recording_files,
        "on_demand_ingest": zoom_event == "on.demand.ingest",
        "zip_id": zip_id,
        "received_time": now,
        "download_token": download_token,
    }

    for optional_param in [
        "allow_multiple_ingests",
        "ingest_all_mp4",
        "on_demand_series_id",
        "oc_workflow",
    ]:
        if optional_param in payload:
            sqs_message[optional_param] = payload[optional_param]

    for flag in ["allow_multiple_ingests", "ingest_all_mp4"]:
        if flag not in sqs_message:
            sqs_message[flag] = False

    # not used in downloader or uploader but useful for cloudwatch dashboard
    if "total_size" in payload["object"]:
        sqs_message["zoom_total_size_bytes"] = payload["object"]["total_size"]

    if zoom_event == "recording.completed":
        zoom_processing_mins = estimated_processing_mins(
            sqs_message["start_time"], sqs_message["duration"]
        )
        sqs_message["zoom_processing_minutes"] = zoom_processing_mins

    return sqs_message


def estimated_processing_mins(start_ts, duration_in_minutes):
    rec_start = datetime.strptime(start_ts, TIMESTAMP_FORMAT)
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
            DelaySeconds=delay,
        )

    except Exception as e:
        logger.error(
            "Error when sending SQS message for meeting "
            f"uuid {message['uuid']} :{e}"
        )
        raise

    logger.debug({"Message sent": message_sent})
