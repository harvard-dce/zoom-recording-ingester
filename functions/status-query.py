from common import setup_logging, ts_to_date_and_seconds, DATE_FORMAT, \
    TIMESTAMP_FORMAT, PipelineStatus, retrieve_schedule, schedule_days
import json
import boto3
from boto3.dynamodb.conditions import Key
from os import getenv as env
from urllib.parse import quote, parse_qs
from datetime import datetime, timedelta
import time
import hmac
import hashlib
import requests
from pytz import timezone

import logging

logger = logging.getLogger()

PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")
SLACK_SIGNING_SECRET = env("SLACK_SIGNING_SECRET")
SECONDS_PER_DAY = 86400
PRETTY_TIMESTAMP_FORMAT = "%B %d, %Y at %-I:%M%p"
STACK_NAME = env("STACK_NAME")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")


def resp_400(msg):
    logger.error(f"http 400 response: {msg}")
    return {"statusCode": 400, "headers": {}, "body": msg}


def slack_error_response(msg):
    logger.error(f"Slack error response: {msg}")
    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({
            "response_type": "ephemeral",
            "text": msg
        })
    }


@setup_logging
def handler(event, context):

    logger.info(event)

    meeting_id = None
    request_seconds = None
    slackbot = False
    if slack_event(event):
        if not valid_slack_request(event):
            return slack_error_response("Slack request verification failed.")
        slackbot = True
        query = parse_qs(event["body"])
        logger.info(query)
        meeting_id = int(query["text"][0])
        response_url = query["response_url"][0]
    else:
        query = event["queryStringParameters"]
        if "meeting_id" in query:
            meeting_id = int(query["meeting_id"])
        elif "seconds" in query:
            request_seconds = int(query["seconds"])
        else:
            return resp_400(
                "Missing identifer in query params. "
                "Must include one of 'meeting_id', 'recording_id', or 'seconds'"
            )

    dynamodb = boto3.resource("dynamodb")
    logger.info(PIPELINE_STATUS_TABLE)
    status_table = dynamodb.Table(PIPELINE_STATUS_TABLE)

    if meeting_id:
        r = status_table.query(
            IndexName="mid_index",
            KeyConditionExpression=(Key("meeting_id").eq(meeting_id))
        )
        items = r["Items"]
    elif request_seconds:
        now = datetime.utcnow()
        logger.info(f"Retrieving records updated within the last {request_seconds} seconds")
        today, time_in_seconds = ts_to_date_and_seconds(now)

        if request_seconds > SECONDS_PER_DAY:
            return resp_400(
                f"Invalid number of seconds. Seconds must be <= {SECONDS_PER_DAY}"
            )
        elif time_in_seconds < request_seconds:
            # handle case in which request spans two dates
            items = request_recent_items(status_table, today, 0)
            remaining = time_in_seconds - request_seconds
            ts = today.strptime(DATE_FORMAT)
            yesterday = (ts - timedelta(days=1)).strftime(DATE_FORMAT)
            items += request_recent_items(
                status_table,
                yesterday,
                SECONDS_PER_DAY - remaining
            )
        else:
            items = request_recent_items(
                status_table,
                today,
                time_in_seconds - request_seconds
            )

    logger.info(items)

    records = []
    for item in items:
        logger.info(item)
        date = datetime.strptime(item["update_date"], DATE_FORMAT)
        ts = date + timedelta(seconds=int(item["update_time"]))
        last_updated = ts.strftime(TIMESTAMP_FORMAT)

        record = {
            "last_updated": last_updated,
            "status": item["pipeline_state"],
            "origin": item["origin"],
            "recording": {
                "meeting_id": int(item["meeting_id"]),
                "recording_id": item["recording_id"],
                "topic": item["topic"],
                "start_time": item["recording_start_time"]
            }
        }
        if "reason" in item:
            record["reason"] = item["reason"]

        records.append(record)

    if slackbot:
        try:
            if not records:
                slack_error_response(
                    f"No recordings found for Zoom MID {meeting_id}"
                )
            response = slack_response(records)
            r = requests.post(response_url, data="This is a test")
            logger.info(f"Response URL response: {r.status_code}")
        except Exception as e:
            logger.error(f"Error generating slack response: {str(e)}")
            return slack_error_response(
                "We're sorry! There was an error when handling your request."
            )
    else:
        response = {"records": records}

    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps(response)
    }


def request_recent_items(table, date, seconds):
    logger.warning(f"Request items since {seconds}")
    r = table.query(
        IndexName="time_index",
        KeyConditionExpression=(
            Key("update_date").eq(date) & Key("update_time").gte(seconds)
        )
    )
    return r["Items"]


def slack_event(event):
    return "Slackbot" in event["headers"]["User-Agent"]


def pretty_local_time(ts):
    tz = timezone(LOCAL_TIME_ZONE)
    utc = datetime.strptime(ts, TIMESTAMP_FORMAT).replace(tzinfo=timezone("UTC"))
    return utc.astimezone(tz).strftime(PRETTY_TIMESTAMP_FORMAT)


def valid_slack_request(event):
    slack_ts = event["headers"]["X-Slack-Request-Timestamp"]
    signature = event["headers"]["X-Slack-Signature"]
    version = signature.split("=")[0]
    body = event["body"]

    basestring = f"{version}:{slack_ts}:{body}"

    # rejecting request more than 5 minutes old
    if abs(int(slack_ts) - time.time()) > 300:
        return False

    h = hmac.new(
        bytes(SLACK_SIGNING_SECRET, "UTF-8"),
        bytes(basestring, "UTF-8"), 
        hashlib.sha256
    )

    return hmac.compare_digest(f"{version}={str(h.hexdigest())}", signature)


def slack_response(records):
    mid = records[0]["recording"]["meeting_id"]
    topic = records[0]["recording"]["topic"]
    schedule = retrieve_schedule(mid)
    logger.info(schedule)

    events = ""
    for i, event in enumerate(schedule["events"]):
        event_time = datetime.strptime(event["time"], "%H:%M").strftime("%-I:%M%p")
        events += f":calendar: {schedule['course_code']} {event['title']} "
        events += f"on {schedule_days[event['day']]} at {event_time}"

        if i + 1 < len(schedule["events"]):
            events += "\n"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{topic}"
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "plain_text",
                    "text": f"Source: {STACK_NAME}",
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Zoom Meeting:* {mid} :arrow_right: *Opencast Series:* {schedule['opencast_series_id']}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": events
            }
        }
    ]

    for record in records:
        start_time = pretty_local_time(record["recording"]["start_time"])
        last_updated = pretty_local_time(record["last_updated"])
        rec_id = quote(record['recording']['recording_id'])
        mgmt_url = f"https://zoom.us/recording/management/detail?meeting_id={rec_id}"
        on_demand = "Yes" if record["origin"] == "on_demand" else "No"

        record_detail_fields = [
            {
                "type": "mrkdwn",
                "text": f"*Last Updated:*\n{last_updated}"
            },
            {
                "type": "mrkdwn",
                "text": f"*Status*: {status_description(record)}\n"
            }
        ]

        record_data = [
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":movie_camera: *Recording on {start_time}*\n"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Zoom+ ingest request?* {on_demand}"
                }
            },
            {
                "type": "section",
                "fields": record_detail_fields
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Recording management (admins and hosts only)"
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View in Zoom"
                    },
                    "action_id": "action_id_0",
                    "url": mgmt_url
                }
            }
        ]

        blocks.extend(record_data)

    slack_response = {
        "response_type": "in_channel",
        "blocks": blocks
    }
    logger.info({"slack_response": slack_response})
    return slack_response


def status_description(record):

    status = record["status"]

    # Processing
    if status == PipelineStatus.ON_DEMAND_RECEIVED.name:
        status_msg = "Received Zoom+ request."
    elif status == PipelineStatus.WEBHOOK_RECEIVED.name:
        status_msg = "Received by ZIP."
    elif status == PipelineStatus.SENT_TO_DOWNLOADER.name:
        status_msg = "Received by ZIP."
    elif status == PipelineStatus.OC_SERIES_FOUND.name:
        status_msg = "Found match in schedule. Downloading files."
    # Ingesting
    elif status == PipelineStatus.SENT_TO_UPLOADER.name:
        status_msg = "Ready to ingest to Opencast"
    elif status == PipelineStatus.UPLOADER_RECEIVED.name:
        status_msg = "Ingesting to Opencast."
    # Success
    elif status == PipelineStatus.SENT_TO_OPENCAST.name:
        status_msg = ":white_check_mark: Complete. Ingested to Opencast."
    # Ignored
    elif status == PipelineStatus.IGNORED.name:
        status_msg = "Ignored by ZIP."
    # Failures
    elif status == PipelineStatus.WEBHOOK_FAILED.name:
        status_msg = ":x: Failed at receiving stage."
    elif status == PipelineStatus.DOWNLOADER_FAILED.name:
        status_msg = ":x: Failed at file download stage."
    elif status == PipelineStatus.UPLOADER_FAILED.name:
        status_msg = ":x: Failed at ingest to Opencast stage"
    else:
        status_msg = status

    if "reason" in record:
        status_msg += f"\n{record['reason']}."

    return status_msg
