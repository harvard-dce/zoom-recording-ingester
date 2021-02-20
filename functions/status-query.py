from common import setup_logging, ts_to_date_and_seconds, DATE_FORMAT, TIMESTAMP_FORMAT
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from os import getenv as env
from urllib.parse import quote, unquote, parse_qs
from datetime import datetime, timedelta
import time
import hmac
import hashlib

import logging

logger = logging.getLogger()

PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")
SLACK_SIGNING_SECRET = env("SLACK_SIGNING_SECRET")
SECONDS_PER_DAY = 86400
PRETTY_TIMESTAMP_FORMAT = "%A, %B %d, %Y at %I:%M%p"
STACK_NAME = env("STACK_NAME")


def resp_400(msg):
    logger.error("http 400 response: {}".format(msg))
    return {"statusCode": 400, "headers": {}, "body": msg}


def resp_401(msg):
    logger.error("http 400 response: {}".format(msg))
    return {"statusCode": 401, "headers": {}, "body": msg}


@setup_logging
def handler(event, context):

    logger.info(event)

    meeting_id = None
    recording_id = None
    request_seconds = None
    slack_integration = False
    if event["httpMethod"] == "GET":
        query = event["queryStringParameters"]
        if "meeting_id" in query:
            meeting_id = int(query["meeting_id"])
        elif "recording_id" in query:
            recording_id = unquote(query["recording_id"])
        elif "seconds" in query:
            request_seconds = int(query["seconds"])
        else:
            return resp_400(
                "Missing identifer in query params. "
                "Must include one of 'meeting_id', 'recording_id', or 'seconds'"
            )
    else:
        if not valid_slack_request(event):
            resp_401("Slack request verification failed")
        slack_integration = True
        query = parse_qs(event["body"])
        logger.info(query)
        meeting_id = int(query["text"][0])

    dynamodb = boto3.resource("dynamodb")
    logger.info(PIPELINE_STATUS_TABLE)
    table = dynamodb.Table(PIPELINE_STATUS_TABLE)

    if meeting_id:
        r = table.scan(FilterExpression=Attr("meeting_id").eq(meeting_id))
        items = r["Items"]
    elif recording_id:
        r = table.scan(FilterExpression=Attr("recording_id").eq(recording_id))
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
            items = request_recent_items(table, today, 0)
            remaining = time_in_seconds - request_seconds
            ts = today.strptime(DATE_FORMAT)
            yesterday = (ts - timedelta(days=1)).strftime(DATE_FORMAT)
            items += request_recent_items(table, yesterday, SECONDS_PER_DAY - remaining)
        else:
            items = request_recent_items(table, today, time_in_seconds - request_seconds)

    logger.info(items)

    records = []
    for item in items:
        logger.info(item)
        if "last_update" in item:
            last_updated = item["last_update"]
            last_updated = datetime.strptime(last_updated, TIMESTAMP_FORMAT).strftime(PRETTY_TIMESTAMP_FORMAT)
        else:
            date = datetime.strptime(item["update_date"], DATE_FORMAT)
            ts = date + timedelta(seconds=int(item["update_time"]))
            last_updated = ts.strftime(PRETTY_TIMESTAMP_FORMAT)

        record = {
            "last_updated": last_updated,
            "status": item["pipeline_state"],
            "origin": item["origin"],
            "recording": {
                "meeting_id": int(item["meeting_id"]),
                "recording_id": item["recording_id"],
                "topic": item["topic"],
                "recording_start_time": item["recording_start_time"]
            }
        }
        if "reason" in item:
            record["status_reason"] = item["reason"]

        records.append(record)

    if slack_integration:

        s = f"*Data from {STACK_NAME}*\n"
        s += f"*Zoom MID:* {items[0]['meeting_id']}\n"
        s += f"*Topic:* {items[0]['topic']}\n\n"

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": s
                }
            }
        ]

        for item in items:
            mgmt_url = f"https://zoom.us/recording/management/detail?meeting_id={quote(item['recording_id'])}"
            heading = f"*Recording from {datetime.strptime(item['recording_start_time'], TIMESTAMP_FORMAT).strftime(PRETTY_TIMESTAMP_FORMAT)}*\n"

            on_demand = "Yes" if item["origin"] == "on_demand" else "No"
            body = f"*Zoom+ ingest request?* {on_demand}\n"

            status = item["pipeline_state"]
            status_msg = ""
            if status == "IGNORED":
                status_msg = f"*Status*: This message was ignored by ZIP on {last_updated}"
                if "reason" in item and item["reason"] == "No opencast series match":
                    status_msg += " because there was no match in the schedule."
            else:
                status_msg = f"Status: {item['pipeline_state']} (updated {last_updated})\n"
                if "reason" in item:
                    status_msg += f"Reason: {item['reason']}"

            body += status_msg + "\n"
            body += f"*Recordings page:* <{mgmt_url}>\n"

            blocks.extend([
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{heading}{body}"
                    }
                }
            ])

        response = {
            "response_type": "in_channel",
            "blocks": blocks
        }
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
        ),
    )
    return r["Items"]


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
