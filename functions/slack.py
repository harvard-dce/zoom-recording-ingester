from common.common import setup_logging, TIMESTAMP_FORMAT, retrieve_schedule, \
    schedule_days
from common.status import PipelineStatus, status_by_mid
import json
from os import getenv as env
from urllib.parse import quote, parse_qs
from datetime import datetime
import time
import hmac
import hashlib
import requests
from pytz import timezone

import logging

logger = logging.getLogger()

SLACK_SIGNING_SECRET = env("SLACK_SIGNING_SECRET")
PRETTY_TIMESTAMP_FORMAT = "%B %d, %Y at %-I:%M%p"
STACK_NAME = env("STACK_NAME")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
RESULTS_BUTTON_TEXT = "Next 2 Results"


def resp_204(msg):
    logger.info("http 204 response: {}".format(msg))
    return {
        "statusCode": 204,
        "headers": {},
        "body": ""  # 204 = no content
    }


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


def slack_help_response():
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "These are the available ZIP commands:"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ">`/zip [zoom-mid]` See the status of the latest ZIP ingests with the specified Zoom MID."
            }
        }
    ]
    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({
            "response_type": "ephemeral",
            "blocks": blocks
        })
    }


@setup_logging
def handler(event, context):

    logger.info(event)

    try:
        if not valid_slack_request(event):
            return slack_error_response("Slack request verification failed.")
    except Exception:
        return slack_error_response("Error when validating Slack request.")

    query = parse_qs(event["body"])
    logger.info(query)

    if "command" in query:
        slash_command = True
        cmd = query["command"][0]
        text = query["text"][0]
        if text == "help":
            return slack_help_response()
        try:
            meeting_id = int(text)
        except ValueError:
            return slack_error_response(
                f"Sorry, `{cmd} {text}` is not a valid command. Try `{cmd} help`?"
            )
        response_url = query["response_url"][0]
    else:
        slash_command = False
        payload = json.loads(query["payload"][0])
        logger.info({"interaction_payload": payload})
        logger.info(
            f"Interaction from user {payload['user']['username']}"
        )
        response_url = payload["response_url"]

        action_text = payload["actions"][0]["text"]["text"]
        if action_text != RESULTS_BUTTON_TEXT:
            return resp_204(f"Ignore action: {action_text}")
        action_value = json.loads(payload["actions"][0]["value"])
        meeting_id = action_value["mid"]
        records_sent = action_value["count"]
        logger.info(f"Interaction value: {action_value}")

    records = status_by_mid(meeting_id)

    try:
        if slash_command:
            blocks = slack_response(records)
            if not blocks:
                return slack_error_response(
                    f"No recordings found for Zoom MID {meeting_id}"
                )
            response = {
                "response_type": "in_channel",
                "blocks": blocks
            }
            return {
                "statusCode": 200,
                "headers": {},
                "body": json.dumps(response)
            }
        else:
            blocks = slack_response(records, max_results=records_sent + 2)
            logger.info(f"Send interaction response to response_url: {response_url}")
            r = requests.post(
                response_url,
                json={
                    "response_type": "in_channel",
                    "replace_original": True,
                    "blocks": blocks
                }
            )
            logger.info(f"Response url returned status {r.status_code}")
            r.raise_for_status()
            return resp_204("Interaction response successful.")
    except Exception as e:
        logger.error(f"Error generating slack response: {str(e)}")
        if slash_command:
            return slack_error_response(
                f"We're sorry! There was an error when handling your request: `{cmd} {text}`"
            )
        else:
            return resp_400("Error handling interaction.")


def pretty_local_time(ts):
    tz = timezone(LOCAL_TIME_ZONE)
    utc = datetime.strptime(ts, TIMESTAMP_FORMAT).replace(tzinfo=timezone("UTC"))
    return utc.astimezone(tz).strftime(PRETTY_TIMESTAMP_FORMAT)


def valid_slack_request(event):
    if "Slackbot" not in event["headers"]["User-Agent"]:
        return False

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


def slack_response(records, max_results=2):
    if not records:
        return None
    # sort by recording start time
    records = sorted(
        records,
        key=lambda r: r["recording"]["start_time"],
        reverse=True
    )
    # limit to 2 most recent results
    more_results = len(records) > max_results
    records = records[:max_results]

    mid = records[0]["recording"]["meeting_id"]
    topic = records[0]["recording"]["topic"]
    schedule = retrieve_schedule(mid)
    if schedule:
        logger.info({"schedule": schedule})
        events = ""
        for i, event in enumerate(schedule["events"]):
            event_time = datetime.strptime(
                event["time"], "%H:%M"
            ).strftime("%-I:%M%p")
            events += f":calendar: {schedule['course_code']} {event['title']} "
            events += f"on {schedule_days[event['day']]} at {event_time}"

            if i + 1 < len(schedule["events"]):
                events += "\n"
        opencast_mapping = f":arrow_right: *Opencast Series:* {schedule['opencast_series_id']}"
    else:
        logger.info(f"No matching schedule for mid {mid}")
        events = ":calendar: No ZIP ingests scheduled."
        opencast_mapping = ""

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
                "text": f"*Zoom Meeting:* {mid} {opencast_mapping}"
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
                "text": f"*Zoom+ ingest request?* {on_demand}"
            },
            {
                "type": "mrkdwn",
                "text": f"*View in Zoom:* <{mgmt_url}|Recording Management>"
            },
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
                "fields": record_detail_fields
            }
        ]

        blocks.extend(record_data)

    if more_results:
        more_results_button = [
            {
                "type": "divider"
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "emoji": True,
                            "text": RESULTS_BUTTON_TEXT
                        },
                        "value": json.dumps({
                            "mid": mid,
                            "count": len(records)
                        })
                    }
                ]
            }
        ]
        blocks.extend(more_results_button)
    else:
        blocks.extend([
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": "No more results."
                    }
                },
            ]
        )

    return blocks


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
