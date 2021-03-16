from common.common import (
    setup_logging,
    TIMESTAMP_FORMAT,
    retrieve_schedule,
    schedule_days,
    schedule_match,
)
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
import traceback

import logging

logger = logging.getLogger()

SLACK_SIGNING_SECRET = env("SLACK_SIGNING_SECRET")
PRETTY_TIMESTAMP_FORMAT = "%B %d, %Y at %-I:%M%p"
STACK_NAME = env("STACK_NAME")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
ZIP_SLACK_CHANNEL = env("ZIP_SLACK_CHANNEL")
OC_CLUSTER_NAME = env("OC_CLUSTER_NAME")
# Slack places an upper limit of 50 UI blocks per message
# so we must limit the number of records per message
# Should be a multiple of RESULTS_PER_REQUEST
MAX_RECORDS_PER_MSG = 6
RESULTS_PER_REQUEST = 2
RESULTS_BUTTON_TEXT = f"Next {RESULTS_PER_REQUEST} Results"
ZOOM_MID_LENGTHS = [10, 11]


def resp_204(msg):
    logger.info(f"http 204 response: {msg}")
    return {"statusCode": 204, "headers": {}, "body": ""}  # 204 = no content


def resp_400(msg):
    logger.error(f"http 400 response: {msg}")
    return {"statusCode": 400, "headers": {}, "body": msg}


def slack_error_response(msg):
    logger.error(f"Slack error response: {msg}")
    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"response_type": "ephemeral", "text": msg}),
    }


def slack_help_response(cmd):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "These are the available ZIP commands:",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f">`{cmd} [Zoom meeting ID]` See the status of the latest ZIP ingests with the specified Zoom MID.",
            },
        },
    ]
    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"response_type": "ephemeral", "blocks": blocks}),
    }


@setup_logging
def handler(event, context):

    logger.info(event)
    try:
        if not valid_slack_request(event):
            return slack_error_response("Slack request verification failed.")
    except Exception:
        return slack_error_response("Error while validating Slack request.")

    query = parse_qs(event["body"])
    logger.info(query)

    if "command" in query:
        slash_command = query["command"][0]
        if "text" not in query:
            return slack_help_response(slash_command)
        text = query["text"][0]
        if text == "help":
            return slack_help_response(slash_command)

        # Accept mid that includes spaces, dashes or is bold
        # and has a valid number of digits
        mid_txt = text.replace("-", "").replace(" ", "").replace("*", "")
        if not mid_txt or not mid_txt.isnumeric() or len(mid_txt) not in ZOOM_MID_LENGTHS:
            return slack_error_response(
                "Please specify a valid Zoom meeting id."
            )

        meeting_id = int(mid_txt)
        response_url = query["response_url"][0]
        channel_name = query["channel_name"][0]
    else:
        slash_command = None
        payload = json.loads(query["payload"][0])
        logger.info({"interaction_payload": payload})

        response_url = payload["response_url"]
        channel_name = payload["channel"]["name"]
        action_text = payload["actions"][0]["text"]["text"]
        if action_text != RESULTS_BUTTON_TEXT:
            return resp_204(f"Ignore action: {action_text}")

        action_value = json.loads(payload["actions"][0]["value"])
        meeting_id = action_value["mid"]
        prev_msg = {
            "newest_start_time": action_value["newest_start_time"],
            "start_index": action_value["start_index"],
            "rec_count": action_value["count"],
            "blocks": payload["message"]["blocks"],
        }
        logger.info(f"Interaction value: {action_value}")

    meeting_status = status_by_mid(meeting_id)
    logger.info({"meeting_data": meeting_status})

    if channel_name != "directmessage" and channel_name != ZIP_SLACK_CHANNEL:
        return slack_error_response(
            f"The command {slash_command} can only be used in DM or in the #{ZIP_SLACK_CHANNEL} channel"
        )

    try:
        if slash_command:
            blocks = slack_response_blocks(meeting_id, meeting_status)
            # Response type must be "in_channel" for the "more results"
            # button to work. (Ephermeral messages cannot be updated.)
            response = {"response_type": "in_channel", "blocks": blocks}
            logger.info({"send_response": response})
            return {
                "statusCode": 200,
                "headers": {},
                "body": json.dumps(response)
            }
        else:
            add_new_message = prev_msg["rec_count"] % MAX_RECORDS_PER_MSG == 0
            if add_new_message:
                # Remove button and divider from previous message
                send_interaction_response(
                    response_url,
                    prev_msg["blocks"][:-2],
                    replace_original=True
                )
                # Put new results in new message
                blocks = slack_response_blocks(
                    meeting_id,
                    meeting_status,
                    search_identifier=prev_msg["newest_start_time"],
                    start_index=prev_msg["start_index"] + prev_msg["rec_count"],
                    max_results=RESULTS_PER_REQUEST,
                    interaction=True,
                )
                send_interaction_response(
                    response_url,
                    blocks,
                    replace_original=False
                )
            else:
                # Replace original message with more results
                blocks = slack_response_blocks(
                    meeting_id,
                    meeting_status,
                    search_identifier=prev_msg["newest_start_time"],
                    start_index=prev_msg["start_index"],
                    max_results=prev_msg["rec_count"] + RESULTS_PER_REQUEST,
                    interaction=True,
                )
                send_interaction_response(
                    response_url,
                    blocks,
                    replace_original=True
                )
            return resp_204("Interaction response successful")
    except Exception as e:
        track = traceback.format_exc()
        logger.error(f"Error generating slack response. {str(e)} {track}")
        if slash_command:
            return slack_error_response(
                f"We're sorry! There was an error when handling your request: {slash_command} {text}"
            )
        else:
            return resp_400("Error handling interaction.")


def local_time(ts):
    tz = timezone(LOCAL_TIME_ZONE)
    utc = datetime.strptime(ts, TIMESTAMP_FORMAT).replace(tzinfo=timezone("UTC"))
    return utc.astimezone(tz)


def pretty_local_time(ts):
    return local_time(ts).strftime(PRETTY_TIMESTAMP_FORMAT)


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
        bytes(SLACK_SIGNING_SECRET, "UTF-8"), bytes(basestring, "UTF-8"), hashlib.sha256
    )

    return hmac.compare_digest(f"{version}={str(h.hexdigest())}", signature)


def send_interaction_response(
    response_url,
    blocks,
    replace_original=True
):
    response = {
        "response_type": "in_channel",
        "replace_original": replace_original,
        "blocks": blocks,
    }
    logger.info(
        {
            f"Send interaction response": {
                "response_url": response_url,
                "response": response,
            }
        }
    )
    r = requests.post(response_url, json=response)
    logger.info(
        f"Response url returned status: {r.status_code} content: {str(r.content)}"
    )
    r.raise_for_status()


def slack_response_blocks(
    mid,
    meeting_status,
    search_identifier=None,
    start_index=0,
    max_results=RESULTS_PER_REQUEST,
    interaction=False,
):
    
    # Meeting metadata
    schedule = retrieve_schedule(mid)
    if schedule:
        logger.info({"schedule": schedule})
        events = ""
        for i, event in enumerate(schedule["events"]):
            event_time = datetime.strptime(
                event["time"], "%H:%M"
            ).strftime("%-I:%M%p")
            events += f":clock3: {schedule['course_code']} {event['title']} "
            events += f"on {schedule_days[event['day']]} at {event_time}"

            if i + 1 < len(schedule["events"]):
                events += "\n"
        opencast_mapping = (
            f":arrow_right: *Opencast Series:* {schedule['opencast_series_id']}"
        )
    else:
        logger.info(f"No matching schedule for mid {mid}")
        events = "This Zoom meeting is not configured for ZIP ingests."
        opencast_mapping = ""

    blocks = []
    # Beginning of a search, include meeting metadata header
    if start_index == 0:
        if meeting_status:
            topic = meeting_status["topic"]
        elif schedule:
            topic = f"{schedule['course_code']} Zoom Meeting"
        else:
            topic = f"Zoom Meeting {format_mid(mid)}"

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{topic}"},
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "plain_text",
                        "text": f"Source: {STACK_NAME}",
                    }
                ],
            }
        ]

        if meeting_status or schedule:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Zoom Meeting ID:* {format_mid(mid)} {opencast_mapping}",
                    }
                }
            )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": events}})

    if not meeting_status:
        blocks.extend([
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "No recent recordings found."
                }
            }
        ])
        return blocks

    recordings = meeting_status["recordings"]
    # filter out newer recordings
    # this is just to keep the search consistent as the user requests
    # more results
    if search_identifier:
        recordings = list(
            filter(
                lambda r: r["start_time"] <= search_identifier,
                recordings
            )
        )

    # sort by recording start time
    recordings = sorted(
        recordings,
        key=lambda r: r["start_time"],
        reverse=True
    )

    # limit amount and range of results
    more_results = len(recordings) > start_index + max_results
    recordings = recordings[start_index: start_index + max_results]

    for rec in recordings:
        pretty_start_time = pretty_local_time(rec["start_time"])
        rec_id = quote(rec["recording_id"])
        mgmt_url = f"https://zoom.us/recording/management/detail?meeting_id={rec_id}"
        match = schedule_match(schedule, local_time(rec["start_time"]))

        ingest_details_text = ""
        # Sort ingests from most to least recent
        ingests = sorted(
            rec["zip_ingests"],
            key=lambda r: r["last_updated"],
            reverse=True
        )
        for ingest in ingests:
            update_time = pretty_local_time(ingest["last_updated"])
            on_demand = ingest["origin"] == "on_demand"
            on_demand_text = "Yes" if on_demand else "No"

            ingest_details_text += (
                f"*Status:* {status_description(ingest, on_demand, match)}\n"
                f"*Updated:* {update_time}\n"
                f"*Zoom+ ingest?* {on_demand_text}\n\n"
            )

        blocks.extend([
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":movie_camera: *Recording on {pretty_start_time}*\n",
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ingest_details_text
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{mgmt_url}|*View in Zoom*>"
                }
            }
        ])

    if more_results:
        search_identifier = recordings[0]["start_time"]
        more_results_button = [
            {"type": "divider"},
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "emoji": True,
                            "text": RESULTS_BUTTON_TEXT,
                        },
                        "value": json.dumps(
                            {
                                "version": 1,
                                "mid": mid,
                                "newest_start_time": search_identifier,
                                "count": len(recordings),
                                "start_index": start_index,
                            }
                        ),
                    }
                ],
            },
        ]
        blocks.extend(more_results_button)
    elif interaction:
        blocks.extend(
            [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "plain_text", "text": "End of results."},
                },
            ]
        )

    return blocks


def format_mid(mid):
    s = str(mid)
    if len(s) < 11:
        return f"{s[:3]} {s[3:6]} {s[6:]}"
    else:
        return f"{s[:3]} {s[3:7]} {s[7:]}"


def status_description(ingest_details, on_demand, match):

    status = ingest_details["status"]

    # Processing
    if status == PipelineStatus.ON_DEMAND_RECEIVED.name:
        status_msg = "Received Zoom+ request."
    elif status == PipelineStatus.WEBHOOK_RECEIVED.name or status == PipelineStatus.SENT_TO_DOWNLOADER.name:
        if on_demand:
            status_msg = "ZIP received Zoom+ request."
        elif match:
            status_msg = "ZIP received scheduled ingest."
        else:
            status_msg = "Ignored by ZIP."
    # Schedule match
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
        status_msg = ":exclamation: Error while receiving ZIP request."
    elif status == PipelineStatus.DOWNLOADER_FAILED.name:
        status_msg = ":exclamation: Failed to download files."
    elif status == PipelineStatus.UPLOADER_FAILED.name:
        status_msg = f":exclamation: Failed to ingest to Opencast cluster {OC_CLUSTER_NAME}."
    else:
        status_msg = status

    if "reason" in ingest_details:
        status_msg += f" {ingest_details['reason']}."

    return status_msg
