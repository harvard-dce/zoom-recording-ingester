from os import getenv as env
from urllib.parse import quote
from datetime import datetime
from pytz import timezone
import json
from utils import (
    TIMESTAMP_FORMAT,
    retrieve_schedule,
    schedule_days,
    schedule_match,
    PipelineStatus,
    ZoomStatus,
)

import logging

logger = logging.getLogger()


STACK_NAME = env("STACK_NAME")
PRETTY_TIMESTAMP_FORMAT = "%A, %B %d, %Y at %-I:%M%p"
SHORT_TIMESTAMP_FORMAT = "%m/%d/%y %-I:%M%p"
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
OC_CLUSTER_NAME = env("OC_CLUSTER_NAME")
# Slack places an upper limit of 50 UI blocks per message
# so we must limit the number of records per message
# Should be a multiple of RESULTS_PER_REQUEST
MAX_RECORDS_PER_MSG = 6
RESULTS_PER_REQUEST = 2


def slack_help_menu_blocks(cmd):
    return [
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


def slack_results_blocks(
    mid,
    meeting_status_data,
    newest_start_time=None,
    start_index=0,
    max_results=RESULTS_PER_REQUEST,
    interaction=False,
):
    header_blocks = []
    metadata_blocks = []
    ingest_detail_blocks = []
    footer_blocks = []

    schedule = retrieve_schedule(mid)
    logger.warning(schedule)
    events, opencast_mapping = format_schedule_details(schedule, mid)

    # Beginning of a search, include meeting metadata header
    if start_index == 0:
        if meeting_status_data:
            topic = meeting_status_data["topic"]
        elif schedule:
            topic = f"{schedule['course_code']} Zoom Meeting"
        else:
            topic = f"Zoom Meeting {format_mid(mid)}"

        header_blocks = slack_results_header(topic)
        metadata_blocks = slack_results_metadata(
            meeting_status_data,
            mid,
            schedule,
            opencast_mapping,
            events,
        )

    if not meeting_status_data:
        ingest_detail_blocks = [
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "No recent recordings found.",
                },
            },
        ]
        return header_blocks + metadata_blocks + ingest_detail_blocks

    recordings = sorted_filtered_recordings(
        meeting_status_data["recordings"],
        newest_start_time,
    )

    # limit amount and range of results
    more_results = len(recordings) > start_index + max_results
    recordings = recordings[start_index : start_index + max_results]

    for rec in recordings:
        ingest_detail_blocks += ingest_details(rec, schedule)

    footer_blocks = []
    if more_results:
        start_time = recordings[0]["start_time"]
        footer_blocks = more_results_button_blocks(
            recordings,
            mid,
            start_time,
            start_index,
        )
    elif interaction:
        footer_blocks = [
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "plain_text", "text": "End of results."},
            },
        ]

    blocks = (
        header_blocks + metadata_blocks + ingest_detail_blocks + footer_blocks
    )

    return blocks


"""
Slack results blocks
"""


def slack_results_header(topic):
    return [
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
        },
    ]


def slack_results_metadata(
    meeting_status,
    mid,
    schedule,
    opencast_mapping,
    events,
):
    blocks = []
    if meeting_status or schedule:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Zoom Meeting ID:* {format_mid(mid)} {opencast_mapping}",
                },
            }
        )
    blocks.append(
        {"type": "section", "text": {"type": "mrkdwn", "text": events}}
    )

    return blocks


def ingest_details(rec, schedule):
    pretty_start_time = formatted_local_time(rec["start_time"])
    match = schedule_match(schedule, local_time(rec["start_time"]))
    recordings_ready = True

    if len(rec["zip_ingests"]) == 1:
        recording_status_txt = recording_status_description(
            rec["zip_ingests"][0]
        )
        if recording_status_txt:
            recordings_ready = False

    if recordings_ready:
        ingest_details = ""
        # Sort ingests from most to least recent
        ingests = sorted(
            rec["zip_ingests"],
            key=lambda r: r["last_updated"],
            reverse=True,
        )

        for ingest in ingests:
            update_time = formatted_local_time(
                ingest["last_updated"],
                format=SHORT_TIMESTAMP_FORMAT,
            )
            on_demand = ingest["origin"] == "on_demand"

            if "ingest_request_time" in ingest:
                request_time = formatted_local_time(
                    ingest["ingest_request_time"]
                )
            else:
                logger.warning(
                    f"Ingest missing ingest_request_time field: {ingest}"
                )
                request_time = "[unknown]"

            if on_demand:
                ingest_details += f"*+Zoom Ingest on {request_time}*\n"
            else:
                ingest_details += f"*Automated Ingest on {request_time}*\n"

            ingest_details += f"> Status: {pipeline_status_description(ingest, on_demand, match)} (updated {update_time})\n"

            if "oc_series_id" in ingest and on_demand:
                ingest_details += f"> :arrow_right: Opencast Series: {ingest['oc_series_id']}\n"
    else:
        ingest_details = f"*Status* : {recording_status_txt}"

    blocks = [
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":movie_camera: *Recording on {pretty_start_time}*\n",
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": ingest_details},
        },
    ]

    if recordings_ready:
        # Add the link to the processed recordings
        mgmt_url = (
            "https://zoom.us/recording/management/detail?meeting_id="
            f"{quote(rec['recording_id'])}"
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{mgmt_url}|*View in Zoom*>",
                },
            }
        )

    return blocks


def more_results_button_blocks(recordings, mid, start_time, start_index):
    return [
        {"type": "divider"},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "emoji": True,
                        "text": f"Next {RESULTS_PER_REQUEST} Results",
                    },
                    "value": json.dumps(
                        {
                            "version": 1,
                            "mid": mid,
                            "newest_start_time": start_time,
                            "count": len(recordings),
                            "start_index": start_index,
                        }
                    ),
                }
            ],
        },
    ]


"""
Helpers
"""


def format_schedule_details(schedule, mid):
    if schedule:
        logger.info({"schedule": schedule})
        events = ""
        for i, event in enumerate(schedule["events"]):
            event_time = datetime.strptime(event["time"], "%H:%M").strftime(
                "%-I:%M%p"
            )
            events += f":clock3: {schedule['course_code']} {event['title']} "
            events += f"on {schedule_days[event['day']]} at {event_time}"

            if i + 1 < len(schedule["events"]):
                events += "\n"
        opencast_mapping = f":arrow_right: *Opencast Series:* {schedule['opencast_series_id']}"
    else:
        logger.info(f"No matching schedule for mid {mid}")
        events = "This Zoom meeting is not configured for ZIP ingests."
        opencast_mapping = ""

    return events, opencast_mapping


def sorted_filtered_recordings(recordings, newest_start_time=None):
    # filter out newer recordings to keep the search consistent
    # as the user requests more results
    if newest_start_time:
        recordings = list(
            filter(lambda r: r["start_time"] <= newest_start_time, recordings)
        )

    # sort by recording start time
    recordings = sorted(
        recordings,
        key=lambda r: r["start_time"],
        reverse=True,
    )

    return recordings


def local_time(ts):
    tz = timezone(LOCAL_TIME_ZONE)
    utc = datetime.strptime(ts, TIMESTAMP_FORMAT).replace(
        tzinfo=timezone("UTC")
    )
    return utc.astimezone(tz)


def formatted_local_time(ts, format=PRETTY_TIMESTAMP_FORMAT):
    return local_time(ts).strftime(format)


def format_mid(mid):
    s = str(mid)
    if len(s) < 11:
        return f"{s[:3]} {s[3:6]} {s[6:]}"
    else:
        return f"{s[:3]} {s[3:7]} {s[7:]}"


"""
Status descriptions
"""


def recording_status_description(ingest_details):
    status = ingest_details["status"]

    if status == ZoomStatus.RECORDING_IN_PROGRESS.name:
        status_msg = "Meeting in progress. Currently recording."
    elif status == ZoomStatus.RECORDING_PAUSED.name:
        status_msg = "Meeting in progress. Recording paused."
    elif status == ZoomStatus.RECORDING_STOPPED.name:
        status_msg = "Meeting in progress. Recording stopped."
    elif status == ZoomStatus.RECORDING_PROCESSING.name:
        status_msg = "Meeting finished. Recording files processing in Zoom."
    else:
        return None

    return status_msg


def pipeline_status_description(ingest_details, on_demand=False, match=False):
    status = ingest_details["status"]

    # Processing
    if status == PipelineStatus.ON_DEMAND_RECEIVED.name:
        status_msg = "Received +Zoom request."
    elif (
        status == PipelineStatus.WEBHOOK_RECEIVED.name
        or status == PipelineStatus.SENT_TO_DOWNLOADER.name
    ):
        if on_demand:
            status_msg = "ZIP received +Zoom request."
        elif match:
            status_msg = "ZIP received scheduled ingest."
        else:
            status_msg = "Ignored by ZIP."
    # Schedule match
    elif status == PipelineStatus.OC_SERIES_FOUND.name:
        status_msg = "Downloading files."
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
        status_msg += f" {ingest_details['reason']}"

    return status_msg
