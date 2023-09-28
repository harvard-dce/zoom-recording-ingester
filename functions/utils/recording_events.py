import logging
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname
from .common import TIMESTAMP_FORMAT
from pytz import utc
from itertools import chain

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), "../.env"))

RECORDING_EVENTS_TABLE = env("RECORDING_EVENTS_TABLE", None)

START_EVENTS = ["recording.started", "recording.resumed"]
END_EVENTS = ["recording.paused", "recording.stopped"]
RECORDING_EVENTS = START_EVENTS + END_EVENTS

# This was copied from status.py:
# If RECORDING_EVENTS_TABLE is not set we assume the events table
# is not being used. (For example tasks.py imports utils but doesn't
# use the recording times table)
recording_events_table = None
if RECORDING_EVENTS_TABLE:
    dynamodb = boto3.resource("dynamodb")
    recording_events_table = dynamodb.Table(RECORDING_EVENTS_TABLE)


def get_recording_events(zoom_uuid):
    """
    Gets recording events from the database by zoom uuid.
    """
    r = recording_events_table.get_item(Key={"zoom_uuid": zoom_uuid})
    logger.debug(f"Recording events from db: ${r}")
    return r["Item"] if "Item" in r else None


def set_recording_events(
    zoom_uuid,
    zoom_event,
    zoom_event_timestamp,  # in ms
):
    """
    Records a recording event in the database. Events are stored by zoom uuid in a list of
    {
        "event": "ZOOM_EVENT",  # recording.started, paused, resumed, stopped
        "timestamp": "TIMESTAMP_IN_SECS",
    }
    """
    # If not one of desired events or no timestamp, ignore
    if zoom_event not in RECORDING_EVENTS or not zoom_event_timestamp:
        return

    # If this is an event for recording started/paused/resumed/stopped,
    # record the event time in the table in a list.
    formatted_local_time = datetime.strftime(
        datetime.fromtimestamp(int(zoom_event_timestamp / 1000)),
        "%m/%d/%Y %H:%M:%S",
    )
    logger.info(
        f"Record event for '{zoom_uuid}': {zoom_event} at {formatted_local_time}"
    )

    try:
        new_recording_event_list = [
            {
                "event": zoom_event,
                "timestamp": int(zoom_event_timestamp / 1000),  # in secs
            }
        ]
        utcnow = datetime.utcnow()
        update_expression = (
            "SET "
            "expiration = :expiration, "
            "recording_event_times = list_append("
            "if_not_exists(recording_event_times, :empty_list), "
            ":new_recording_event"
            ")"
        )
        # Expiration 120 days, which should cover at least a semester
        expression_attribute_values = {
            ":expiration": int((utcnow + timedelta(days=120)).timestamp()),
            ":new_recording_event": new_recording_event_list,
            ":empty_list": [],
        }

        logger.info(
            {
                "update recording events": {
                    "zoom_uuid": zoom_uuid,
                    "update_expression": update_expression,
                    "expression_attribute_values": expression_attribute_values,
                }
            }
        )

        update_recording_events_table(
            recording_events_table,
            zoom_uuid,
            update_expression,
            expression_attribute_values,
        )
    # Will not raise any exceptions, only log
    except ClientError as e:
        error = e.response["Error"]
        logger.exception(f"{error['Code']}: {error['Message']}")
    except Exception as e:
        logger.exception(
            f"Something went wrong updating recording event status: {e}"
        )


# Isolated for unit testing
def update_recording_events_table(
    recording_events_table,
    zoom_uuid,
    update_expression,
    expression_attribute_values,
):
    """
    Updates the dynamodb table.
    """
    update_args = dict(
        Key={"zoom_uuid": zoom_uuid},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )

    recording_events_table.update_item(**update_args)


def get_recording_segments(
    zoom_uuid,
    start_stop_segments,
):
    """
    This method returns a string with segments start and end to be passed to
    Opencast as an ingest workflow configuration. The string has the format:
    "<START1_UTC>_<END1_UTC>,<START2_UTC>_<END2_UTC>,etc"
    e.g. "2023-09-13T16:29:32Z_2023-09-13T18:07:26Z,2023-09-13T18:17:05Z_2023-09-13T19:00:03Z"
    Segments are found based on start/pause/resume/stop events and are sorted.

    Parameters:
        zoom_uuid: the recording uuid
        start_stop_times: this comes from each recording (zoom_uploader);
            one segment for each start/stop. The first segment
            may have already been discarded if its duration is too
            short.
            Format: Hash of:
                segment_number => {
                    "start": "2023-09-13T15:44:21Z", # utc datetime
                    "stop": "2023-09-13T15:45:53Z",
                }
    """

    # Transform start_stop_times into list with the same format as the recorded events
    all_events_list = list(
        chain.from_iterable(
            [
                [
                    {
                        "event": "recording.started",
                        "timestamp": from_utc_to_timestamp(x["start"]),
                    },
                    {
                        "event": "recording.stopped",
                        "timestamp": from_utc_to_timestamp(x["stop"]),
                    },
                ]
                for x in start_stop_segments.values()
            ]
        )
    )
    # Get start of first segment (because we may have discarded the first
    # segment if it was too short)
    first_start_ts = min(
        [
            x["timestamp"]
            for x in all_events_list
            if x["event"] == "recording.started"
        ]
    )

    segments = []
    item = None
    try:
        item = get_recording_events(zoom_uuid)
    except Exception:
        logger.exception(
            f"Could not get recording events for zoom uuid '{zoom_uuid}'."
        )

    # Events for this item? Add them to the list of events. We may have
    # duplicated recording.start, recording.end, but that's ok.
    # Note that the notification timestamp may be after the recording start/stop
    # was clicked. Since we consider also the start/stop times passed with the
    # recordings and choose the first in START_EVENTS and STOP_EVENTS, this should
    # be OK.
    if item and item["recording_event_times"]:
        all_events_list.extend(item["recording_event_times"])

    # Sort events by timestamp
    all_events_list = sorted(all_events_list, key=lambda x: x["timestamp"])

    # Note that if we fail to get all notifications, we may get weird
    # segments. We can't do anything about this...
    start = None
    for event in all_events_list:
        # Ignore events before the first segment that will be ingested
        if event["timestamp"] < first_start_ts:
            continue
        if event["event"] in START_EVENTS and not start:
            start = event["timestamp"]
        elif event["event"] in END_EVENTS and start:
            segments.append(
                f"{from_timestamp_to_utc(start)}_{from_timestamp_to_utc(event['timestamp'])}"
            )
            start = None

    return ",".join(segments) if segments else ""


def from_utc_to_timestamp(date_str):
    return int(
        utc.localize(datetime.strptime(date_str, TIMESTAMP_FORMAT)).timestamp()
    )


def from_timestamp_to_utc(ts):
    return datetime.strftime(datetime.utcfromtimestamp(ts), TIMESTAMP_FORMAT)
