import logging
from enum import Enum, auto
from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname
from .common import TIMESTAMP_FORMAT


logger = logging.getLogger()

load_dotenv(join(dirname(__file__), "../.env"))

DATE_FORMAT = "%Y-%m-%d"
PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE", None)
SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600


# If PIPELINE_STATUS_TABLE is not set we assume the status table
# is not being used. (For example tasks.py imports utils but doesn't
# use the status table)
status_table = None
if PIPELINE_STATUS_TABLE:
    dynamodb = boto3.resource("dynamodb")
    status_table = dynamodb.Table(PIPELINE_STATUS_TABLE)


class InvalidStatusQuery(Exception):
    pass


class ZoomStatus(Enum):
    RECORDING_IN_PROGRESS = auto()
    RECORDING_PAUSED = auto()
    RECORDING_STOPPED = auto()
    RECORDING_PROCESSING = auto()


class PipelineStatus(Enum):
    ON_DEMAND_RECEIVED = auto()
    WEBHOOK_RECEIVED = auto()
    WEBHOOK_FAILED = auto()
    SENT_TO_DOWNLOADER = auto()
    OC_SERIES_FOUND = auto()
    IGNORED = auto()
    DOWNLOADER_FAILED = auto()
    SENT_TO_UPLOADER = auto()
    UPLOADER_RECEIVED = auto()
    SENT_TO_OPENCAST = auto()
    UPLOADER_FAILED = auto()


def ts_to_date_and_seconds(ts):
    date = ts.strftime(DATE_FORMAT)
    t = ts.time()
    seconds = int(
        timedelta(
            hours=t.hour,
            minutes=t.minute,
            seconds=t.second,
        ).total_seconds()
    )
    return date, seconds


def record_exists(zip_id):
    r = status_table.get_item(Key={"zip_id": zip_id})
    return "Item" in r


def set_pipeline_status(
    zip_id,
    state,
    origin=None,
    reason="",
    meeting_id=None,
    recording_id=None,
    recording_start_time=None,
    topic=None,
    oc_series_id=None,
):
    logger.info(f"Set pipeline status to {state.name} for id {zip_id}")

    utcnow = datetime.utcnow()
    today, seconds = ts_to_date_and_seconds(utcnow)
    try:
        update_expression = (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state"
        )
        expression_attribute_values = {
            ":update_date": today,
            ":update_time": int(seconds),
            ":expiration": int((utcnow + timedelta(days=7)).timestamp()),
            ":pipeline_state": state.name,
        }

        ingest_request_time = None
        if (
            state == PipelineStatus.WEBHOOK_RECEIVED
            or state == PipelineStatus.ON_DEMAND_RECEIVED
        ):
            ingest_request_time = utcnow.strftime(TIMESTAMP_FORMAT)

        optional_attributes = {
            "ingest_request_time": ingest_request_time,
            "meeting_id": int(meeting_id) if meeting_id else None,
            "recording_id": recording_id,
            "reason": reason,
            "origin": origin,
            "recording_start_time": recording_start_time,
            "topic": topic,
            "oc_series_id": oc_series_id,
        }
        for key, val in optional_attributes.items():
            if val is not None:
                update_expression += f", {key}=:{key}"
                expression_attribute_values[f":{key}"] = val

        condition_expression = None
        if not meeting_id or not origin:
            # When a recording enters the ZIP pipeline, for simplicity,
            # only the first status tracking update includes additional metadata
            # such as the meeting_id or origin. Subsequent status updates report
            # status using a unique zip_id.
            # Prevent adding records to dynamo status table for recordings
            # that haven't been tracked since the beginning of the pipeline and
            # therefore don't contain enough useful metadata. (This happens when
            # you start status tracking for the first time or make modifications
            # to the table that require it to be recreated.)
            condition_expression = (
                "attribute_exists(meeting_id) AND attribute_exists(origin)"
            )
        elif state == ZoomStatus.RECORDING_PROCESSING:
            expression_attribute_values.update(
                {
                    ":recording_in_progress": ZoomStatus.RECORDING_IN_PROGRESS.name,
                    ":recording_paused": ZoomStatus.RECORDING_PAUSED.name,
                    ":recording_stopped": ZoomStatus.RECORDING_STOPPED.name,
                }
            )

            # Enforce transition to recording processing can only happen from
            # recording in progress states
            condition_expression = (
                "pipeline_state = :recording_in_progress "
                "OR pipeline_state = :recording_paused "
                "OR pipeline_state = :recording_stopped "
            )
        elif state in ZoomStatus:
            expression_attribute_values[":recording_processing"] = (
                ZoomStatus.RECORDING_PROCESSING.name
            )

            # Enforce cannot transition back to a recording in progress state
            # from recording processing
            condition_expression = "pipeline_state <> :recording_processing"

        logger.info(
            {
                "dynamo update item": {
                    "zip_id": zip_id,
                    "update_expression": update_expression,
                    "expression_attribute_values": expression_attribute_values,
                    "condition_expression": condition_expression,
                }
            }
        )

        update_status_table(
            status_table,
            zip_id,
            update_expression,
            expression_attribute_values,
            condition_expression,
        )
    except ClientError as e:
        error = e.response["Error"]
        if error["Code"] == "ConditionalCheckFailedException":
            # Don't treat failed conditional update as an error
            logger.info(f"Conditional check ({condition_expression}) failed.")
            return
        logger.exception(f"{error['Code']}: {error['Message']}")
        raise
    except Exception as e:
        logger.exception(f"Something went wrong updating pipeline status: {e}")
        raise


# Isolated for unit testing
def update_status_table(
    status_table,
    zip_id,
    update_expression,
    expression_attribute_values,
    condition_expression=None,
):
    update_args = dict(
        Key={"zip_id": zip_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )
    if condition_expression:
        update_args["ConditionExpression"] = condition_expression

    status_table.update_item(**update_args)


def status_by_mid(mid):
    r = status_table.query(
        IndexName="mid_index",
        KeyConditionExpression=Key("meeting_id").eq(mid),
    )
    items = r["Items"]

    r = format_status_records(items)
    if len(r["meetings"]) == 0:
        return None
    else:
        return r["meetings"][0]


def status_by_seconds(request_seconds):
    now = datetime.utcnow()
    logger.info(
        f"Retrieving records updated within the last {request_seconds} seconds"
    )
    today, time_in_seconds = ts_to_date_and_seconds(now)

    if request_seconds > SECONDS_PER_HOUR:
        raise InvalidStatusQuery(
            f"Invalid number of seconds. Seconds must be <= {SECONDS_PER_HOUR}"
        )
    elif time_in_seconds < request_seconds:
        # handle case in which request spans two dates
        items = request_recent_items(status_table, today, 0)
        remaining = request_seconds - time_in_seconds
        ts = datetime.strptime(today, DATE_FORMAT)
        yesterday = (ts - timedelta(days=1)).strftime(DATE_FORMAT)
        items += request_recent_items(
            status_table,
            yesterday,
            SECONDS_PER_DAY - remaining,
        )
    else:
        items = request_recent_items(
            status_table, today, time_in_seconds - request_seconds
        )

    return format_status_records(items)


def request_recent_items(table, date, seconds):
    r = table.query(
        IndexName="time_index",
        KeyConditionExpression=(
            Key("update_date").eq(date) & Key("update_time").gte(seconds)
        ),
    )
    return r["Items"]


def format_status_records(items):
    meetings = {}
    for item in items:
        date = datetime.strptime(item["update_date"], DATE_FORMAT)
        ts = date + timedelta(seconds=int(item["update_time"]))
        last_updated = ts.strftime(TIMESTAMP_FORMAT)

        mid = int(item["meeting_id"])
        if mid not in meetings:
            meetings[mid] = {
                "meeting_id": mid,
                "topic": item["topic"],
                "recordings": {},
            }

        rec_id = item["recording_id"]
        if rec_id not in meetings[mid]["recordings"]:
            meetings[mid]["recordings"][rec_id] = {
                "recording_id": rec_id,
                "start_time": item["recording_start_time"],
                "zip_ingests": [],
            }

        zip_ingest = {
            "last_updated": last_updated,
            "status": item["pipeline_state"],
            "origin": item["origin"],
        }

        for optional_field in [
            "reason",
            "ingest_request_time",
            "oc_series_id",
        ]:
            if optional_field in item:
                zip_ingest[optional_field] = item[optional_field]

        meetings[mid]["recordings"][rec_id]["zip_ingests"].append(zip_ingest)

    results = {"meetings": list(meetings.values())}
    for mtg in results["meetings"]:
        mtg["recordings"] = list(mtg["recordings"].values())

    return results
