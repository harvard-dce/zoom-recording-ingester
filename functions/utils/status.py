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


class InvalidStatusQuery(Exception):
    pass


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


def zip_status_table():
    if PIPELINE_STATUS_TABLE:
        dynamodb = boto3.resource("dynamodb")
        return dynamodb.Table(PIPELINE_STATUS_TABLE)
    return None


def ts_to_date_and_seconds(ts):
    date = ts.strftime(DATE_FORMAT)
    t = ts.time()
    seconds = int(
        timedelta(
            hours=t.hour, minutes=t.minute, seconds=t.second
        ).total_seconds()
    )
    return date, seconds


def set_pipeline_status(
    correlation_id,
    state,
    origin=None,
    reason=None,
    meeting_id=None,
    recording_id=None,
    recording_start_time=None,
    topic=None,
    oc_series_id=None,
):
    logger.info(f"Set pipeline status to {state.name} for id {correlation_id}")

    utcnow = datetime.utcnow()
    today, seconds = ts_to_date_and_seconds(utcnow)
    try:
        status_table = zip_status_table()
        update_expression = (
            "set update_date=:d, "
            "update_time=:ts, "
            "expiration=:e, "
            "pipeline_state=:s"
        )
        expression_attribute_values = {
            ":d": today,
            ":ts": int(seconds),
            ":e": int((utcnow + timedelta(days=7)).timestamp()),
            ":s": state.name,
        }
        if meeting_id:
            update_expression += ", meeting_id=:m"
            expression_attribute_values[":m"] = meeting_id
        if recording_id:
            update_expression += ", recording_id=:u"
            expression_attribute_values[":u"] = recording_id
        if reason:
            update_expression += ", reason=:r"
            expression_attribute_values[":r"] = reason
        if origin:
            update_expression += ", origin=:o"
            expression_attribute_values[":o"] = origin
        if recording_start_time:
            update_expression += ", recording_start_time=:rst"
            expression_attribute_values[":rst"] = recording_start_time
        if topic:
            update_expression += ", topic=:t"
            expression_attribute_values[":t"] = topic
        if oc_series_id:
            update_expression += ", oc_series_id=:osi"
            expression_attribute_values[":osi"] = oc_series_id

        # When a recording enters the ZIP pipeline, for simplicity,
        # only the first status tracking update includes additional metadata
        # such as the meeting_id or origin. Subsequent status updates report
        # status using a unique correlation id.
        # Prevent adding records to dynamo status table for recordings
        # that haven't been tracked since the beginning of the pipeline and
        # therefore don't contain enough useful metadata. (This happens when
        # you start status tracking for the first time or make modifications
        # to the table that require it to be recreated.)
        if not meeting_id or not origin:
            condition_expression = (
                "attribute_exists(meeting_id) AND attribute_exists(origin)"
            )
            logger.debug(
                {
                    "dynamo update item": {
                        "correlation_id": correlation_id,
                        "update_expression": update_expression,
                        "expresssion_attribute_values": expression_attribute_values,
                        "condition_expression": condition_expression,
                    }
                }
            )
            status_table.update_item(
                Key={"correlation_id": correlation_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ConditionExpression=condition_expression,
            )
        else:
            logger.debug(
                {
                    "dynamo update item": {
                        "correlation_id": correlation_id,
                        "update_expression": update_expression,
                        "expresssion_attribute_values": expression_attribute_values,
                    }
                }
            )
            # New request
            status_table.update_item(
                Key={"correlation_id": correlation_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
            )

        # Conditionally update origin time
        condition_expression = "if_not_exists(origin_time)"
        logger.debug(
            {
                "dynamo update item": {
                    "correlation_id": correlation_id,
                    "update_expression": update_expression,
                    "expresssion_attribute_values": expression_attribute_values,
                    "condition_expression": condition_expression,
                }
            }
        )
        status_table.update_item(
            Key={"correlation_id": correlation_id},
            UpdateExpression="set origin_time=:ot",
            ExpressionAttributeValues={":ot": int(utcnow.timestamp())},
            ConditionExpression=condition_expression,
        )
    except ClientError as e:
        error = e.response["Error"]
        logger.exception(f"{error['Code']}: {error['Message']}")
    except Exception as e:
        logger.exception(f"Something went wrong updating pipeline status: {e}")


def status_by_mid(mid):
    status_table = zip_status_table()
    r = status_table.query(
        IndexName="mid_index",
        KeyConditionExpression=(Key("meeting_id").eq(mid)),
    )
    items = r["Items"]

    r = format_status_records(items)
    if len(r["meetings"]) == 0:
        return None
    else:
        return r["meetings"][0]


def status_by_seconds(request_seconds):
    status_table = zip_status_table()
    now = datetime.utcnow()
    logger.info(
        f"Retrieving records updated within the last {request_seconds} seconds"
    )
    today, time_in_seconds = ts_to_date_and_seconds(now)

    if request_seconds > SECONDS_PER_DAY:
        raise InvalidStatusQuery(
            f"Invalid number of seconds. Seconds must be <= {SECONDS_PER_DAY}"
        )
    elif time_in_seconds < request_seconds:
        # handle case in which request spans two dates
        items = request_recent_items(status_table, today, 0)
        remaining = time_in_seconds - request_seconds
        ts = today.strptime(DATE_FORMAT)
        yesterday = (ts - timedelta(days=1)).strftime(DATE_FORMAT)
        items += request_recent_items(
            status_table, yesterday, SECONDS_PER_DAY - remaining
        )
    else:
        items = request_recent_items(
            status_table, today, time_in_seconds - request_seconds
        )

    return format_status_records(items)


def request_recent_items(table, date, seconds):
    logger.warning(f"Request items since {seconds}")
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
        if "reason" in item:
            zip_ingest["reason"] = item["reason"]

        meetings[mid]["recordings"][rec_id]["zip_ingests"].append(zip_ingest)

    results = {"meetings": list(meetings.values())}
    for mtg in results["meetings"]:
        mtg["recordings"] = list(mtg["recordings"].values())

    return results
