from common import setup_logging, ts_to_date_and_seconds, DATE_FORMAT, TIMESTAMP_FORMAT
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from os import getenv as env
from urllib.parse import unquote
from datetime import datetime, timedelta

import logging

logger = logging.getLogger()

PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")
SECONDS_PER_DAY = 86400


def resp_400(msg):
    logger.error("http 400 response: {}".format(msg))
    return {"statusCode": 400, "headers": {}, "body": msg}


@setup_logging
def handler(event, context):

    logger.info(event)

    query = event["queryStringParameters"]

    dynamodb = boto3.resource("dynamodb")
    logger.info(PIPELINE_STATUS_TABLE)
    table = dynamodb.Table(PIPELINE_STATUS_TABLE)

    if "meeting_id" in query:
        r = table.scan(FilterExpression=Attr("meeting_id").eq(int(query["meeting_id"])))
        items = r["Items"]
    elif "recording_id" in query:
        value = unquote(query["recording_id"])
        r = table.scan(FilterExpression=Attr("recording_id").eq(value))
        items = r["Items"]
    elif "seconds" in query:
        threshold = int(query["seconds"])
        now = datetime.utcnow()
        logger.info(f"Retrieving records updated within the last {threshold} seconds")
        today, seconds = ts_to_date_and_seconds(now)

        if seconds > SECONDS_PER_DAY:
            return resp_400(
                f"Invalid number of seconds. Seconds must be <= {SECONDS_PER_DAY}"
            )
        elif seconds < threshold:
            items = request_recent_items(table, today, 0)
            remaining = threshold - seconds
            ts = today.strptime(DATE_FORMAT)
            yesterday = (ts - timedelta(days=1)).strftime(DATE_FORMAT)
            items += request_recent_items(table, yesterday, SECONDS_PER_DAY - remaining)
        else:
            items = request_recent_items(table, today, seconds - threshold)
    else:
        return resp_400(
            "Missing identifer in query params. "
            "Must include one of 'meeting_id', 'recording_id', or 'seconds'"
        )

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
                "recording_start_time": item["recording_start_time"]
            }
        }
        if "reason" in item:
            record["status_reason"] = item["reason"]

        records.append(record)

    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"results": records})
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
