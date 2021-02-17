from common import setup_logging, current_day_and_time
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from os import getenv as env
from urllib.parse import unquote
from datetime import datetime, timedelta

import logging
logger = logging.getLogger()

PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")


def resp_400(msg):
    logger.error("http 400 response: {}".format(msg))
    return {
        "statusCode": 400,
        "headers": {},
        "body": msg
    }


@setup_logging
def handler(event, context):

    logger.info(event)

    query = event["queryStringParameters"]

    dynamodb = boto3.resource("dynamodb")
    logger.info(PIPELINE_STATUS_TABLE)
    table = dynamodb.Table(PIPELINE_STATUS_TABLE)

    if "meeting_id" in query:
        r = table.scan(FilterExpression=Attr("meeting_id").eq(int(query["meeting_id"])))
    elif "recording_id" in query:
        value = unquote(query["recording_id"])
        r = table.scan(FilterExpression=Attr("recording_id").eq(value))
    elif "seconds" in query:
        value = int(query["seconds"])
        today, seconds = current_day_and_time()
        r = table.query(
            IndexName="time_index",
            KeyConditionExpression=Key("update_date").eq(today) & Key("update_time").gte(int(seconds) - value),
        )
    else:
        return resp_400(
            "Missing identifer in query params. "
            "Must include one of 'meeting_id', 'recording_id', or 'seconds'"
        )

    logger.info(r)

    items = r["Items"]
    for item in items:
        item["meeting_id"] = str(item["meeting_id"])
        item["update_time"] = str(item["update_time"])
        item.pop("expiration")

    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"results": items})
    }
