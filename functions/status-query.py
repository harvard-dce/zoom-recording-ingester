from common import setup_logging
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from os import getenv as env
from urllib.parse import unquote

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

    if "request_id" in query:
        r = table.query(KeyConditionExpression=Key("request_id").eq(query["request_id"]))
    elif "meeting_id" in query:
        r = table.scan(FilterExpression=Attr("meeting_id").eq(int(query["meeting_id"])))
    elif "recording_id" in query:
        value = unquote(query["recording_id"])
        r = table.scan(FilterExpression=Attr("recording_id").eq(value))
    else:
        return resp_400(
            "Missing identifer in query params. "
            "Must include one of 'request_id', 'meeting_id', or 'recording_id'"
        )

    logger.info(r)

    items = r["Items"]
    for item in items:
        item["meeting_id"] = str(item["meeting_id"])
        item.pop("expiration")

    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"results": items})
    }
