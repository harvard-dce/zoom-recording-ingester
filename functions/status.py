from common import setup_logging
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from os import getenv as env

import logging
logger = logging.getLogger()

PIPELINE_STATUS_TABLE = env("ON_DEMAND_STATUS_TABLE")


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
        result = format_results(r)
    elif "meeting_id" in query:
        r = table.scan(FilterExpression=Attr("meeting_id").eq(int(query["meeting_id"])))
        result = format_results(r)
    elif "recording_id" in query:
        r = table.scan(FilterExpression=Attr("recording_id").eq(query["recording_id"]))
        result = format_results(r)
    else:
        return resp_400(
            "Missing identifer in query params. "
            "Must include one of 'request_id', 'meeting_id', or 'recording_id'"
        )

    logger.info(r)

    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps(result)
    }


def format_results(r):
    items = r["Items"]

    results = {}

    for item in items:
        mid = str(item["meeting_id"])
        rid = item["recording_id"]
        if mid not in results:
            results[mid] = {"recordings": {}}

        if rid not in results[mid]["recordings"]:
            results[mid]["recordings"][rid] = []
        results[mid]["recordings"][rid].append({
            "status": item["pipeline_state"],
            "update_time": item["last_update"],
            "reason": item["reason"]
        })
    return results


