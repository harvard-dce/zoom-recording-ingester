import json
import gzip
import boto3
from os import getenv as env
from io import BytesIO
import codecs

import logging
from common.common import setup_logging

logger = logging.getLogger()

SNS_TOPIC_ARN = env("SNS_TOPIC_ARN")

sns = boto3.client("sns")


@setup_logging
def handler(event, context):

    logger.info("processing subscribed events")

    # log event data comes gzipped & base64 decoded
    raw_data = event["awslogs"]["data"]
    decoded = codecs.decode(bytes(raw_data, "utf8"), "base64", "strict")
    decompressed = gzip.decompress(BytesIO(decoded).read())
    log_data = json.loads(decompressed.decode())
    logger.debug({"log data": log_data})

    log_group = log_data["logGroup"]
    log_stream = log_data["logStream"]
    log_events = log_data["logEvents"]

    for idx in range(len(log_events)):
        log_events[idx]["message"] = json.loads(log_events[idx]["message"])

    region = context.invoked_function_arn.split(":")[3]
    function_name = log_group.split("/")[-1]
    tracebacks = "\n\n".join(
        [
            "{}\n{}".format(x["id"], x.get("exception").strip())
            for x in log_events
            if "exception" in x
        ]
    )

    message = """
Error events in log group {}

Log Stream: https://console.aws.amazon.com/cloudwatch/home?region={}#logEventViewer:group={};stream={}

{}

Exception(s):

{}
"""
    message = message.format(
        log_group,
        region,
        log_group,
        log_stream,
        json.dumps(log_events, indent=2),
        tracebacks,
    )

    logger.debug({"sns message": message})

    logger.info("publishing alert to topic {}".format(SNS_TOPIC_ARN))
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="[ERROR] {}".format(function_name),
        Message=message,
    )
