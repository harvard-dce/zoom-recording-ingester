from utils import (
    setup_logging,
    status_by_mid,
    status_by_seconds,
    InvalidStatusQuery,
)
import json

import logging

logger = logging.getLogger()


def resp_400(msg):
    logger.error(f"http 400 response: {msg}")
    return {"statusCode": 400, "headers": {}, "body": msg}


@setup_logging
def handler(event, context):

    logger.info(event)

    meeting_id = None
    request_seconds = None

    query = event["queryStringParameters"]
    if query and "meeting_id" in query:
        meeting_id = query["meeting_id"]
    elif query and "seconds" in query:
        request_seconds = query["seconds"]
    else:
        return resp_400(
            "Missing identifer in query params. "
            "Must include one of 'meeting_id', 'seconds'"
        )

    if meeting_id:
        if meeting_id.isnumeric():
            meeting_id = int(meeting_id)
        else:
            return resp_400(f"Invalid meeting_id: {meeting_id}")

    if request_seconds:
        if request_seconds.isnumeric():
            request_seconds = int(request_seconds)
        else:
            return resp_400(f"Invalid request seconds: {request_seconds}")

    try:
        if meeting_id:
            records = status_by_mid(meeting_id)
        elif request_seconds:
            records = status_by_seconds(request_seconds)
    except InvalidStatusQuery as e:
        return resp_400(e)

    # sort by last updated
    records = sorted(records, key=lambda r: r["last_updated"], reverse=True)

    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"records": records}),
    }
