import jwt
import time
import logging
import requests
import aws_lambda_logging
from functools import wraps
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname
from enum import Enum, auto
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from collections import OrderedDict

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), '../.env'))

LOG_LEVEL = env('DEBUG') and 'DEBUG' or 'INFO'
BOTO_LOG_LEVEL = env('BOTO_DEBUG') and 'DEBUG' or 'INFO'
ZOOM_API_BASE_URL = env("ZOOM_API_BASE_URL")
ZOOM_API_KEY = env("ZOOM_API_KEY")
ZOOM_API_SECRET = env("ZOOM_API_SECRET")
APIGEE_KEY = env("APIGEE_KEY")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT = "%Y-%m-%d"
PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")
CLASS_SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")


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


schedule_days = OrderedDict([
            ("M", "Mondays"),
            ("T", "Tuesdays"),
            ("W", "Wednesdays"),
            ("R", "Thursdays"),
            ("F", "Fridays"),
            ("S", "Saturday"),
            ("U", "Sunday")
        ])


class ZoomApiRequestError(Exception):
    pass


def setup_logging(handler_func):

    @wraps(handler_func)
    def wrapped_func(event, context):

        extra_info = {'aws_request_id': context.aws_request_id}
        aws_lambda_logging.setup(
            level=LOG_LEVEL,
            boto_level=BOTO_LOG_LEVEL,
            **extra_info
        )

        logger = logging.getLogger()

        logger.debug("{} invoked!".format(context.function_name))
        logger.debug({
            'event': event,
            'context': context.__dict__
        })

        try:
            retval = handler_func(event, context)
        except Exception:
            logger.exception("handler failed!")
            raise

        logger.debug("{} complete!".format(context.function_name))
        return retval

    wrapped_func.__name__ = handler_func.__name__
    return wrapped_func


def gen_token(key, secret, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def zoom_api_request(endpoint, seconds_valid=60, ignore_failure=False, retries=3):
    if not endpoint:
        raise Exception("Call to zoom_api_request missing endpoint")

    if not APIGEE_KEY and not (ZOOM_API_KEY and ZOOM_API_SECRET):
        raise Exception(("Missing api credentials. "
            "Must have APIGEE_KEY or ZOOM_API_KEY and ZOOM_API_SECRET"))

    url = f"{ZOOM_API_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"

    if APIGEE_KEY:
        headers = {
            "X-Api-Key": APIGEE_KEY
        }
        logger.info(f"Apigee request to {url}")
    else:
        token = gen_token(ZOOM_API_KEY, ZOOM_API_SECRET, seconds_valid).decode()
        headers = {
            "Authorization": f"Bearer {token}"
        }
        logger.info(f"Zoom api request to {url}")

    while True:
        try:
            r = requests.get(url, headers=headers)
            break
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ConnectTimeout) as e:
            if retries > 0:
                logger.warning("Connection Error: {}".format(e))
                retries -= 1
            else:
                logger.error("Connection Error: {}".format(e))
                raise ZoomApiRequestError(
                    "Error requesting {}: {}".format(url, e)
                )

    if not ignore_failure:
        r.raise_for_status()

    return r


def ts_to_date_and_seconds(ts):
    date = ts.strftime(DATE_FORMAT)
    t = ts.time()
    seconds = int(timedelta(
        hours=t.hour, minutes=t.minute, seconds=t.second
    ).total_seconds())
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
    oc_series_id=None
):
    today, seconds = ts_to_date_and_seconds(datetime.utcnow())
    try:
        update_expression = "set update_date=:d, update_time=:ts, expiration=:e, pipeline_state=:s"
        expression_attribute_values = {
            ":d": today,
            ":ts": int(seconds),
            ":e": int((datetime.now() + timedelta(days=7)).timestamp()),
            ":s": state.name
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

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(PIPELINE_STATUS_TABLE)
        table.update_item(
            Key={"correlation_id": correlation_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        logger.info(
            f"Set pipeline status to {state.value} for id {correlation_id}"
        )
    except ClientError as e:
        error = e.response["Error"]
        logger.exception(f"{error['Code']}: {error['Message']}")
    except Exception as e:
        logger.exception(f"Something went wrong updating pipeline status: {e}")


def retrieve_schedule(zoom_mid):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(CLASS_SCHEDULE_TABLE)

    r = table.get_item(
        Key={"zoom_series_id": str(zoom_mid)}
    )

    if "Item" not in r:
        return None

    schedule = r["Item"]
    schedule["opencast_series_id"] = str(schedule["opencast_series_id"])

    return schedule
