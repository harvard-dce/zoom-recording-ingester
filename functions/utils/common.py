import jwt
import time
import logging
import requests
import aws_lambda_logging
from functools import wraps
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname
import boto3
from collections import OrderedDict
from datetime import datetime

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), "../../.env"))

LOG_LEVEL = env("DEBUG") and "DEBUG" or "INFO"
BOTO_LOG_LEVEL = env("BOTO_DEBUG") and "DEBUG" or "INFO"
ZOOM_API_BASE_URL = env("ZOOM_API_BASE_URL")
ZOOM_API_KEY = env("ZOOM_API_KEY")
ZOOM_API_SECRET = env("ZOOM_API_SECRET")
APIGEE_KEY = env("APIGEE_KEY")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")
CLASS_SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")
# Recordings that happen within BUFFER_MINUTES a courses schedule
# start time will be captured
BUFFER_MINUTES = int(env("BUFFER_MINUTES", 30))


schedule_days = OrderedDict(
    [
        ("M", "Mondays"),
        ("T", "Tuesdays"),
        ("W", "Wednesdays"),
        ("R", "Thursdays"),
        ("F", "Fridays"),
        ("S", "Saturday"),
        ("U", "Sunday"),
    ]
)


class ZoomApiRequestError(Exception):
    pass


def setup_logging(handler_func):
    @wraps(handler_func)
    def wrapped_func(event, context):

        extra_info = {"aws_request_id": context.aws_request_id}
        aws_lambda_logging.setup(
            level=LOG_LEVEL,
            boto_level=BOTO_LOG_LEVEL,
            **extra_info,
        )

        logger = logging.getLogger()

        logger.debug(f"{context.function_name} invoked!")
        logger.debug({"event": event, "context": context.__dict__})

        try:
            retval = handler_func(event, context)
        except Exception:
            logger.exception("handler failed!")
            raise

        logger.debug(f"{context.function_name} complete!")
        return retval

    wrapped_func.__name__ = handler_func.__name__
    return wrapped_func


def gen_token(key, secret, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def zoom_api_request(
    endpoint,
    seconds_valid=60,
    ignore_failure=False,
    retries=3,
):
    if not endpoint:
        raise Exception("Call to zoom_api_request missing endpoint")

    if not APIGEE_KEY and not (ZOOM_API_KEY and ZOOM_API_SECRET):
        raise Exception(
            (
                "Missing api credentials. "
                "Must have APIGEE_KEY or ZOOM_API_KEY and ZOOM_API_SECRET"
            )
        )

    url = f"{ZOOM_API_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"

    if APIGEE_KEY:
        headers = {"X-Api-Key": APIGEE_KEY}
        logger.info(f"Apigee request to {url}")
    else:
        token = gen_token(ZOOM_API_KEY, ZOOM_API_SECRET, seconds_valid)
        headers = {"Authorization": f"Bearer {token}"}
        logger.info(f"Zoom api request to {url}")

    while True:
        try:
            r = requests.get(url, headers=headers)
            break
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ConnectTimeout,
        ) as e:
            if retries > 0:
                logger.warning(f"Connection Error: {e}")
                retries -= 1
            else:
                logger.error(f"Connection Error: {e}")
                raise ZoomApiRequestError(f"Error requesting {url}: {e}")

    if not ignore_failure:
        r.raise_for_status()

    return r


def retrieve_schedule(zoom_mid):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(CLASS_SCHEDULE_TABLE)

    r = table.get_item(Key={"zoom_series_id": str(zoom_mid)})

    if "Item" not in r:
        return None

    schedule = r["Item"]
    schedule["opencast_series_id"] = str(schedule["opencast_series_id"])

    return schedule


def schedule_match(schedule, local_start_time):
    if not schedule:
        return None

    zoom_time = local_start_time
    logger.info(
        {"meeting creation time": zoom_time, "course schedule": schedule}
    )
    zoom_day_code = list(schedule_days.keys())[zoom_time.weekday()]

    # events is a list of {title, day, time} dictionaries
    for event in schedule["events"]:
        # match day
        if zoom_day_code != event["day"]:
            continue

        # match time
        scheduled_time = datetime.strptime(event["time"], "%H:%M")
        timedelta = abs(
            zoom_time
            - zoom_time.replace(
                hour=scheduled_time.hour, minute=scheduled_time.minute
            )
        ).total_seconds()
        if timedelta < (BUFFER_MINUTES * 60):
            return event
        else:
            logger.info(
                f"Match for day {event['day']} but not within"
                f" {BUFFER_MINUTES} minutes of time {event['time']}"
            )

    return None
