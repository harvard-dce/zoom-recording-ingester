import json
import logging
import time
from collections import OrderedDict
from datetime import datetime, timezone
from functools import wraps
from os import getenv as env
from os.path import dirname, join

import boto3
import jwt
import requests
from dotenv import load_dotenv

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), "../../.env"))


def _env_flag(name):
    value = env(name)
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


LOG_LEVEL = "DEBUG" if _env_flag("DEBUG") else "INFO"
BOTO_LOG_LEVEL = "DEBUG" if _env_flag("BOTO_DEBUG") else "INFO"
ZOOM_API_BASE_URL = env("ZOOM_API_BASE_URL")
ZOOM_API_KEY = env("ZOOM_API_KEY")
ZOOM_API_SECRET = env("ZOOM_API_SECRET")
APIGEE_KEY = env("APIGEE_KEY")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PIPELINE_STATUS_TABLE = env("PIPELINE_STATUS_TABLE")
RECORDING_EVENTS_TABLE = env("RECORDING_EVENTS_TABLE")
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


class _AwsRequestIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.aws_request_id = None

    def set_request_id(self, aws_request_id):
        self.aws_request_id = aws_request_id

    def filter(self, record):
        # The lambda runtime's own LambdaLoggerFilter (added to the handler
        # before this one) stamps records with an always-fresh request id;
        # defer to it and only fill in when it's absent (local scripts,
        # tests) so we never overwrite a fresher value with a stale one.
        if not getattr(record, "aws_request_id", None):
            record.aws_request_id = self.aws_request_id
        return True


class _JsonLambdaFormatter(logging.Formatter):
    def format(self, record):
        record_dict = record.__dict__.copy()
        timestamp = (
            datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

        payload = {
            "timestamp": timestamp,
            "level": record.levelname,
            "location": f"{record.name}.{record.funcName}:{record.lineno}",
            "aws_request_id": record_dict.get("aws_request_id"),
            "message": record_dict.get("msg"),
        }

        if not isinstance(payload["message"], dict):
            payload["message"] = record.getMessage()
            try:
                payload["message"] = json.loads(payload["message"])
            except (TypeError, ValueError):
                pass

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # The trailing newline is the record terminator in the lambda
        # logging contract: without it, successive records are buffered
        # and flushed as one giant concatenated CloudWatch event (the
        # ZIP-104 garbling). The runtime's own formatters do the same.
        return json.dumps(payload, default=str) + "\n"


_REQUEST_ID_FILTER = _AwsRequestIdFilter()


def _setup_logging():
    root_logger = logging.getLogger()

    # The lambda runtime pre-installs a root handler wired to the runtime's
    # log sink. Reformat it rather than replacing it (see
    # docs/adr/0001-preserve-lambda-runtime-log-handler.md); only create a
    # handler when none exists (local scripts, tests).
    if not root_logger.handlers:
        root_logger.addHandler(logging.StreamHandler())

    for handler in root_logger.handlers:
        if not isinstance(handler.formatter, _JsonLambdaFormatter):
            handler.setFormatter(_JsonLambdaFormatter())
        if _REQUEST_ID_FILTER not in handler.filters:
            handler.addFilter(_REQUEST_ID_FILTER)

    root_logger.setLevel(LOG_LEVEL)

    boto_level = logging.DEBUG if BOTO_LOG_LEVEL == "DEBUG" else logging.INFO
    for logger_name in ("boto", "boto3", "botocore", "s3transfer", "urllib3"):
        logging.getLogger(logger_name).setLevel(boto_level)


# wrap the default getenv so we can enforce required vars
def getenv(param_name, required=True):
    val = env(param_name)
    if required and not val:
        raise Exception(f"Missing environment variable {param_name}")
    return val


def setup_logging(handler_func):
    @wraps(handler_func)
    def wrapped_func(event, context):
        _setup_logging()
        _REQUEST_ID_FILTER.set_request_id(
            getattr(context, "aws_request_id", None)
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

    actual_time = local_start_time
    logger.info(
        {"meeting creation time": actual_time, "course schedule": schedule}
    )
    zoom_day_code = list(schedule_days.keys())[actual_time.weekday()]

    # events is a list of {title, day, time} dictionaries
    for event in schedule["events"]:
        # match day
        if zoom_day_code != event["day"]:
            continue

        # match time
        scheduled_time = datetime.strptime(event["time"], "%H:%M")
        expected_time = actual_time.replace(
            hour=scheduled_time.hour,
            minute=scheduled_time.minute,
        )
        timedelta = abs(actual_time - expected_time).total_seconds()
        if timedelta < (BUFFER_MINUTES * 60):
            return event
        else:
            logger.info(
                f"Match for day {event['day']} but not within"
                f" {BUFFER_MINUTES} minutes of time {event['time']}"
            )

    return None
