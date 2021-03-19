import jwt
import time
import logging
import requests
import aws_lambda_logging
from functools import wraps
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname
from urllib.parse import urljoin

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), "../.env"))

LOG_LEVEL = env("DEBUG") and "DEBUG" or "INFO"
BOTO_LOG_LEVEL = env("BOTO_DEBUG") and "DEBUG" or "INFO"
ZOOM_API_BASE_URL = env("ZOOM_API_BASE_URL")
ZOOM_API_KEY = env("ZOOM_API_KEY")
ZOOM_API_SECRET = env("ZOOM_API_SECRET")
APIGEE_KEY = env("APIGEE_KEY")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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

        logger.debug("{} invoked!".format(context.function_name))
        logger.debug({"event": event, "context": context.__dict__})

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
        token = gen_token(
            ZOOM_API_KEY,
            ZOOM_API_SECRET,
            seconds_valid,
        ).decode()
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
