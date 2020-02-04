import jwt
import time
import logging
import requests
import aws_lambda_logging
from functools import wraps
from os import getenv as env

LOG_LEVEL = env('DEBUG') and 'DEBUG' or 'INFO'
BOTO_LOG_LEVEL = env('BOTO_DEBUG') and 'DEBUG' or 'INFO'
ZOOM_API_BASE_URL = "https://api.zoom.us/v2/"


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


class ZoomAPIRequests:

    def __init__(self, zoom_api_key, zoom_api_secret):
        self.key = zoom_api_key
        self.secret = zoom_api_secret
        self.base_url = ZOOM_API_BASE_URL

    def __gen_token(self, seconds_valid=60):
        header = {"alg": "HS256", "typ": "JWT"}
        payload = {"iss": self.key, "exp": int(time.time() + seconds_valid)}
        return jwt.encode(payload, self.secret, headers=header)

    def get(self, path, seconds_valid=60, ignore_failure=False):
        if not path:
            raise Exception(
                "Call to ZoomAPIRequests.get "
                "missing required param 'path'"
            )

        url = "{}{}".format(self.base_url, path)
        headers = {
            "Authorization": "Bearer {}"
            .format(self.__gen_token(seconds_valid).decode())}
        r = requests.get(url, headers=headers)

        if not ignore_failure:
            r.raise_for_status()

        return r
