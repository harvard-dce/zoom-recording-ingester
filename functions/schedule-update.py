import os.path
import csv
from os import getenv as env
import logging
from common import setup_logging

logger = logging.getLogger()


@setup_logging
def handler(event, context):

    logger.info(event)

    return {
        "statusCode": 200,
        "headers": {},
        "body": "Success"
    }
