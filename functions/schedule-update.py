from os import getenv as env
import logging
from utils import setup_logging, GSheet


GSHEETS_DOC_ID = env("GSHEETS_DOC_ID")
GSHEETS_SHEET_NAME = env("GSHEETS_SHEET_NAME")

logger = logging.getLogger()


@setup_logging
def handler(event, context):
    """
    Load google sheet, parse, and import into dynamoDB.
    """
    logger.info(f"Get Google Sheet doc ID '{GSHEETS_DOC_ID}'")
    sheet = GSheet(GSHEETS_DOC_ID)
    logger.info(f"Import data from '{GSHEETS_SHEET_NAME}' sheet")
    sheet.import_to_dynamo(GSHEETS_SHEET_NAME)

    return {"statusCode": 200, "headers": {}, "body": "Success"}
