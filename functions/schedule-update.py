from os import getenv as env
import logging
from utils import setup_logging, GSheet


GSHEETS_DOC_ID = env("GSHEETS_DOC_ID")
GSHEETS_SHEET_NAMES = env("GSHEETS_SHEET_NAMES")

logger = logging.getLogger()


@setup_logging
def handler(event, context):
    """
    Load google sheet, parse, and import into dynamoDB.
    """
    logger.info(f"Get Google Sheet doc ID '{GSHEETS_DOC_ID}'")
    doc = GSheet(GSHEETS_DOC_ID)

    sheets = GSHEETS_SHEET_NAMES.split(",")
    for sheet in sheets:
        logger.info(f"Import data from '{sheet}' sheet")
        doc.import_to_dynamo(sheet)

    return {"statusCode": 200, "headers": {}, "body": "Success"}
