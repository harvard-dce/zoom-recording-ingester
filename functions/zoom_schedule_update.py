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

    sheets = [x.strip() for x in GSHEETS_SHEET_NAMES.split(",") if x]
    for sheet in sheets:
        logger.info(f"Import data from '{sheet}' sheet")
        doc.import_to_dynamo(sheet)

    return {"statusCode": 200, "headers": {}, "body": "Success"}


if __name__ == "__main__":

    class FakeContextObj:
        def __init__(self):
            self.aws_request_id = "foo"
            self.function_name = __file__

    handler({}, FakeContextObj())
