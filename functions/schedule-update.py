import pickle
import os.path
import csv
from os import getenv as env
from common import setup_logging, gsheets_token, GSHEETS_SCOPE
from googleapiclient.discovery import build

# If modifying scopes or the spreadsheet id,
# a new token.pickle file must be generated

GSHEETS_DOC_ID = env("GSHEETS_DOC_ID")
GSHEETS_SHEET_NAME = env("GSHEETS_SHEET_NAME")

logger = logging.getLogger()


@setup_logging
def handler(event, context):
    """
    TODO: Explain the purpose of this function.
    """
    sheet = Spreadsheet(GSHEETS_DOC_ID, [GSHEETS_SCOPE])
    sheet.download_sheet_to_csv(GSHEETS_SHEET_NAME)


class Spreadsheet:
    def __init__(self, spreadsheet_id, scopes):
        self.spreadsheet_id = spreadsheet_id
        self.credentials = gsheets_token()

    @property
    def service(self):
        return build(
            "sheets", "v4", credentials=self.credentials, cache_discovery=False
        )

    def download_sheet_to_csv(self, sheet_name):
        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=sheet_name)
            .execute()
        )

        print(result.get("values"))
        output_file = f"gsheets/{sheet_name.replace('/', '-')}.csv"

        with open(output_file, "w") as f:
            writer = csv.writer(f)
            writer.writerows(result.get("values"))

        f.close()

        logger.debug(f"Successfully downloaded {sheet_name}.csv")
        return output_file
