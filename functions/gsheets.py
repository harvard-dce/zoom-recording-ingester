import sys
import json
import logging
from os import getenv as env
from os.path import join, dirname, exists
from dotenv import load_dotenv
from urllib.parse import urlparse
import csv
import itertools
from datetime import datetime, timedelta
# boto imports
import boto3
from botocore.exceptions import ClientError
# google sheets imports
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), '../.env'))


class GSheetsAuth():
    def __init__(self, in_lambda=False):
        self.ssm = boto3.client("ssm")
        self.ssm_path = f"/zoom-ingester/common/gapi-credentials"
        # if in lambda, must use tmp folder
        self.default_filename = "/tmp/service_account.json"
        self.creds = None

    @property
    def credentials(self):
        return self.creds

    def load_from_ssm(self):
        """
        Load credentials file from SSM parameter.
        """
        try:
            r = self.ssm.get_parameter(
                Name=self.ssm_path,
                WithDecryption=True,
            )
            data = r["Parameter"]["Value"]
            with open(self.default_filename, "w") as f:
                f.write(data)
            self.creds = service_account.Credentials.from_service_account_file(
                self.default_filename
            )
        except self.ssm.exceptions.ParameterNotFound:
            logger.error(f"SSM Parameter {self.ssm_path} not found")
            self.creds = None

    def save_to_ssm(self, filename):
        """
        Save credentials file to SSM parameter.
        """
        if not filename:
            filename = self.default_filename
        if not exists(filename):
            raise Exception(f"Credentials file {filename} not found")

        data = ""
        with open(filename, "r") as f:
            data = json.load(f)

        r = self.ssm.put_parameter(
            Name=self.ssm_path,
            Description="Service account creds for gsheets authorization",
            Value=json.dumps(data),
            Type="SecureString",
            Overwrite=True,
        )
        logger.info(f"Saved token version {r['Version']}")

    def delete_from_ssm(self):
        """
        Delete credentials from SSM.
        """
        r = self.ssm.delete_parameter(Name=self.ssm_path)
        if (r["ResponseMetadata"]["HTTPStatusCode"] == 200):
            logger.info(f"Successfully destroyed ssm parameter {self.ssm_path}")
        else:
            logger.info(f"Failed to destroy ssm parameter {self.ssm_path}")


class GSheet:
    def __init__(self, spreadsheet_id, in_lambda=False):
        self.spreadsheet_id = spreadsheet_id
        auth = GSheetsAuth()
        auth.load_from_ssm()
        self.creds = auth.credentials

    @property
    def service(self):
        return build(
            "sheets", "v4", credentials=self.creds, cache_discovery=False
        )

    def _download_csv(self, sheet_name):
        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=sheet_name)
            .execute()
        )

        # /tmp is the only directory you can write to in a lambda function
        file_path = f"/tmp/{sheet_name.replace('/', '-')}.csv"

        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(result.get("values"))

        f.close()

        logger.info(f"Successfully downloaded {sheet_name}.csv")
        return file_path

    def import_to_dynamo(self, sheet_name):
        file_path = self._download_csv(sheet_name)

        schedule_csv_to_dynamo(SCHEDULE_TABLE, file_path)


def schedule_json_to_dynamo(dynamo_table_name, json_file=None, schedule_data=None):
    if json_file is not None:
        with open(json_file, "r") as file:
            try:
                schedule_data = json.load(file)
            except Exception as e:
                logger.error("Unable to load {}: {}".format(json_file, str(e)))
                return
    elif schedule_data is None:
        raise Exception("{} called with no json_file or schedule_data args".format(
            sys._getframe().f_code.co_name
        ))

    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(dynamo_table_name)

        for item in schedule_data.values():
            table.put_item(Item=item)
        logger.info(f"Loaded schedule into dynamo table {dynamo_table_name}")
    except ClientError as e:
        error = e.response['Error']
        raise Exception("{}: {}".format(error['Code'], error['Message']))


def schedule_csv_to_dynamo(dynamo_table_name, filepath):
    valid_days = ["M", "T", "W", "R", "F", "S", "U"]

    # make it so we can use lower-case keys in our row dicts;
    # there are lots of ways this spreadsheet data import could go wrong and
    # this is only one, but we do what we can.
    def lower_case_first_line(iter):
        header = next(iter).lower()
        return itertools.chain([header], iter)

    with open(filepath, "r") as f:
        reader = csv.DictReader(lower_case_first_line(f))
        rows = list(reader)

    required_columns = [
        "course code",
        "day",
        "start",
        "meeting id with password",
        "oc series"
    ]
    for col in required_columns:
        if col not in rows[0]:
            raise Exception(f"Missing required field \"{col}\"")

    schedule_data = {}
    for row in rows:

        meeting_id_with_pwd = row["meeting id with password"]
        if not meeting_id_with_pwd:
            logger.warning(f"{row['course code']}: \tMissing zoom link")
            continue

        try:
            zoom_link = urlparse(row["meeting id with password"].strip())
            assert zoom_link.scheme.startswith("https")
        except AssertionError:
            logger.error(f"{row['course code']}: \tInvalid zoom link")

        zoom_series_id = zoom_link.path.split("/")[-1]
        schedule_data.setdefault(zoom_series_id, {})
        schedule_data[zoom_series_id]["zoom_series_id"] = zoom_series_id

        opencast_series_id = urlparse(row["oc series"]) \
            .fragment.replace("/", "")
        if not opencast_series_id:
            logger.warning(f"{row['course code']}: \tMissing oc series")
        schedule_data[zoom_series_id]["opencast_series_id"] = opencast_series_id

        subject = "{} - {}".format(row["course code"], row["type"])
        schedule_data[zoom_series_id]["opencast_subject"] = subject

        schedule_data[zoom_series_id].setdefault("Days", set())

        days_value = row["day"].strip()

        # value might look like "MW", "TR" or "MWF"
        if len(days_value) > 1 and " " not in days_value:
            days = list(days_value)
        else:
            split_by = " "
            if "," in days_value:
                split_by = ","
            days = [day.strip() for day in days_value.split(split_by)]

        for day in days:
            if day not in valid_days:
                logger.warning(
                    f"{row['course code']}: \tbad day value \"{day}\""
                )
                continue
            schedule_data[zoom_series_id]["Days"].add(day)

        schedule_data[zoom_series_id].setdefault("Time", set())
        time_object = datetime.strptime(row["start"], "%H:%M")
        schedule_data[zoom_series_id]["Time"].update([
            datetime.strftime(time_object, "%H:%M")
        ])

    for item in schedule_data.values():
        # sort days for easier testing
        item["Days"] = sorted(
            list(item["Days"]), key=lambda x: valid_days.index(x[0])
        )
        item["Time"] = list(item["Time"])

    logger.info({"Parsed schedule": schedule_data})
    schedule_json_to_dynamo(dynamo_table_name, schedule_data=schedule_data)
