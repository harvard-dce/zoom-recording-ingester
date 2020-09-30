import sys
import json
import jwt
import time
import logging
import requests
import aws_lambda_logging
from functools import wraps
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname, exists
from urllib.parse import urlparse
import csv
import itertools
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import codecs

import boto3
# google sheets imports
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logger = logging.getLogger()

load_dotenv(join(dirname(__file__), '../.env'))

LOG_LEVEL = env('DEBUG') and 'DEBUG' or 'INFO'
BOTO_LOG_LEVEL = env('BOTO_DEBUG') and 'DEBUG' or 'INFO'
ZOOM_API_BASE_URL = "https://api.zoom.us/v2/"
ZOOM_API_KEY = env("ZOOM_API_KEY")
ZOOM_API_SECRET = env("ZOOM_API_SECRET")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")
STACK_NAME = env("STACK_NAME")


class ZoomApiRequestError(Exception):
    pass


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


def gen_token(key, secret, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def zoom_api_request(endpoint, key=ZOOM_API_KEY, secret=ZOOM_API_SECRET,
                     seconds_valid=60, ignore_failure=False, retries=3):
    required_params = [("endpoint", endpoint),
                       ("zoom api key", key),
                       ("zoom api secret", secret)]
    for name, param in required_params:
        if not param:
            raise Exception(
                "Call to zoom_api_request "
                "missing required param '{}'".format(name)
            )

    url = "{}{}".format(ZOOM_API_BASE_URL, endpoint)
    headers = {
        "Authorization": "Bearer {}"
        .format(gen_token(key, secret, seconds_valid).decode())
    }

    while True:
        try:
            r = requests.get(url, headers=headers)
            break
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ConnectTimeout) as e:
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


class GSheetsToken():
    def __init__(self, in_lambda=False):
        self.ssm = boto3.client("ssm")
        self.ssm_path = f"/zoom-ingester/{STACK_NAME}/token.pickle"
        self.load_token()
        if not self.creds:
            if in_lambda:
                raise Exception("Cannot find token.pickle file.")
            else:
                logger.info("Create a new gsheets token")
                self.create_token()
                self.save_token()
        elif not self.creds.valid:
            logger.info("Refresh existing gsheets token")
            self.refresh_token()
            self.save_token()

    def valid(self):
        return self.creds and self.creds.valid

    def load_token(self):
        # The file token.pickle stores the user's access and refresh tokens,
        # and is created automatically when the authorization flow completes
        # for the first time.
        try:
            r = self.ssm.get_parameter(
                Name=self.ssm_path,
                WithDecryption=True,
            )
            token = r["Parameter"]["Value"]
            self.creds = pickle.loads(codecs.decode(token.encode(), "base64"))
        except self.ssm.exceptions.ParameterNotFound:
            logger.error(f"SSM Parameter {self.ssm_path} not found")
            self.creds = None

    def create_token(self):
        if not exists("credentials.json"):
            raise Exception("Missing required credentials.json file.")
        flow = InstalledAppFlow.from_client_secrets_file(
            "credentials.json", [GSHEETS_SCOPE]
        )
        self.creds = flow.run_local_server(port=0)

    def refresh_token(self):
        # If there are no (valid) credentials available,
        # try to refresh the token or let the user log in.
        if self.creds and self.creds.expired and self.creds.refresh_token:
            self.creds.refresh(Request())

    def save_token(self):
        # Save the credentials for the next run
        r = self.ssm.put_parameter(
            Name=self.ssm_path,
            Description="Token for gsheets authorization",
            Value=codecs.encode(pickle.dumps(self.creds), "base64").decode(),
            Type="SecureString",
            Overwrite=True,
        )
        logger.info(f"Saved token version {r['Version']}")

    def delete_token(self):
        r = self.ssm.delete_parameter(Name=self.ssm_path)
        if (r["ResponseMetadata"]["HTTPStatusCode"] == 200):
            logger.info(f"Successfully destroyed ssm parameter {self.ssm_path}")
        else:
            logger.info(f"Failed to destroy ssm parameter {self.ssm_path}")


class GSheet:
    def __init__(self, spreadsheet_id, in_lambda=False):
        self.spreadsheet_id = spreadsheet_id
        token = GSheetsToken(in_lambda)
        if not token.valid():
            raise Exception("No valid gsheets token found.")
        else:
            self.creds = token.creds

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

        logger.debug(f"Successfully downloaded {sheet_name}.csv")
        return file_path

    def import_to_dynamo(self, sheet_name):
        file_path = self._download_csv(sheet_name)

        schedule_csv_to_dynamo(SCHEDULE_TABLE, file_path)


def schedule_json_to_dynamo(schedule_table, json_file=None, schedule_data=None):

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
        table_name = f"{schedule_table}"
        table = dynamodb.Table(table_name)

        for item in schedule_data.values():
            table.put_item(Item=item)
    except ClientError as e:
        error = e.response['Error']
        raise Exception("{}: {}".format(error['Code'], error['Message']))


def schedule_csv_to_dynamo(schedule_table, filepath):
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
            datetime.strftime(time_object, "%H:%M"),
            (time_object + timedelta(minutes=30)).strftime("%H:%M"),
            (time_object + timedelta(hours=1)).strftime("%H:%M")
        ])

    for id, item in schedule_data.items():
        item["Days"] = list(item["Days"])
        item["Time"] = list(item["Time"])

    schedule_json_to_dynamo(schedule_table, schedule_data=schedule_data)
