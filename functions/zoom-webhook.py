import requests
import boto3
import jwt
import time
import json
from os import getenv as env
from botocore.exceptions import ClientError

DOWNLOAD_URLS_TABLE = env('DOWNLOAD_URLS_TABLE')
ZOOM_API_KEY = env('ZOOM_API_KEY')
ZOOM_API_SECRET = env('ZOOM_API_SECRET')
MEETING_LOOKUP_RETRIES = 2
MEETING_LOOKUP_RETRY_DELAY = 5


class BadWebhookData(Exception):
    pass


class MeetingLookupFailure(Exception):
    pass

def resp_400(msg):
    print("http 400 response: {}".format(msg))
    return {
        'statusCode': 400,
        'headers': {},
        'body': msg
    }

def handler(event, context):
    """
    This function accepts the incoming POST relay from the API Gateway endpoint that
    serves as the Zoom webhook endpoint. The payload from Zoom does not include
    the actual download url so we have to fetch that in a Zoom api call.
    """

    if 'body' not in event:
        return resp_400("bad data: no body in event")

    payload = json.loads(event['body'])

    try:
        uuid = get_meeting_uuid(payload)
    except BadWebhookData as e:
        print(payload)
        return resp_400("bad webhook data: {}".format(str(e)))

    lookup_retries = MEETING_LOOKUP_RETRIES
    while True:
        try:
            print("looking up meeting {}".format(uuid))
            recording_data = get_recording_data(uuid)
            break
        except MeetingLookupFailure as e:
            if lookup_retries > 0:
                lookup_retries -= 1
                print("retrying. {} retries left".format(lookup_retries))
                time.sleep(MEETING_LOOKUP_RETRY_DELAY)
            else:
                print("retries exhausted.")
                resp_400("Meeting lookup failure: {}".format(str(e)))

    records = generate_records(recording_data)

    if not len(records):
        return resp_400("No recordings to download")

    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(DOWNLOAD_URLS_TABLE)

    for record in records:
        send_to_dynamodb(record, table)

    return {
        'statusCode': 200,
        'headers': {},
        'body': "Success"
    }


def get_meeting_uuid(payload):

    if 'type' in payload:
        if payload['type'] == 'RECORDING_MEETING_COMPLETED':
            return payload["content"]["uuid"]
        else:
            raise BadWebhookData(
                "Don't know how to handle 'type' of {}".format(payload['type'])
            )
    elif 'status' in payload:
        if payload['status'] == "RECORDING_MEETING_COMPLETED":
            return payload["uuid"]
        else:
            raise BadWebhookData(
                "Don't know how to handle 'status' of {}".format(payload['status'])
            )
    raise BadWebhookData("Payload missing 'type' or 'status'")


def gen_token(key=ZOOM_API_KEY, secret=ZOOM_API_SECRET, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def get_recording_data(uuid):

    token = gen_token(seconds_valid=60)

    try:
        meeting_url = "https://api.zoom.us/v2/meetings/%s/recordings" % uuid
        headers = {"Authorization": "Bearer %s" % token.decode()}
        r = requests.get(meeting_url, headers=headers)
        r.raise_for_status()
        recording_data = r.json()
        print("Recording lookup response: {}".format(str(recording_data)))

    except requests.HTTPError as e:
        raise MeetingLookupFailure("Zoom API request error: {}, {}".format(r.content, repr(e)))
    except requests.ConnectionError as e:
        raise MeetingLookupFailure("Zoom API connection error: {}".format(repr(e)))

    if 'code' in recording_data:
        print("Meeting: {}, response code: '{}', message: '{}'".format(
            uuid,
            recording_data.get('code', ''),
            recording_data.get('message', '')
        ))

        if recording_data['code'] == 3301:
            raise MeetingLookupFailure("No recording found for meeting %s" % uuid)

    return recording_data


def generate_records(recording_data):

    records = []

    if 'recording_files' not in recording_data:
        return records

    for file in recording_data['recording_files']:
        record = {}

        if file['file_type'].lower() == "mp4":

            if file['status'] != 'completed':
                print("ERROR: Recording status not 'completed'")
                continue

            if 'download_url' in file:
                record['DownloadUrl'] = file['download_url']
            else:
                print("ERROR: Download url not found.")
                continue

            for key in ['file_type', 'play_url', 'recording_start', 'recording_end']:
                record[key] = file[key]

            if 'file_size' in file:
                record['file_size_bytes'] = file['file_size']
            if 'id' in file:
                record['file_id'] = file['id']
            if 'meeting_id' in file:
                record['meeting_uuid'] = file['meeting_id']

            for key in ['account_id', 'duration', 'host_id',
                        'start_time', 'timezone', 'topic', 'uuid']:
                if key in recording_data:
                    record[key] = recording_data[key]

            if 'meeting_number' in recording_data:
                record['meeting_series_id'] = recording_data['meeting_number']

            # dynamoDB does not accept values that are empty strings
            record = {k: v for k, v in record.items() if v}

            records.append(record)

    return records


def send_to_dynamodb(record, dbtable):
    try:
        dbtable.put_item(Item=record, ConditionExpression="attribute_not_exists(DownloadUrl)")
        print("Record created at %s. Record: %s" % (dbtable.creation_date_time, record))
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("Duplicate. URL: %s already in database" % record['DownloadUrl'])
            pass
        else:
            raise
