import requests
import boto3
import jwt
import time
import json
from datetime import datetime
from urllib.parse import parse_qsl
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


class NoRecordingFound(Exception):
    pass


class IgnoreEventType(Exception):
    pass


class ApiResponseParsingFailure(Exception):
    pass


def resp_204(msg):
    print("http 204 response: {}".format(msg))
    return {
        'statusCode': 204,
        'headers': {},
        'body': "" # 204 = no content
    }


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

    print("EVENT:", event)

    if 'body' not in event:
        return resp_400("bad data: no body in event")

    try:
        payload = parse_payload(event['body'])
        print('PAYLOAD: ' + str(payload))
    except BadWebhookData as e:
        return resp_400("bad webhook payload data: {}".format(str(e)))

    if payload['status'] != 'RECORDING_MEETING_COMPLETED':
        return resp_204(
            "Handling not implement for status '{}'".format(payload['status'])
        )

    lookup_retries = MEETING_LOOKUP_RETRIES
    while True:
        try:
            print("looking up meeting {}".format(payload['uuid']))
            recording_data = get_recording_data(payload['uuid'])
            break
        except NoRecordingFound as e:
            return resp_204(str(e))
        except MeetingLookupFailure as e:
            if lookup_retries > 0:
                lookup_retries -= 1
                print("retrying. {} retries left".format(lookup_retries))
                time.sleep(MEETING_LOOKUP_RETRY_DELAY)
            else:
                return resp_400("Meeting lookup retries exhausted: {}".format(str(e)))

    try:
        if not verify_status(recording_data):
            return resp_204("No recordings ready to download")
    except ApiResponseParsingFailure:
        return resp_400("Failed to parse Zoom API response")

    now = datetime.utcnow().isoformat()
    db_record = {
        'meeting_uuid': payload['uuid'],
        'recording_data': json.dumps(recording_data),
        'created': now,
        'updated': now
    }

    if 'dryrun' not in event:
        save_to_dynamodb(db_record)
    else:
        print("dryrun: skipping dynamodb put item")

    return {
        'statusCode': 200,
        'headers': {},
        'body': "Success"
    }


def parse_payload(event_body):

    try:
        payload = dict(parse_qsl(event_body, strict_parsing=True))
    except ValueError as e:
        raise BadWebhookData(str(e))

    if 'type' in payload:
        print("Got old-style payload")
        payload['status'] = payload['type']
        del payload['type']
        if 'content' in payload:
            try:
                payload['uuid'] = json.loads(payload['content'])['uuid']
                del payload['content']
            except Exception as e:
                raise BadWebhookData("Failed to parse payload 'content' value")
        else:
            raise BadWebhookData("payload missing 'content' value")
    elif 'status' not in payload:
        raise BadWebhookData("payload missing 'status' value")
    else:
        print("Got new-style payload")

    return payload


def gen_token(key=ZOOM_API_KEY, secret=ZOOM_API_SECRET, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def get_recording_data(uuid):

    token = gen_token(seconds_valid=600)

    try:
        meeting_url = "https://api.zoom.us/v2/meetings/%s/recordings" % uuid
        headers = {"Authorization": "Bearer %s" % token.decode()}
        r = requests.get(meeting_url, headers=headers)
        r.raise_for_status()
        recording_data = r.json()
        print("Recording lookup response: {}".format(str(recording_data)))

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            recording_data = e.response.json()
            if 'code' in recording_data and recording_data['code'] == 3301:
                print("Meeting: {}, response code: '{}', message: '{}'".format(
                    uuid,
                    recording_data.get('code', ''),
                    recording_data.get('message', '')
                ))

            raise NoRecordingFound("No recording found for meeting %s" % uuid)
        else:
            raise MeetingLookupFailure("Zoom API request error: {}, {}".format(r.content, repr(e)))
    except requests.ConnectionError as e:
        raise MeetingLookupFailure("Zoom API connection error: {}".format(repr(e)))

    return recording_data


def verify_status(recording_data):

    if 'recording_files' not in recording_data \
            or not len(recording_data['recording_files']):
        return False

    for file in recording_data['recording_files']:

        file_id = file['id']
        status = file['status']
        print("file '{}' has status {}".format(file_id, status))

        if status != 'completed':
            print("ERROR: Recording status not 'completed'")
            return False

        if 'download_url' not in file:
            raise ApiResponseParsingFailure(
                "ERROR: file '{}' is missing a download_url".format(file_id)
            )

    return True


def save_to_dynamodb(record):

    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(DOWNLOAD_URLS_TABLE)

    try:
        table.put_item(Item=record, ConditionExpression="attribute_not_exists(meeting_uuid)")
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("Duplicate! meeting_uuid '{}' already in database".format(record['meeting_uuid']))
            pass
        else:
            raise


if __name__ == '__main__':
    """
    for local testing. pass in the "body" payload string as the only argument.
    for this to work you need to have the .env values pre-sourced.
    alternatively, there is a pycharm plugin that will allow you to configure
    a .env file to load in a run configuration: https://github.com/Ashald/EnvFile
    """

    import sys
    body = sys.argv[-1]

    handler({'dryrun': True, 'body': body}, None)
