import requests
import boto3
import jwt
import time
import json
from os import getenv as env

DOWNLOAD_URLS_TABLE = env('DOWNLOAD_URLS_TABLE')
ZOOM_API_KEY = env('ZOOM_API_KEY')
ZOOM_API_SECRET = env('ZOOM_API_SECRET')

dynamo = boto3.resource('dynamodb')
table = dynamo.Table(DOWNLOAD_URLS_TABLE)


def handler(event, context):
    """
    This function accepts the incoming POST relay from the API Gateway endpoint that
    serves as the Zoom webhook endpoint. The payload from Zoom does not include
    the actual download url so we have to fetch that in a Zoom api call.
    """

    if 'body' not in event:
        print("Bad data: %s" % str(event))
        return {
            'statusCode': 400,
            'headers': {},
            'body': "No body in event"
        }

    # try except only for testing
    try:
        body = json.loads(event['body'])
    except Exception as e:
        print(e)
        body = event['body']

    uuid = None

    if 'type' in body:
        uuid = body["content"]["uuid"]
    elif 'status' in body:
        if body['status'] == "RECORDING_MEETING_COMPLETED":
            uuid = body["uuid"]
        else:
            print("Not handling notifications of type", body['status'])
    else:
        print("Bad data: %s" % str(event))
        return {
            'statusCode': 400,
            'headers': {},
            'body': event
        }

    record = get_recording_data(uuid)

    if record is not None:
        send_to_dynamodb(record)

    return {
        'statusCode': 200,
        'headers': {},
        'body': "Success"
    }


def gen_token(key=ZOOM_API_KEY, secret=ZOOM_API_SECRET, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def get_recording_data(uuid):

    if uuid is None:
        return None

    token = gen_token(seconds_valid=60)

    r = requests.get("https://api.zoom.us/v2/meetings/%s/recordings" % uuid,
                     headers={"Authorization": "Bearer %s" % token.decode()})
    response = r.json()

    if 'code' in response:
        if response['code'] == 3301:
            print("No recording found for meeting %s" % uuid)
        else:
            print("Meeting: %s, Received response: %s, %s" % (uuid, response['code'], response['message']))
        return None

    return format_metadata(response)


def format_metadata(recording):

    metadata = {}

    for key in ['account_id', 'duration', 'host_id',
                'start_time', 'timezone', 'topic', 'uuid']:
        # dynamoDB does not accept values that are empty strings
        if key in recording and recording[key] != '':
            metadata[key] = recording[key]

    if 'meeting_number' in recording:
        metadata['meeting_series_id'] = recording['meeting_number']

    for data in recording['recording_files']:
        if data['file_type'] == "MP4":
            if data['status'] != 'completed':
                print("ERROR: Recording status not 'completed'")
                return None

            if 'download_url' in data:
                metadata['DownloadUrl'] = data['download_url']
            else:
                print("ERROR: Download url not found.")

            for key in ['file_type', 'play_url',
                        'recording_start', 'recording_end']:
                if data[key] != '':
                    metadata[key] = data[key]

            if 'file_size' in data and data['file_size'] != '':
                metadata['file_size_bytes'] = data['file_size']
            if 'id' in data and data['id'] != '':
                metadata['file_id'] = data['id']
            if 'meeting_id' in data and data['meeting_id'] != '':
                metadata['meeting_uuid'] = data['meeting_id']

            return metadata

    return None


def send_to_dynamodb(record, dbtable=table):
    try:
        dbtable.put_item(Item=record, ConditionExpression="attribute_not_exists(DownloadUrl)")
        print("Record created at %s. Record: %s" % (dbtable.creation_date_time, record))
    except Exception as e:
        if type(e).__name__ == "ConditionalCheckFailedException":
            print("Duplicate. URL: %s already in database" % record['DownloadUrl'])
            pass
        else:
            print("Exception %s" % e)
