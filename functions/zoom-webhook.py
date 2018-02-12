import requests
import boto3
import jwt
import time
import json
from datetime import datetime
from urllib.parse import parse_qsl
from os import getenv as env
from botocore.exceptions import ClientError

import logging
from common import setup_logging
logger = logging.getLogger()

DOWNLOAD_URLS_TABLE = env('DOWNLOAD_URLS_TABLE')
ZOOM_API_KEY = env('ZOOM_API_KEY')
ZOOM_API_SECRET = env('ZOOM_API_SECRET')
MEETING_LOOKUP_RETRIES = 2
MEETING_LOOKUP_RETRY_DELAY = 5

dynamo = boto3.resource('dynamodb')


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
    logger.info("http 204 response: {}".format(msg))
    return {
        'statusCode': 204,
        'headers': {},
        'body': "" # 204 = no content
    }


def resp_400(msg):
    logger.error("http 400 response: {}".format(msg))
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

    setup_logging(context)
    logger.info(event)

    if 'body' not in event:
        return resp_400("bad data: no body in event")

    try:
        payload = parse_payload(event['body'])
        logger.info('PAYLOAD: ' + str(payload))
    except BadWebhookData as e:
        return resp_400("bad webhook payload data: {}".format(str(e)))

    if payload['status'] != 'RECORDING_MEETING_COMPLETED':
        return resp_204(
            "Handling not implement for status '{}'".format(payload['status'])
        )

    lookup_retries = MEETING_LOOKUP_RETRIES
    while True:
        try:
            logger.info("looking up meeting {}".format(payload['uuid']))
            recording_data = get_recording_data(payload['uuid'])
            break
        except NoRecordingFound as e:
            logger.error(e)
        except MeetingLookupFailure as e:
            logger.error(e)
        except Exception:
            raise

        if lookup_retries > 0:
            lookup_retries -= 1
            logger.info("retrying. {} retries left".format(lookup_retries))
            time.sleep(MEETING_LOOKUP_RETRY_DELAY)
        else:
            return resp_400("Meeting lookup retries exhausted: {}")

    try:
        if not verify_status(recording_data):
            return resp_204("No recordings ready to download")
        logger.info("All recordings ready to download")
    except ApiResponseParsingFailure:
        return resp_400("Failed to parse Zoom API response")
          
    try:
        recording_data.update(get_host_data(payload['host_id']))
    except Exception as e:
        return resp_400(repr(e))

    now = datetime.utcnow().isoformat()
    db_record = {
        'meeting_uuid': payload['uuid'],
        'recording_data': json.dumps(recording_data),
        'created': now,
        'updated': now,
        'webhook_correlation_id': context.aws_request_id
    }

    save_to_dynamodb(db_record)
    logger.info("webhook handler complete")

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
        logger.info("Got old-style payload")
        payload['status'] = payload['type']
        del payload['type']
        if 'content' in payload:
            try:
                content = json.loads(payload['content'])
                payload['uuid'] = content['uuid']
                payload['host_id'] = content['host_id']
                if 'id' in payload:
                    payload['id'] = content['id']
                    logger.info("Missing meeting series id ('id')")
                del payload['content']
            except Exception as e:
                raise BadWebhookData("Failed to parse payload 'content' value")
        else:
            raise BadWebhookData("payload missing 'content' value")
    elif 'status' not in payload:
        raise BadWebhookData("payload missing 'status' value")
    else:
        logger.info("Got new-style payload")

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
        logger.debug(recording_data)

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            recording_data = e.response.json()
            if 'code' in recording_data and recording_data['code'] == 3301:
                logger.error({
                    'meeting': uuid,
                    'response code': recording_data.get('code'),
                    'message': recording_data.get('message')
                })

            raise NoRecordingFound("No recording found for meeting %s" % uuid)
        else:
            raise MeetingLookupFailure("Zoom API request error: {}, {}".format(r.content, repr(e)))
    except requests.ConnectionError as e:
        raise MeetingLookupFailure("Zoom API connection error: {}".format(repr(e)))

    return recording_data


def get_host_data(host_id):

    host_data = {}

    try:
        logger.debug('Looking up host_id "{}"'.format(host_id))
        r = requests.get("https://api.zoom.us/v2/users/%s" % host_id,
                         headers={"Authorization": "Bearer %s" % gen_token().decode()})
        r.raise_for_status()
        response = r.json()

        logger.debug(response)

        host_data['host_name'] = "{} {}".format(response['first_name'], response['last_name'])
        host_data['host_email'] = response['email']

    except KeyError as e:
        raise MeetingLookupFailure("Missing host data. {}".format(repr(e)))
    except requests.HTTPError as e:
        raise MeetingLookupFailure("Zoom API request error: {}, {}".format(r.content, repr(e)))

    return host_data


def verify_status(recording_data):

    if 'recording_files' not in recording_data \
            or not len(recording_data['recording_files']):
        return False

    for file in recording_data['recording_files']:

        file_id = file['id']
        status = file['status']
        logger.debug("file '{}' has status {}".format(file_id, status))

        if status != 'completed':
            logger.error("ERROR: Recording status not 'completed'")
            return False

        if 'download_url' not in file:
            raise ApiResponseParsingFailure(
                "ERROR: file '{}' is missing a download_url".format(file_id)
            )

    return True


def save_to_dynamodb(record):

    table = dynamo.Table(DOWNLOAD_URLS_TABLE)
    logger.debug(record)

    try:
        table.put_item(Item=record, ConditionExpression="attribute_not_exists(meeting_uuid)")
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.error("Duplicate! meeting_uuid '{}' already in database".format(record['meeting_uuid']))
            pass
        else:
            raise


