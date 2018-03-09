import boto3
import json
import time
import requests
from os import getenv as env
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from hashlib import md5
from urllib.parse import urljoin
from botocore.exceptions import ClientError
from operator import itemgetter
from common import setup_logging, gen_token

import logging
logger = logging.getLogger()

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
DOWNLOAD_QUEUE_NAME = env('DOWNLOAD_QUEUE_NAME')
UPLOAD_QUEUE_NAME = env('UPLOAD_QUEUE_NAME')
MIN_CHUNK_SIZE = 5242880
MEETING_LOOKUP_RETRIES = 2
MEETING_LOOKUP_RETRY_DELAY = 60
ZOOM_API_BASE_URL = 'https://api.zoom.us/v2/'

s3 = boto3.client('s3')
sqs = boto3.resource('sqs')


class RecordingSegmentsOverlap(Exception):
    pass


class FileNameNotFound(Exception):
    pass


class HostLookupFailure(Exception):
    pass


class ApiLookupFailure(Exception):
    """
    Only raise this on retry-able failures, e.g. connection/timeout errors
    or cases where recording status is not really complete
    """
    pass


class ApiResponseParsingFailure(Exception):
    pass


@setup_logging
def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    logger.debug("downloader invoked!")
    logger.info(event)

    download_queue = sqs.get_queue_by_name(QueueName=DOWNLOAD_QUEUE_NAME)

    try:
        messages = download_queue.receive_messages(
            MaxNumberOfMessages=1,
            VisibilityTimeout=700
        )
        download_message = messages[0]
        logger.debug({'queue message': download_message})

    except IndexError:
        logger.info("No download queue messages available")
        return

    meeting_data = json.loads(download_message.body)
    logger.info(meeting_data)

    try:
        # get name/email of host
        host_data = get_host_data(meeting_data['host_id'])
        logger.info({'host_data': host_data})

        # get data about recording files
        recording_data = get_recording_data(meeting_data['uuid'])
        logger.info(recording_data)

    except ApiLookupFailure as e:
        logger.error(e)

        # this type of exception should only get raised on retry-able failures
        # unset the visibility timeout on this message to make sure it can
        # get picked up again on the next scheduled execution
        download_message.change_visibility(VisibilityTimeout=0)
        return

    except Exception as e:
        logger.exception("Something went horribly wrong "
                         "trying to fetch the host or recording data"
                         )
        # TODO: move download_message to the DLQ and return without raising
        raise

    chronological_files = sorted(recording_data['recording_files'], key=itemgetter('recording_start'))
    logger.info("downloading {} files".format(len(chronological_files)))

    track_sequence = 1
    prev_file = None

    for file in chronological_files:
        if next_track_sequence(prev_file, file):
                track_sequence += 1

        logger.debug("file {} is track sequence {}".format(file, track_sequence))
        prev_file = file

        stream_file_to_s3(file, recording_data['uuid'], track_sequence)

    upload_message = {
        "uuid": meeting_data['uuid'],
        "meeting_number": recording_data['meeting_number'],
        "host_name": host_data['host_name'],
        "topic": recording_data['topic'],
        "start_time": recording_data['start_time'],
        "recording_count": recording_data['recording_count'],
        "correlation_id": meeting_data['correlation_id']
    }
    send_sqs_message(upload_message)

    download_message.delete()
    logger.info("downloader handler complete")


def get_host_data(host_id):

    endpoint_url = urljoin(ZOOM_API_BASE_URL, 'users/{}'.format(host_id))
    host_data = get_api_data(endpoint_url)
    return {
        'host_name': "{} {}".format(host_data['first_name'], host_data['last_name']),
        'host_email': host_data['email']
    }


def get_recording_data(uuid):
    endpoint_url = urljoin(ZOOM_API_BASE_URL, 'meetings/{}/recordings'.format(uuid))
    return get_api_data(endpoint_url, validate_callback=verify_recording_status)


def get_api_data(endpoint_url, validate_callback=None):

    lookup_retries = 0
    while True:
        try:
            resp_data = api_request(endpoint_url)

            if validate_callback is None or validate_callback(resp_data):
                return resp_data

        except requests.ConnectTimeout as e:
            logger.error(e)

        if lookup_retries < MEETING_LOOKUP_RETRIES:
            lookup_retries += 1
            logger.info("waiting {} seconds to retry. this is retry {} of {}"
                        .format(MEETING_LOOKUP_RETRY_DELAY,
                                lookup_retries,
                                MEETING_LOOKUP_RETRIES
                                )
                        )
            time.sleep(MEETING_LOOKUP_RETRY_DELAY)
        else:
            raise ApiLookupFailure("No more retries for you!")


def api_request(endpoint_url):

    logger.debug("getting response from {}".format(endpoint_url))
    token = gen_token(seconds_valid=600)
    headers = {"Authorization": "Bearer %s" % token.decode()}
    r = requests.get(endpoint_url, headers=headers)
    r.raise_for_status()
    resp_data = r.json()
    logger.debug(resp_data)
    return resp_data


def verify_recording_status(recording_data):

    if 'recording_files' not in recording_data \
            or not len(recording_data['recording_files']):
        return False

    for file in recording_data['recording_files']:

        file_id = file['id']
        status = file['status']
        logger.debug("file '{}' has status {}".format(file_id, status))

        if status != 'completed':
            logger.warning("ERROR: Recording status not 'completed'")
            return False

        if 'download_url' not in file:
            raise ApiResponseParsingFailure(
                "ERROR: file '{}' is missing a download_url".format(file_id)
            )

    logger.info("All recordings ready to download")
    return True


def stream_file_to_s3(file, uuid, track_sequence):
    metadata = {'uuid': uuid,
                'track_sequence': str(track_sequence)}

    if file['file_type'].lower() in ["mp4", "m4a", "chat"]:
        stream, zoom_name = get_connection_using_play_url(file)

        if 'gallery' in zoom_name.lower():
            metadata['view'] = "gallery"
        elif file['file_type'].lower() == "mp4":
            metadata['view'] = "speaker"
    else:
        stream, zoom_name = get_connection_using_download_url(file)

    metadata['file_type'] = zoom_name.split('.')[-1]
    filename = create_filename(file['id'], file['meeting_id'], zoom_name)
    logger.info("filename: {}".format(filename))

    try:
        if key_exists(filename):
            logger.warning("Skip stream to S3. Key {} already in bucket.".format(filename))
            return
    except Exception as e:
        logger.error("Received error when searching for s3 bucket key: {}".format(e))
        raise

    logger.info("Beginning upload of {}".format(filename))
    part_info = {'Parts': []}
    mpu = s3.create_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename, Metadata=metadata)

    try:
        for part_number, chunk in enumerate(stream.iter_content(chunk_size=MIN_CHUNK_SIZE), 1):
            logger.info("uploading part {}".format(part_number))
            part = s3.upload_part(Body=chunk, Bucket=ZOOM_VIDEOS_BUCKET,
                                  Key=filename, PartNumber=part_number, UploadId=mpu['UploadId'])

            part_info['Parts'].append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })

        s3.complete_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                     UploadId=mpu['UploadId'],
                                     MultipartUpload=part_info)
        print("Completed multipart upload of {}.".format(filename))
    except Exception as e:
        logger.exception("Something went wrong with upload of {}:{}".format(filename, e))
        s3.abort_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                  UploadId=mpu['UploadId'])

    stream.close()


def send_sqs_message(message):
    logger.debug("SQS sending start...")
    logger.debug(message)

    try:
        upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

        message_sent = upload_queue.send_message(
            MessageBody=json.dumps(message),
            MessageGroupId=message['uuid'],
            MessageDeduplicationId=message['uuid']
        )
    except Exception as e:
        logger.exception("Error when sending SQS message for meeting uuid {} :{}".format(message['uuid'], e))
        raise

    logger.debug({"Message sent": message_sent})


def next_track_sequence(prev_file, file):

    if prev_file is None:
        return False

    logger.debug({'start': file['recording_start'], 'end': file['recording_end']})

    if file['recording_start'] == prev_file['recording_start']:
        logger.debug("start matches")
        if file['recording_end'] == prev_file['recording_end']:
            logger.debug("end matches")
            return False

    if file['recording_start'] >= prev_file['recording_end']:
        logger.debug("start >= end")
        return True

    raise RecordingSegmentsOverlap


def retrieve_url_from_play_page(play_url):

    logger.info("requesting {}".format(play_url))

    r = requests.get(play_url)
    r.raise_for_status()

    only_source_tags = SoupStrainer("source", type="video/mp4")

    source = BeautifulSoup(r.content, "html.parser", parse_only=only_source_tags)
    logger.debug(str(source))

    source_object = source.find("source")

    if source_object is None:
        logger.warning("No source element found")
        return None

    link = source_object['src']
    logger.info("Got download url {}".format(link))

    return link


def get_connection_using_play_url(file):
    file_url = retrieve_url_from_play_page(file['play_url'])
    if file_url is None:
        raise FileNameNotFound("Cannot get file name from {}".format(file['play_url']))

    zoom_name = file_url.split("?")[0].split("/")[-1]

    logger.info("requesting {}".format(file_url))

    r = requests.get(file_url, stream=True)
    r.raise_for_status()

    return r, zoom_name


def get_connection_using_download_url(file):

    logger.info("requesting {}".format(file['download_url']))

    r = requests.get(file['download_url'], stream=True)
    r.raise_for_status()

    try:
        zoom_name = r.headers['Content-Disposition'].split("=")[-1]
        logger.info("got filename {}".format(zoom_name))
    except KeyError as e:
        raise FileNameNotFound

    return r, zoom_name


def create_filename(uuid, meeting_id, zoom_filename):
    file_type = ".".join(zoom_filename.split(".")[1:]).lower()
    md5_uuid = md5(meeting_id.encode()).hexdigest()
    filename = "{}/{}.{}".format(md5_uuid, uuid, file_type)
    return filename


def key_exists(filename):
    try:
        s3.head_object(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename)
        logger.debug("key {} already in bucket {}".format(filename, ZOOM_VIDEOS_BUCKET))
        return True
    except ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            print("Key {} not yet in bucket {}".format(filename, ZOOM_VIDEOS_BUCKET))
            return False
        raise
