import boto3
import json
import time
import requests
from os import getenv as env
from hashlib import md5
from urllib.parse import urljoin
from operator import itemgetter
from common import setup_logging, gen_token
import subprocess

import logging
logger = logging.getLogger()

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
DOWNLOAD_QUEUE_NAME = env('DOWNLOAD_QUEUE_NAME')
UPLOAD_QUEUE_NAME = env('UPLOAD_QUEUE_NAME')
DEADLETTER_QUEUE_NAME = env('DEADLETTER_QUEUE_NAME')
MIN_CHUNK_SIZE = 5242880
MEETING_LOOKUP_RETRIES = 2
MEETING_LOOKUP_RETRY_DELAY = 60
ZOOM_API_BASE_URL = env('ZOOM_API_BASE_URL')
ZOOM_ADMIN_EMAIL = env('ZOOM_ADMIN_EMAIL')


class PermanentDownloadError(Exception):
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
    sqs = boto3.resource('sqs')

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

    message_body = json.loads(download_message.body)
    logger.info(message_body)

    try:
        # get name/email of host
        host_data = get_host_data(message_body['host_id'])
        logger.info({'host_data': host_data})

        # get data about recording files
        recording_data = get_recording_data(message_body['uuid'])
        logger.info(recording_data)
    except ApiLookupFailure as e:
        # Retry-able error
        logger.error(e)
        return
    except Exception as e:
        logger.exception("Something went horribly wrong "
                         "trying to fetch the host or recording data:"
                         " {}".format(e))
        send_to_sqs(message_body, DEADLETTER_QUEUE_NAME, error=e)
        download_message.delete()
        logger.info("Moved message to DLS and deleted message from source queue.")
        raise

    chronological_files = filter_and_sort(recording_data['recording_files'])
    if not chronological_files:
        raise PermanentDownloadError("No files available to download.")
    logger.info("downloading {} files".format(len(chronological_files)))

    track_sequence = 1
    prev_file = None

    try:
        for file in chronological_files:
            if next_track_sequence(prev_file, file):
                    track_sequence += 1

            logger.debug("file {} is track sequence {}".format(file, track_sequence))
            prev_file = file

            stream_file_to_s3(file, recording_data['uuid'], track_sequence)

        if 'meeting_number' in recording_data:
            meeting_number = recording_data['meeting_number']
        elif 'id' in recording_data:
            meeting_number = recording_data['id']
        else:
            raise PermanentDownloadError("Missing meeting number in API response")

        if not (8 < len(str(meeting_number)) < 12):
            raise PermanentDownloadError("Invalid meeting number: {}".format(meeting_number))

        upload_message = {
            "uuid": message_body['uuid'],
            "meeting_number": meeting_number,
            "host_name": host_data['host_name'],
            "host_id": message_body['host_id'],
            "topic": recording_data['topic'],
            "meeting_start_time": recording_data['start_time'],
            "recording_start_time": recording_data['recording_files'][0]['recording_start'],
            "recording_count": recording_data['recording_count'],
            "webhook_received_time": message_body['received_time'],
            "correlation_id": message_body['correlation_id']
        }

        send_to_sqs(upload_message, UPLOAD_QUEUE_NAME)
    except PermanentDownloadError as e:
        logger.error(e)
        send_to_sqs(message_body, DEADLETTER_QUEUE_NAME, error=e)
        download_message.delete()
        raise

    download_message.delete()


def get_host_data(host_id):

    endpoint_url = urljoin(ZOOM_API_BASE_URL, 'users/{}'.format(host_id))
    host_data = get_api_data(endpoint_url)
    return {
        'host_name': "{} {}".format(host_data['first_name'], host_data['last_name']),
        'host_email': host_data['email']
    }


def get_recording_data(uuid):
    # Must use string concatenation rather than urljoin because uuids may contain
    # url unsafe characters like forward slash
    endpoint_url = ZOOM_API_BASE_URL + 'meetings/{}/recordings'.format(uuid)
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
        raise PermanentDownloadError("No recordings for this meeting.")

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

    raise PermanentDownloadError("Recording segments overlap.")


def stream_file_to_s3(file, uuid, track_sequence):
    s3 = boto3.client('s3')

    metadata = {'uuid': uuid}

    stream, zoom_name = get_stream(file['download_url'])

    if 'gallery' in zoom_name.lower():
        metadata['view'] = "gallery"
    elif file['file_type'].lower() == "mp4":
        metadata['view'] = "speaker"

    metadata['file_type'] = zoom_name.split('.')[-1]
    filename = create_filename("{:03d}-{}".format(track_sequence, file['id']),
                               file['meeting_id'],
                               zoom_name)

    logger.info("Beginning upload of {}".format(filename))
    part_info = {'Parts': []}
    mpu = s3.create_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename, Metadata=metadata)

    try:
        for part_number, chunk in enumerate(stream.iter_content(chunk_size=MIN_CHUNK_SIZE), 1):
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

    if file['file_type'].lower() == "mp4":
        if not is_valid_mp4(filename):
            stream.close()
            raise Exception("MP4 failed to transfer.")

    stream.close()


def get_stream(download_url):

    admin_token = get_admin_token()

    # use zak token to get download stream
    logger.info("requesting {}".format(download_url))

    r = requests.get("{}?zak={}".format(download_url, admin_token), allow_redirects=False)
    if 'Location' in r.headers:
        location = r.headers['Location']
        zoom_name = location.split('?')[0].split('/')[-1]
    else:
        zoom_name = r.headers['Content-Disposition'].split("=")[-1]

    if zoom_name == '':
        raise PermanentDownloadError("Failed to get file name from download header.")

    logger.info("got filename {}".format(zoom_name))

    r = requests.get("{}?zak={}".format(download_url, admin_token), stream=True)

    return r, zoom_name


def get_admin_token():
    # get admin user id from admin email
    r = requests.get("{}users/{}".format(ZOOM_API_BASE_URL, ZOOM_ADMIN_EMAIL),
                     headers={"Authorization": "Bearer %s" % gen_token().decode()})
    admin_id = r.json()['id']
    # get admin level zak token from admin id
    r = requests.get("{}users/{}/token?type=zak".format(ZOOM_API_BASE_URL, admin_id),
                     headers={"Authorization": "Bearer %s" % gen_token().decode()})
    return r.json()['token']


def create_filename(file_id, meeting_id, zoom_filename):
    file_type = ".".join(zoom_filename.split(".")[1:]).lower()
    md5_meeting_id = md5(meeting_id.encode()).hexdigest()
    filename = "{}/{}.{}".format(md5_meeting_id, file_id, file_type)
    return filename


def send_to_sqs(message, queue_name, error=None):
    logger.debug("Sending SQS message to {}...".format(queue_name))
    logger.debug(message)
    sqs = boto3.resource('sqs')

    if error is None:
        message_attributes = {}
    else:
        message_attributes = {
            'FailedReason': {
                'StringValue': str(error),
                'DataType': 'String'
            }}

    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)

        if 'fifo' in queue.url:
            message_sent = queue.send_message(
                MessageBody=json.dumps(message),
                MessageGroupId=message['uuid'],
                MessageDeduplicationId=message['uuid'],
                MessageAttributes=message_attributes
            )
        else:
            message_sent = queue.send_message(
                MessageBody=json.dumps(message),
                MessageAttributes=message_attributes
            )
    except Exception as e:
        logger.exception("Error when sending SQS message for meeting uuid {} to queue {}:{}"
                         .format(message['uuid'], queue_name, e))
        raise

    logger.debug({"Queue": queue_name,
                  "Message sent": message_sent,
                  "FailedReason": error})


def is_valid_mp4(filename):
    s3 = boto3.client('s3')
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': ZOOM_VIDEOS_BUCKET, 'Key': filename}
    )

    command = ['/var/task/ffprobe', '-of', 'json', url]
    if subprocess.call(command) == 1:
        logger.warning("Corrupt MP4, need to retry download from zoom to S3. {}".format(url))
        return False
    else:
        logger.debug("Successfully verified mp4 {}".format(url))
        return True


def filter_and_sort(files):
    """ Sort files by recording start time and filter out multiple MP4 files
    that occur during the same time segment. Choose which MP4 recording_type to
    keep based on priority_list."""
    if not files:
        return None

    non_mp4_files = [file for file in files
                     if file['file_type'].lower() != 'mp4']

    mp4_files = []

    priority_list = [
        'shared_screen_with_speaker_view',
        'shared_screen',
        'active_speaker']
    start_times = set([file['recording_start'] for file in files])
    for start_time in start_times:
        recordings = {file['recording_type']: file for file in files
                      if file['file_type'].lower() == 'mp4'
                      and file['recording_start'] == start_time}

        added_mp4 = False
        for mp4_type in priority_list:
            if mp4_type in recordings:
                logger.debug("Selected MP4 recording type '{}' for start time {}."
                             .format(mp4_type, start_time))
                added_mp4 = True
                mp4_files.append(recordings[mp4_type])
                break

        # make sure at least one mp4 is added, in case the zoom api changes
        if not added_mp4 and recordings:
            logger.warning("No MP4 found with the 'recording_type'"
                           "shared_screen_with_speaker_view, "
                           "shared_screen, or"
                           "active_speaker.")
            mp4_files.append(recordings.values()[0])

    sorted_files = sorted(mp4_files + non_mp4_files,
                          key=itemgetter('recording_start'))

    return sorted_files
