import boto3
import json
import time
import requests
import re
from os import getenv as env
from hashlib import md5
from operator import itemgetter
from common import setup_logging, gen_token
import subprocess
from pytz import timezone
from datetime import datetime
from urllib.parse import quote

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
DEFAULT_SERIES_ID = env("DEFAULT_SERIES_ID")
CLASS_SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")

# Recordings that happen within BUFFER_MINUTES a courses schedule
# start time will be captured
BUFFER_MINUTES = 30
# Ignore recordings that are less than MIN_DURATION (in minutes)
MINIMUM_DURATION = 2

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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

    ignore_schedule = event.get('ignore_schedule', False)
    override_series_id = event.get('override_series_id')

    sqs = boto3.resource('sqs')
    download_queue = sqs.get_queue_by_name(QueueName=DOWNLOAD_QUEUE_NAME)

    # find a message from the downloads queue
    try:
        messages = download_queue.receive_messages(
            MaxNumberOfMessages=1,
            VisibilityTimeout=700
        )
        download_message = messages[0]
        logger.info({'queue message': download_message})
    except IndexError:
        logger.info("No download queue messages available")
        return

    download_data = json.loads(download_message.body)
    download_data['ignore_schedule'] = ignore_schedule
    download_data['override_series_id'] = override_series_id
    meeting_info = meeting_metadata(download_data['uuid'])
    download_data.update(meeting_info)

    logger.info(download_data)

    # download the recordings to s3
    try:
        upload_message = process_download(download_data)
    except PermanentDownloadError as e:
        logger.error(e)
        send_to_sqs(download_data, DEADLETTER_QUEUE_NAME, error=e)
        download_message.delete()
        raise
    except Exception as e:
        logger.exception(e)
        raise

    # send a message to the opencast uploader
    if upload_message:
        send_to_sqs(upload_message, UPLOAD_QUEUE_NAME)

    # remove processed message from downloads queue
    download_message.delete()


def process_download(download_data):
    download = Download(download_data)
    return download.download()


class Download:

    def __init__(self, data):
        self.data = data

    @property
    def uuid(self):
        return self.data['uuid']

    @property
    def zoom_series_id(self):
        return self.data['id']

    @property
    def start_time(self):
        if 'start_time' in self.data:
            return self.data['start_time']
        else:
            return None

    @property
    def end_time(self):
        if 'end_time' in self.data:
            return self.data['end_time']
        else:
            return None

    @property
    def duration(self):
        """
        Either returns a duration in the form HH:MM:SS or None.
        """
        duration = self.data['duration']
        # regular expressions
        HHMMSS = r"\d{2}:\d{2}:\d{2}"
        MMSS = r"\d{2}:\d{2}"

        if duration:
            if re.match(MMSS, duration) and len(duration) == len("MM:SS"):
                duration = "00:" + duration

            # validate the duration format is a valid time format
            try:
                datetime.strptime(duration, "%H:%M:%S")
                return duration
            except ValueError:
                duration = None

        if not duration and self.start_time and self.end_time:
            try:
                s = datetime.strptime(self.start_time, TIMESTAMP_FORMAT)
                e = datetime.strptime(self.end_time, TIMESTAMP_FORMAT)
            except ValueError:
                return None
            delta = e - s
            hours, remainder = divmod(delta.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            duration = "{:02d}:{:02d}:{:02}".format(hours, minutes, seconds)
            return duration

        return None

    @property
    def created(self):
        utc = datetime.strptime(
            self.start_time, TIMESTAMP_FORMAT) \
            .replace(tzinfo=timezone('UTC'))
        return utc

    @property
    def shorter_than_minimum_duration(self):
        """
        Interpret duration of meeting and determine whether duration is
        shorter than the MINIMUM_DURATION in minutes.
        """
        if self.duration:
            duration_in_mins = time.strptime(self.duration, "%H:%M:%S").tm_min
            return duration_in_mins < MINIMUM_DURATION
        else:
            return False

    @property
    def recording_data(self):
        if not hasattr(self, '_recording_data'):
            endpoint_url = '{}meetings/{}/recordings'.format(
                                                        ZOOM_API_BASE_URL,
                                                        url_safe(self.uuid))
            raw_data = get_api_data(endpoint_url)
            clean_data = remove_incomplete_metadata(raw_data)
            verify_recording_status(clean_data)
            self._recording_data = clean_data

        return self._recording_data

    def series_id_from_schedule(self):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(CLASS_SCHEDULE_TABLE)

        r = table.get_item(
            Key={"zoom_series_id": str(self.zoom_series_id)}
        )

        if 'Item' not in r:
            return None
        else:
            schedule = r['Item']
            logger.info(schedule)

        zoom_time = self.created.astimezone(timezone(LOCAL_TIME_ZONE))
        weekdays = {'M': 'Mondays',
                    'T': 'Tuesdays',
                    'W': 'Wednesdays',
                    'R': 'Thursdays',
                    'F': 'Fridays'}
        if zoom_time.weekday() > 4:
            logger.debug("Meeting occurred on a weekend.")
            return None
        letter = list(weekdays.keys())[zoom_time.weekday()]
        if letter not in schedule['Days']:
            logger.debug("No opencast recording scheduled on {}."
                         .format(weekdays[letter]))
            return None

        for t in schedule['Time']:
            scheduled_time = datetime.strptime(t, '%H:%M')
            timedelta = abs(zoom_time -
                            zoom_time.replace(hour=scheduled_time.hour,
                                              minute=scheduled_time.minute)
                            ).total_seconds()
            if timedelta < (BUFFER_MINUTES * 60):
                return schedule['opencast_series_id']

        logger.debug("Meeting started more than {} minutes before or after "
                     "opencast scheduled start time."
                     .format(BUFFER_MINUTES))

        return None

    @property
    def override_series_id(self):
        return self.data['override_series_id']

    @property
    def opencast_series_id(self):

        if not hasattr(self, '_oc_series_id'):

            self._oc_series_id = None

            if self.override_series_id:
                self._oc_series_id = self.override_series_id
                logger.info("Using override series id '{}'"
                            .format(self._oc_series_id))
                return self._oc_series_id

            if self.data['ignore_schedule']:
                logger.info("Ignoring schedule")
            else:
                self._oc_series_id = self.series_id_from_schedule()
                if self._oc_series_id is not None:
                    logger.info("Matched with opencast series '{}'!"
                                .format(self._oc_series_id))
                    return self._oc_series_id

            if DEFAULT_SERIES_ID is not None and DEFAULT_SERIES_ID != "None":
                logger.info("Using default series id {}"
                            .format(DEFAULT_SERIES_ID))
                self._oc_series_id = DEFAULT_SERIES_ID

        return self._oc_series_id

    @property
    def chronological_files(self):
        return filter_and_sort(self.recording_data['recording_files'])

    @property
    def upload_message(self):
        upload_message = {
            "uuid": self.uuid,
            "zoom_series_id": self.zoom_series_id,
            "opencast_series_id": self.opencast_series_id,
            "host_name": self.data['host'],
            "topic": self.data['topic'],
            "created": datetime.strftime(self.created, TIMESTAMP_FORMAT),
            "webhook_received_time": self.data['received_time'],
            "correlation_id": self.data['correlation_id']
        }
        return upload_message

    def download(self):

        if self.shorter_than_minimum_duration:
            logger.info("Recording duration shorter than minimum {} minutes"
                        .format(MINIMUM_DURATION))
            return None

        if not self.opencast_series_id:
            logger.info("No opencast series match found")
            return None

        if self.recording_data:
            logger.info({'recording_data': self.recording_data})
        else:
            raise PermanentDownloadError("No recording data found!")

        logger.info(self.recording_data)

        if not self.chronological_files:
            raise PermanentDownloadError("No files available to download.")
        logger.info("downloading {} files"
                    .format(len(self.chronological_files)))

        track_sequence = 1
        prev_file = None

        for file in self.chronological_files:
            if next_track_sequence(prev_file, file):
                track_sequence += 1

            logger.debug("file {} is track sequence {}"
                         .format(file, track_sequence))
            prev_file = file

            stream_file_to_s3(file, self.uuid, track_sequence)

        return self.upload_message


"""
Helper functions
"""


def meeting_metadata(uuid):
    """
    Get metadata about meeting. Metadata includes zoom meeting id,
    host_name, host_email, topic, and start_time.
    """

    for meeting_type in ['past', 'pastOne', 'live']:
        try:
            endpoint_url = ZOOM_API_BASE_URL + 'metrics/meetings/{}?type={}' \
                .format(url_safe(uuid), meeting_type)
            meeting_info = get_api_data(endpoint_url)
            logger.info({'meeting_info': meeting_info})

            return meeting_info
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                continue
            else:
                raise

    raise PermanentDownloadError(
        "No meeting metadata found for meeting uuid {}".format(uuid))


def url_safe(uuid):
    """
    Zoom API currently only accepts url unsafe characters as path parameters
    if they are url double encoded.
    """
    if "/" in uuid:
        return quote(quote(uuid, safe=''), safe='')
    else:
        return uuid


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


def remove_incomplete_metadata(recording_data):
    """
    Throw away any non-video file metadata objects that don't include the
    required fields.
    """

    required_fields = {
        'id',
        'meeting_id',
        'recording_start',
        'recording_end',
        'file_type',
        'download_url',
        'status',
        'recording_type'
    }
    for file in recording_data['recording_files']:
        if not required_fields.issubset(set(file.keys())):
            if file['file_type'].lower() == 'mp4':
                raise PermanentDownloadError(
                    "MP4 file missing required metadata. {}".format(file)
                )
            else:
                logger.debug("Removing file from recording_data "
                             "(incomplete metadata): {}".format(file))
                recording_data['recording_files'].remove(file)

    return recording_data


def next_track_sequence(prev_file, file):

    if prev_file is None:
        return False

    logger.debug({'start': file['recording_start'],
                  'end': file['recording_end']})

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
    mpu = s3.create_multipart_upload(
                Bucket=ZOOM_VIDEOS_BUCKET,
                Key=filename,
                Metadata=metadata)

    try:
        for part_number, chunk in enumerate(stream.iter_content(chunk_size=MIN_CHUNK_SIZE), 1):
            part = s3.upload_part(Body=chunk, Bucket=ZOOM_VIDEOS_BUCKET,
                                  Key=filename, PartNumber=part_number,
                                  UploadId=mpu['UploadId'])

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

    url = "{}?zak={}".format(download_url, admin_token)
    r = requests.get(url, stream=True)

    return r, zoom_name


def get_admin_token():
    headers = {"Authorization": "Bearer %s" % gen_token().decode()}

    # get admin user id from admin email
    url = "{}users/{}".format(ZOOM_API_BASE_URL, ZOOM_ADMIN_EMAIL)
    r = requests.get(url, headers=headers)
    admin_id = r.json()['id']

    # get admin level zak token from admin id
    url = "{}users/{}/token?type=zak".format(ZOOM_API_BASE_URL, admin_id)
    r = requests.get(url, headers=headers)
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
        logger.exception("Error when sending SQS message for meeting uuid {} "
                         "to queue {}:{}"
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
        logger.warning("Corrupt MP4, need to retry download from zoom to S3."
                       " {}".format(url))
        return False
    else:
        logger.debug("Successfully verified mp4 {}".format(url))
        return True


def filter_and_sort(files):
    """
    Sort files by recording start time and filter out multiple MP4 files
    that occur during the same time segment. Choose which MP4 recording_type to
    keep based on priority_list.
    """
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
                logger.debug("Selected MP4 recording type '{}' "
                             "for start time {}.".format(mp4_type, start_time))
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
