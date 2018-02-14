import requests
import boto3
import json
from os import getenv as env
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from hashlib import md5
from botocore.exceptions import ClientError

import logging
from common import setup_logging
logger = logging.getLogger()

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
DOWNLOAD_QUEUE_NAME = env('DOWNLOAD_QUEUE_NAME')
UPLOAD_QUEUE_NAME = env('UPLOAD_QUEUE_NAME')
MIN_CHUNK_SIZE = 5242880
s3 = boto3.client('s3')
sqs = boto3.resource('sqs')


class RecordingSegmentsOverlap(Exception):
    pass


class FileNameNotFound(Exception):
    pass


def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    setup_logging(context)
    logger.info(event)

    download_queue = sqs.get_queue_by_name(QueueName=DOWNLOAD_QUEUE_NAME)
    logger.debug("got queue {}".format(str(download_queue)))

    try:
        logger.debug("fetching a message...")
        message = download_queue.receive_messages(MaxNumberOfMessages=1)[0]
        logger.debug({'queue_message': message})
    except IndexError:
        logger.warning("No uploads ready for processing")
        return

    download_data = json.loads(message.body)
    logger.info(download_data)
    recording_data = download_data['recording_data']

    chronological_files = sorted(recording_data['recording_files'], key=lambda k: k['recording_start'])
    logger.info("downloading {} files".format(len(chronological_files)))

    track_sequence = 1
    prev_file = None

    for file in chronological_files:
        if next_track_sequence(prev_file, file):
                track_sequence += 1

        logger.debug("file {} is track sequence {}".format(file, track_sequence))
        prev_file = file

        stream_file_to_s3(file, recording_data['uuid'], track_sequence)

    recording_data['downloader_correlation_id'] = context.aws_request_id
    send_sqs_message(recording_data)

    message.delete()
    logger.info("downloader handler complete")


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


def send_sqs_message(record):
    logger.debug("SQS sending start...")
    message = {
        "uuid": record['uuid'],
        "meeting_number": record['meeting_number'],
        "host_name": record['host_name'],
        "topic": record['topic'],
        "start_time": record['start_time'],
        "recording_count": record['recording_count']
    }
    logger.debug(message)

    try:
        upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

        message_sent = upload_queue.send_message(
            MessageBody=json.dumps(message),
            MessageGroupId="uploads",
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
    logger.info("God download url {}".format(link))

    return link


def get_connection_using_play_url(file):
    file_url = retrieve_url_from_play_page(file['play_url'])
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
