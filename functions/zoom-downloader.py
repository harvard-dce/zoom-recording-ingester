import requests
import boto3
import json
from os import getenv as env
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from hashlib import md5
from botocore.exceptions import ClientError

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
UPLOAD_QUEUE_NAME = env('UPLOAD_QUEUE_NAME')
MIN_CHUNK_SIZE = 5242880
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


class RecordingSegmentsOverlap(Exception):
    pass


class FileNameNotFound(Exception):
    pass


def resp_204(msg):
    print("http 204 response: {}".format(msg))
    return {
        'statusCode': 204,
        'headers': {},
        'body': ""
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
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    print(str(event))

    if 'Records' not in event:
        return resp_400("No records in event.")

    if len(event['Records']) > 1:
        return resp_400("DynamoDB stream should be set to BatchSize: 1")

    event_type = event['Records'][0]['eventName']
    if event_type != "INSERT":
        return resp_204("No action on " + event_type)

    record = json.loads(event['Records'][0]['dynamodb']['NewImage']['recording_data']['S'])

    chronological_files = sorted(record['recording_files'], key=lambda k: k['recording_start'])

    metadata = record.copy()
    track_sequence = 1
    prev_file = None

    for file in chronological_files:
        try:
            if next_track_sequence(prev_file, file):
                track_sequence += 1
        except RecordingSegmentsOverlap:
            return resp_400("Segment start/end times overlap for files:\n{}\n{}".format(prev_file, file))

        metadata['track_sequence'] = str(track_sequence)
        prev_file = file

        stream_file_to_s3(file, metadata.copy())

    send_sqs_message(record)

    return {
        'statusCode': 200,
        'header': {},
        'body': ""
    }


def stream_file_to_s3(file, metadata):

    if file['file_type'].lower() in ["mp4", "m4a", "chat"]:
        r, zoom_name = get_connection_using_play_url(file)

        if 'gallery' in zoom_name.lower():
            metadata['view'] = "gallery"
        elif file['file_type'].lower() == "mp4":
            metadata['view'] = "speaker"
    else:
        try:
            r, zoom_name = get_connection_using_download_url(file)
        except FileNameNotFound:
            raise

    filename = create_filename(file['id'], file['meeting_id'], zoom_name)

    try:
        if key_exists(filename):
            print("Skip stream to S3. Key {} already in bucket.".format(filename))
            return
    except Exception as e:
        print("Received error when searching for s3 bucket key: {}".format(e))
        raise

    part_info = {'Parts': []}
    mpu = s3.create_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                     Metadata={key: str(val) for key, val in metadata.items()})

    try:
        for i, chunk in enumerate(r.iter_content(chunk_size=MIN_CHUNK_SIZE)):
            partnumber = i + 1
            part = s3.upload_part(Body=chunk, Bucket=ZOOM_VIDEOS_BUCKET,
                                  Key=filename, PartNumber=partnumber, UploadId=mpu['UploadId'])

            part_info['Parts'].append({
                'PartNumber': partnumber,
                'ETag': part['ETag']
            })

        s3.complete_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                     UploadId=mpu['UploadId'],
                                     MultipartUpload=part_info)
        print("Completed multipart upload {}.".format(filename))
    except Exception as e:
        print(e)
        s3.abort_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                  UploadId=mpu['UploadId'])


def send_sqs_message(record):
    print("SQS sending start...")
    message = {
        "uuid": record['uuid'],
        "meeting_number": record['meeting_number'],
        "host_name": record['host_name'],
        "topic": record['topic'],
        "start_time": record['start_time'],
        "recording_count": record['recording_count']
    }
    print("Sending SQS message: {}".format(message))

    upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

    message_sent = upload_queue.send_message(
        MessageBody=json.dumps(message),
        MessageGroupId="uploads",
        MessageDeduplicationId=message['uuid']
    )
    print("Message sent: {}".format(message_sent))


def next_track_sequence(prev_file, file):

    if prev_file is None:
        return False

    if file['recording_start'] == prev_file['recording_start']:
        if file['recording_end'] == prev_file['recording_end']:
            return False

    if file['recording_start'] >= prev_file['recording_end']:
        return True

    raise RecordingSegmentsOverlap


def retrieve_url_from_play_page(play_url):
    r = requests.get(play_url)
    r.raise_for_status()

    only_source_tags = SoupStrainer("source", type="video/mp4")

    source = BeautifulSoup(r.content, "html.parser", parse_only=only_source_tags)

    source_object = source.find("source")

    if source_object is None:
        return None

    link = source_object['src']

    return link


def get_connection_using_play_url(file):
    file_url = retrieve_url_from_play_page(file['play_url'])
    zoom_name = file_url.split("?")[0].split("/")[-1]

    r = requests.get(file_url, stream=True)
    r.raise_for_status()

    return r, zoom_name


def get_connection_using_download_url(file):
    r = requests.get(file['download_url'], stream=True)
    r.raise_for_status()

    try:
        zoom_name = r.headers['Content-Disposition'].split("=")[-1]
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
        return True
    except ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            print("Key {} not yet in bucket.".format(filename))
            return False
        raise
