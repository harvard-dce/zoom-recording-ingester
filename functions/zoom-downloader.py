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


def resp_404(msg):
    print("http 404 response: {}".format(msg))
    return {
        'statusCode': 404,
        'header': {},
        'body': msg
    }


def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    print(str(event))
    print("---------------------------------")

    # Make sure records in event
    if 'Records' not in event:
        return resp_400("No records in event.")

    # Should only be receiving records one at a time
    if len(event['Records']) > 1:
        return resp_400("DynamoDB stream should be set to BatchSize: 1")

    # We only care about insert events
    event_type = event['Records'][0]['eventName']
    if event_type != "INSERT":
        return resp_204("No action on " + event_type)

    # Load dictionary of recording data
    record = json.loads(event['Records'][0]['dynamodb']['NewImage']['recording_data']['S'])

    # Sort files chronologically
    chronological_files = sorted(record['recording_files'], key=lambda k: k['recording_start'])

    # Make metadata copy to edit for each file - may not need to do this here if you just
    # pass the record into a method
    metadata = record.copy()
    track_sequence = 1

    for i, file in enumerate(chronological_files):
        print("_____________")
        prev_file = chronological_files[i - 1]

        if i > 0:
            if file['recording_start'] == prev_file['recording_start']:
                if file['recording_end'] != prev_file['recording_end']:
                    return resp_400("Recording end {} and recording end {} do not match for segment {}."
                                    "Segments should match exactly.".format(file['recording_end'],
                                                                            prev_file['recording_end'], i))
            elif file['recording_start'] < prev_file['recording_end']:
                    return resp_400("Recording start {} before recording end {}. "
                                    "Segments cannot overlap.".format(file['recording_start'],
                                                                      prev_file['recording_end']))
            else:
                track_sequence += 1

        if file['file_type'].lower() not in ["mp4", "m4a", "chat"]:
            r = requests.get(file['download_url'], stream=True)
            if 'Content-Disposition' in r.headers:
                zoom_name = r.headers['Content-Disposition'].split("=")[-1]
                print("FILENAME via download:", zoom_name)
            file_url = file['download_url']
        else:
            # extract file url from play page
            file_url = retrieve_url_from_play_page(file['play_url'])

            # the name that zoom gives to this file
            zoom_name = zoom_file_name(file_url)
            print("FILENAME via play:", zoom_name, file['recording_start'])

            # set appropriate metadata for view type and make sure gallery and speaker are the same
            if 'gallery' in zoom_name.lower():
                metadata['view'] = "gallery"
            elif file['file_type'].lower() == "mp4":
                metadata['view'] = "speaker"

        metadata['track_sequence'] = str(track_sequence)
        file_type = ".".join(zoom_name.split(".")[1:]).lower()
        md5_uuid = md5(file['meeting_id'].encode()).hexdigest()
        file_name = "{}/{}.{}".format(md5_uuid, file['id'], file_type)

        if file_type == "mp4":
            print("S3 FILENAME", file_name, metadata['view'], metadata['track_sequence'])
        else:
            print("S3 FILENAME:", file_name, metadata['track_sequence'])

        stream_file_to_s3(file_url, file_name, metadata)

    send_sqs_message(record)

    return {
        'statusCode': 200,
        'header': {},
        'body': ""
    }


def zoom_file_name(file_url):
    return file_url.split("?")[0].split("/")[-1]


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


def stream_file_to_s3(download_url, filename, metadata):

    # check if file already exists in s3
    s3 = boto3.client('s3')

    try:
        s3.head_object(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename)
        print("Key {} already in bucket.".format(filename))
    except ClientError:
        return

    metadata = {key: str(val) for key, val in metadata.items()}

    print("STREAMING FILE:", download_url)
    print("STREAMING NAME:", filename)
    print("STREAMING METADATA:", metadata)
    print(type(download_url), type(filename), type(metadata))

    r = requests.get(download_url, stream=True)
    r.raise_for_status()

    print("STREAM STATUS CODE:", r.status_code)

    part_info = {'Parts': []}

    print("ZOOM BUCKET:", ZOOM_VIDEOS_BUCKET)

    mpu = s3.create_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                     Metadata=metadata)

    try:
        for i, chunk in enumerate(r.iter_content(chunk_size=MIN_CHUNK_SIZE)):
            partnumber = i + 1
            print("Part", partnumber)
            part = s3.upload_part(Body=chunk, Bucket=ZOOM_VIDEOS_BUCKET,
                                  Key=filename, PartNumber=partnumber, UploadId=mpu['UploadId'])

            part_info['Parts'].append({
                'PartNumber': partnumber,
                'ETag': part['ETag']
            })

        s3.complete_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET, Key=filename,
                                     UploadId=mpu['UploadId'],
                                     MultipartUpload=part_info)
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

    sqs = boto3.resource('sqs')
    upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

    message_sent = upload_queue.send_message(
        MessageBody=json.dumps(message),
        MessageGroupId="uploads",
        MessageDeduplicationId=message['uuid']
    )
    print("Message sent: {}".format(message_sent))
