import requests
import boto3
from os import getenv as env
from bs4 import BeautifulSoup
from bs4 import SoupStrainer

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
MIN_CHUNK_SIZE = 5242880


def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    print(str(event))

    if len(event['Records']) > 1:
        print("Should only receive one record at a time.")
        return

    record = event['Records'][0]

    if record['eventName'] != "INSERT":
        print("No action on", record['eventName'])
        return {
            'statusCode': 204,
            'headers': {},
            'body': ""
        }

    metadata = {key: list(val.values())[0] for key, val in record['dynamodb']['Keys'].items()}
    metadata.update({field: list(val.values())[0] for field, val in record['dynamodb']['NewImage'].items()})

    filename = "%s-%s.%s" % (metadata['meeting_series_id'],
                             metadata['recording_start'],
                             metadata['file_type'].lower())

    video_url = retrieve_url(metadata['play_url'])

    if video_url is None:
        return {
            'statusCode': 404,
            'header': {},
            'body': ""
        }

    stream_file_to_s3(video_url, filename, metadata)

    return {
        'statusCode': 200,
        'header': {},
        'body': ""
    }


def retrieve_url(play_url):
    r = requests.get(play_url)
    r.raise_for_status()

    only_source_tags = SoupStrainer("source", type="video/mp4")

    source = BeautifulSoup(r.content, "html.parser", parse_only=only_source_tags)

    link = source.find("source")['src']

    return link


def stream_file_to_s3(download_url, filename, metadata):

    r = requests.get(download_url, stream=True)
    r.raise_for_status()

    print(r.status_code)

    s3 = boto3.client('s3')
    part_info = {'Parts': []}

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