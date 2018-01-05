import requests
import boto3
from os import getenv as env

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
MIN_CHUNK_SIZE = 5242880


def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    print(str(event))

    sample_video_url = "http://sample-videos.com/video/mp4/720/big_buck_bunny_720p_10mb.mp4"

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

    stream_file_to_s3(sample_video_url, filename, metadata)

    return {
        'statusCode': 200,
        'header': {},
        'body': ""
    }


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