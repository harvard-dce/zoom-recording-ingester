import requests
import boto3
from os import getenv as env

ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')

def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """
    print(str(event))

    # 1. extract the download url from the dynamodb event

    # 2. create an s3 path for the video file and associated metadata

    # 3. stream the download directly to s3 (no temporary local file

    # 4. write the video metadata to an object alongside the video file

    return {
        'statusCode': 200
    }
