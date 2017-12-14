import requests
import boto3
from os import getenv as env

DOWNLOAD_URLS_TABLE = env('DOWNLOAD_URLS_TABLE')
ZOOM_API_TOKEN = env('ZOOM_API_TOKEN')

dynamo = boto3.resource('dynamodb')
table = dynamo.Table(DOWNLOAD_URLS_TABLE)

def handler(event, context):
    """
    This function accepts the incoming POST relay from the API Gateway endpoint that
    serves as the Zoom webhook endpoint. The payload from Zoom does not include
    the actual download url so we have to fetch that in a Zoom api call.
    """
    print(str(event))

    # 1. get the payload from event['body']

    # 2. hit the zoom api to get the download url

    # 3. create the dynamodb record

    return {
        'statusCode': 200
    }
