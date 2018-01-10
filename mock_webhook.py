import requests
import time
import requests
import jwt
import datetime
from dotenv import load_dotenv
from os.path import join, dirname
import click

load_dotenv(join(dirname(__file__), '.env'))


def gen_token(key, secret, seconds_valid=60):
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"iss": key, "exp": int(time.time() + seconds_valid)}
    return jwt.encode(payload, secret, headers=header)


def get_meetings(key, secret, date=datetime.date.today()):
    page_size = 300
    mtg_type = "past"

    url = "https://api.zoom.us/v2/metrics/meetings/"
    url += "?page_size=%s&type=%s&from=%s&to=%s" % (page_size, mtg_type, date, date)

    token = gen_token(key, secret)
    r = requests.get(url, headers={"Authorization": "Bearer %s" % token.decode()})
    r.raise_for_status()
    response = r.json()
    meetings = response['meetings']

    while 'next_page_token' in response and response['next_page_token'].strip() is True:
        token = gen_token(key, secret)
        r = requests.get(url + "&next_page_token=" + response['next_page_token'],
                         headers={"Authorization": "Bearer %s" % token.decode()})
        r.raise_for_status()
        meetings.extend(r.json()['meetings'])
        time.sleep(60)

    time.sleep(1)

    return meetings


@click.command()
@click.option('--date', default=datetime.datetime.today(), help='Date of recordings. YYYY-MM-DD')
@click.option('--max-recordings', default=1000, help='Maximum number of recordings to process.')
@click.option('--current-format', default=True, help='Current or standard webhook format.')
@click.option("--key", envvar="ZOOM_KEY",
              help="zoom api key; defaults to $ZOOM_KEY")
@click.option("--secret", envvar="ZOOM_SECRET",
              help="zoom api secret; defaults to $ZOOM_SECRET")
@click.option("--endpoint", envvar="AWS_API_ENDPOINT",
              help="AWS API Gateway endpoint defaults to $AWS_API_ENDPOINT")
@click.option("--endpoint-key", envvar="AWS_API_KEY",
              help="AWS API Gateway key defaults to $AWS_API_KEY")
def send_notifications(date, max_recordings, key, secret,
                       endpoint, endpoint_key, current_format):

    num_recordings = 0

    if current_format:
        template = {
            "type": "RECORDING_MEETING_COMPLETED",
            "content": {
                "uuid": "test123",
                "host_id": "bar",
                "id": 12345
            }
        }
    else:
        template = {
            "status": "RECORDING_MEETING_COMPLETED",
            "uuid": "test123",
            "host_id": "bar",
            "id": 12345
        }

    meeting_uuids = [m['uuid'] for m in get_meetings(key, secret, date=date)]

    for uuid in meeting_uuids:
        if current_format:
            template['content']['uuid'] = uuid
        else:
            template['uuid'] = uuid
        send_data = requests.post(endpoint,
                                  headers={'x-api-key': endpoint_key,
                                           'content-type': 'application/json'},
                                  json=template)

        send_data.raise_for_status()
        print(send_data.status_code)
        if send_data.status_code == 200:
            num_recordings += 1
            if num_recordings >= max_recordings:
                break
        time.sleep(1)


if __name__ == '__main__':
    send_notifications()

