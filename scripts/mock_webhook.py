import requests
import time
import requests
import jwt
import json
import datetime
from dotenv import load_dotenv
from os.path import join, dirname
import click

load_dotenv(join(dirname(dirname(__file__)), '.env'))


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
@click.option("--key", envvar="ZOOM_API_KEY",
              help="zoom api key; defaults to $ZOOM_API_KEY")
@click.option("--secret", envvar="ZOOM_API_SECRET",
              help="zoom api secret; defaults to $ZOOM_API_SECRET")
@click.option("--endpoint", envvar="AWS_API_ENDPOINT",
              help="AWS API Gateway endpoint defaults to $AWS_API_ENDPOINT")
@click.option("--endpoint-key", envvar="AWS_API_KEY",
              help="AWS API Gateway key defaults to $AWS_API_KEY")
def send_notifications(date, max_recordings, key, secret,
                       endpoint, endpoint_key, current_format):

    num_recordings = 0

    for meeting in get_meetings(key, secret, date=date):
        uuid = meeting['uuid']
        series_id = meeting['id']

        r = requests.get("https://api.zoom.us/v2/meetings/%s" % series_id,
                         headers={"Authorization": "Bearer %s" % gen_token(key, secret).decode()})
        r.raise_for_status()
        host_id = r.json()['host_id']

        if current_format is True:
            template = {
                "type": "RECORDING_MEETING_COMPLETED",
                "content":
                    json.dumps({
                        "uuid": uuid,
                        "host_id": host_id,
                        "id": 12345
                    })
            }
        else:
            template = {
                "status": "RECORDING_MEETING_COMPLETED",
                "uuid": uuid,
                "host_id": host_id,
                "id": 12345
            }

        try:
            send_data = requests.post(endpoint,
                                      headers={'x-api-key': endpoint_key,
                                               'content-type': 'application/json'},
                                      data=template)

            send_data.raise_for_status()
            print(send_data.status_code)
            if send_data.status_code == 200:
                num_recordings += 1
                if num_recordings >= max_recordings:
                    break
            time.sleep(1)
        except requests.HTTPError as e:
            print("%s %s" % (e.response.status_code, e.response.content))
            break


if __name__ == '__main__':
    send_notifications()

