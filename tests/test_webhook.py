
import site
from os.path import dirname
site.addsitedir(dirname(dirname(__file__)))

import pytest
import jwt
import time
import requests
import requests_mock
from importlib import import_module
from moto import mock_dynamodb


@pytest.fixture(autouse=True)
def moto_boto():
    mock = mock_dynamodb()
    mock.start()
    yield mock
    mock.stop()


webhook = import_module('functions.zoom-webhook', 'functions')


def test_missing_body():
    res = webhook.handler({}, None)
    assert res['statusCode'] == 400
    assert res['body'] == 'bad data: no body in event'


@pytest.mark.parametrize("key,secret,seconds_valid", [
    ('foo', 'bar', 10),
    ('abcd-1234', 'my-secret-key', 60),
    ('23kljh4jh3jkh_asd008', 'asdlkufh9080a9sdufkjn80989sdf', 1000)
])
def test_gen_token(key, secret, seconds_valid):
    token = webhook.gen_token(key, secret, seconds_valid=seconds_valid)
    payload = jwt.decode(token, secret, algorithms=['HS256'])
    assert payload['iss'] == key

    # should be within a second
    now = int(time.time())
    assert payload['exp'] - (now + seconds_valid) in [0, -1]


def test_get_meeting_uuid():

    payloads = [
        {'type': 'RECORDING_MEETING_COMPLETED', 'content': {'uuid': 'abcd-1234'}},
        {'type': 'blah'},
        {'status': 'RECORDING_MEETING_COMPLETED', 'uuid': 'abcd-1234'},
        {'status': 'blerg'}
    ]

    assert webhook.get_meeting_uuid(payloads[0]) == 'abcd-1234'

    with pytest.raises(webhook.BadWebhookData):
        webhook.get_meeting_uuid(payloads[1])

    assert webhook.get_meeting_uuid(payloads[2]) == 'abcd-1234'

    with pytest.raises(webhook.BadWebhookData):
        webhook.get_meeting_uuid(payloads[3])


def test_get_recording_data(monkeypatch):

    def mock_gen_token(**kwargs):
        return b'foobarbaz'

    monkeypatch.setattr(webhook, 'gen_token', mock_gen_token)

    # test that auth token gets in the headers properly
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200, json={})
        recording_data = webhook.get_recording_data('abcd-1234')
        assert recording_data == {}
        assert 'Authorization' in req_mock.last_request.headers
        assert req_mock.last_request.headers['Authorization'] == 'Bearer foobarbaz'

    # test connection problem
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, exc=requests.exceptions.ConnectTimeout)
        with pytest.raises(webhook.MeetingLookupFailure) as excinfo:
            webhook.get_recording_data('abcd-1234')
        assert 'ConnectTimeout' in str(excinfo.value)

    # test api error
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=400, text='bad request')
        with pytest.raises(webhook.MeetingLookupFailure) as excinfo:
            webhook.get_recording_data('abcd-1234')
        assert 'bad request' in str(excinfo.value)

    # test 'no recordings'
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200, json={'code': 3301})
        with pytest.raises(webhook.MeetingLookupFailure) as excinfo:
            webhook.get_recording_data('abcd-1234')
        assert 'No recording found' in str(excinfo.value)

    # test simple response returns ok
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200, json={'code': 9999, 'foo': 'bar'})
        recording_data = webhook.get_recording_data('abcd-1234')
        assert recording_data['foo'] == 'bar'


def test_generate_records():

    rec_data = [
        ({}, []),
        ({'recording_files': []}, []),
        ({'recording_files': [{'file_type': 'jpg'}]}, []),
        ({'recording_files': [{'file_type': 'mp4', 'status': 'idk'}]}, []),
        ({'recording_files': [{'file_type': 'mp4', 'status': 'completed'}]}, []),
        ({'recording_files': [
            {
                'file_type': 'MP4',
                'status': 'completed',
                'download_url': 'http://example.edu/foo.mp4',
                'play_url': 'http://example.edu/play/foo',
                'recording_start': '2017-01-01 00:00:00',
                'recording_end': '2017-01-01 00:00:00'
            }]},
            [

                {
                    'file_type': 'MP4',
                    'DownloadUrl': 'http://example.edu/foo.mp4',
                    'play_url': 'http://example.edu/play/foo',
                    'recording_start': '2017-01-01 00:00:00',
                    'recording_end': '2017-01-01 00:00:00'
                }
            ]
        ),
    ]

    for data, expected in rec_data:
        assert webhook.generate_records(data) == expected

