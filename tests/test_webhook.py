
import site
from os.path import dirname
site.addsitedir(dirname(dirname(__file__)))

import pytest
import jwt
import json
import time
import requests
import requests_mock
from unittest.mock import Mock, patch
from importlib import import_module
from botocore.exceptions import ClientError
from moto import mock_dynamodb

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


def test_get_recording_data(mocker):

    mocker.patch.object(webhook, 'gen_token', return_value=b'foobarbaz')

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
        ({}, webhook.ApiResponseParsingFailure()),
        ({'recording_files': []}, []),
        ({'recording_files': [{'foo': 'bar'}]}, webhook.ApiResponseParsingFailure()),
        ({'recording_files': [{'file_type': 'jpg'}]}, []),
        ({'recording_files': [{'file_type': 'mp4', 'status': 'idk'}]}, []),
        ({'recording_files': [
            {'file_type': 'mp4', 'status': 'completed'}]},
            webhook.ApiResponseParsingFailure()
        ),
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
        if isinstance(expected, Exception):
            with pytest.raises(expected.__class__):
                webhook.generate_records(data)
        else:
            assert webhook.generate_records(data) == expected


def test_send_to_dynamodb():

    mock_table = Mock()
    webhook.send_to_dynamodb({'DownloadUrl': 'http://example.edu/video.mp4'}, mock_table)
    mock_table.put_item.assert_called_once_with(
        Item={'DownloadUrl': 'http://example.edu/video.mp4'},
        ConditionExpression="attribute_not_exists(DownloadUrl)"
    )

    client_error = ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, None)
    mock_table.put_item.reset()
    mock_table.put_item.side_effect = client_error
    webhook.send_to_dynamodb({'DownloadUrl': 'http://example.edu/video.mp4'}, mock_table)

    mock_table.put_item.reset()
    mock_table.put_item.side_effect = Exception()
    with pytest.raises(Exception):
        webhook.send_to_dynamodb({'DownloadUrl': 'http://example.edu/video.mp4'}, mock_table)


def test_handler_happy_trail(mocker):

    event = {
        "type" : "RECORDING_MEETING_COMPLETED",
        "content": {
            "uuid": "abcd-1234",
            "host_id": "foobarbaz",
            "id": 12345
        }
    }

    recording_data = {
        'recording_files': [
            {
                'file_type': 'MP4',
                'status': 'completed',
                'download_url': 'http://example.edu/foo.mp4',
                'play_url': 'http://example.edu/play/foo',
                'recording_start': '2017-01-01 00:00:00',
                'recording_end': '2017-01-01 00:00:00'
            }
        ]
    }

    mock_get_recording_data = mocker.patch.object(
        webhook,
        'get_recording_data',
        return_value=recording_data
    )
    mocker.patch.object(webhook, 'send_to_dynamodb')

    # this mocking of dynamo is just precautionary since we're mocking the
    # send_to_dynamodb function as well
    with mock_dynamodb():
        resp = webhook.handler({'body': json.dumps(event)}, None)

    mock_get_recording_data.assert_called_once_with("abcd-1234")
    assert resp['statusCode'] == 200


def test_api_lookup_too_many_retries(mocker):
    event = {
        "type" : "RECORDING_MEETING_COMPLETED",
        "content": {
            "uuid": "abcd-1234",
            "host_id": "foobarbaz",
            "id": 12345
        }
    }

    mock_get_recording_data = mocker.patch.object(webhook, 'get_recording_data')
    mocker.patch.object(webhook, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    # fail > retry times
    mock_get_recording_data.side_effect = [
        webhook.MeetingLookupFailure()
            for i in range(webhook.MEETING_LOOKUP_RETRIES + 1)
    ]

    resp = webhook.handler({'body': json.dumps(event)}, None)
    assert mock_get_recording_data.call_count == 3
    assert resp['statusCode'] == 400
    assert 'retries exhausted' in resp['body']


def test_api_lookup_retries(mocker):
    event = {
        "type" : "RECORDING_MEETING_COMPLETED",
        "content": {
            "uuid": "abcd-1234",
            "host_id": "foobarbaz",
            "id": 12345
        }
    }

    mock_get_recording_data = mocker.patch.object(webhook, 'get_recording_data')
    mocker.patch.object(webhook, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    mock_get_recording_data.side_effect = [
        webhook.MeetingLookupFailure(),
        webhook.MeetingLookupFailure(),
        {'recording_files': []}
    ]

    resp = webhook.handler({'body': json.dumps(event)}, None)
    assert mock_get_recording_data.call_count == 3
    assert resp['statusCode'] == 400
    assert 'No recordings' in resp['body']

