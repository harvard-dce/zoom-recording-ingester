import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import pytest
import jwt
import time
import requests
import requests_mock
from importlib import import_module

webhook = import_module('zoom-webhook')


def test_missing_body(handler):
    res = handler(webhook, {})
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


def test_parse_payload():

    payloads = [
        ('', webhook.BadWebhookData, 'bad query field'),
        ('foo&bar&baz', webhook.BadWebhookData, 'bad query field'),
        ('type=SOME_TYPE', webhook.BadWebhookData,
         "payload missing 'content'"),
        ('type=SOME_TYPE&content=some,not,json,stuff', webhook.BadWebhookData,
         "Failed to parse payload 'content'"),
        ('type=SOME_TYPE&content={"no_uuid_here": 1}', webhook.BadWebhookData,
         "Failed to parse payload 'content'"),
        ('type=SOME_TYPE&content={"uuid": "1234abcd", "host_id": "xyz789"}',
         {'status': 'SOME_TYPE', 'uuid': '1234abcd', 'host_id': 'xyz789'}, None),
        ('uuid=abcd1234', webhook.BadWebhookData,
         "payload missing 'status'"),
        ('uuid=abcd1234&status=SOME_STATUS&host_id=xyz789',
         {'uuid': 'abcd1234', 'status': 'SOME_STATUS', 'host_id': 'xyz789'}, None)
    ]

    for payload, expected, msg in payloads:
        if isinstance(expected, type):
            with pytest.raises(expected) as exc_info:
                webhook.parse_payload(payload)
            if msg is not None:
                assert exc_info.match(msg)
        else:
            assert webhook.parse_payload(payload) == expected


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
        req_mock.get(requests_mock.ANY, status_code=404, json={'code': 3301})
        with pytest.raises(webhook.NoRecordingFound) as excinfo:
            webhook.get_recording_data('abcd-1234')
        assert 'No recording found' in str(excinfo.value)

    # test simple response returns ok
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200, json={'code': 9999, 'foo': 'bar'})
        recording_data = webhook.get_recording_data('abcd-1234')
        assert recording_data['foo'] == 'bar'


def test_verify_status():

    rec_data = [
        ({}, False, None),
        ({'recording_files': []}, False, None),
        ({'recording_files': [{'id': 1, 'status': 'not completed'}]}, False, None),
        ({'recording_files': [{'id': 1, 'status': 'completed'}]},
         webhook.ApiResponseParsingFailure, 'missing a download_url'),
        ({'recording_files': [{'id': 1, 'status': 'completed', 'download_url': 'http://example.com/video.mp4'}]},
         True, None)
    ]

    for data, expected, msg in rec_data:
        if isinstance(expected, type) and expected.__base__ == Exception:
            with pytest.raises(expected) as exc_info:
                webhook.verify_status(data)
        else:
            assert webhook.verify_status(data) == expected


def test_handler_happy_trail(handler, mocker):

    event = {
        'body': 'id=1&uuid=abcd-1234&host_id=foobarbaz&status=RECORDING_MEETING_COMPLETED'
    }

    recording_data = {
        'host_id': '',
        'recording_files': [
            {
                'id': '1234',
                'status': 'completed',
                'download_url': 'http://example.edu/foo.mp4',
            }
        ]
    }

    host_data = {
        'host_name': "",
        'host_email': ""
    }

    mock_get_recording_data = mocker.patch.object(
        webhook,
        'get_recording_data',
        return_value=recording_data
    )

    mocker.patch.object(
        webhook,
        'get_host_data',
        return_value=host_data
    )
    mocker.patch.object(webhook, 'save_to_dynamodb')

    resp = handler(webhook, event)

    mock_get_recording_data.assert_called_once_with("abcd-1234")
    assert resp['statusCode'] == 200


def test_api_lookup_too_many_retries(handler, mocker):

    event = {
        'body': 'id=1&uuid=abcd-1234&host_id=foobarbaz&status=RECORDING_MEETING_COMPLETED'
    }

    mock_get_recording_data = mocker.patch.object(webhook, 'get_recording_data')
    mocker.patch.object(webhook, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    # fail > retry times
    mock_get_recording_data.side_effect = [
        webhook.MeetingLookupFailure()
        for i in range(webhook.MEETING_LOOKUP_RETRIES + 1)
    ]

    resp = handler(webhook, event)

    assert mock_get_recording_data.call_count == 3
    assert resp['statusCode'] == 400
    assert 'retries exhausted' in resp['body']


def test_api_lookup_retries(handler, mocker):

    event = {
        'body': 'id=1&uuid=abcd-1234&host_id=foobarbaz&status=RECORDING_MEETING_COMPLETED'
    }

    mock_get_recording_data = mocker.patch.object(webhook, 'get_recording_data')
    mocker.patch.object(webhook, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    mock_get_recording_data.side_effect = [
        webhook.MeetingLookupFailure(),
        webhook.MeetingLookupFailure(),
        {'recording_files': []}
    ]

    resp = handler(webhook, event)

    assert mock_get_recording_data.call_count == 3
    assert resp['statusCode'] == 204
    assert resp['body'] == ''



