import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import pytest
from importlib import import_module
from freezegun import freeze_time
from pytz import timezone
from datetime import datetime
import os
LOCAL_TIME_ZONE = os.getenv('LOCAL_TIME_ZONE')

webhook = import_module('zoom-webhook')

FROZEN_TIME = datetime.strftime(
                datetime(2018, 1, 20, 3, 44, 00, 000000).astimezone(timezone(LOCAL_TIME_ZONE)), '%Y-%m-%dT%H:%M:%SZ')


def test_missing_body(handler):
    res = handler(webhook, {})
    assert res['statusCode'] == 400
    assert res['body'] == 'bad data: no body in event'


def test_started_event(handler):
    event = {
        'body': 'type=STARTED&content=%7B%22uuid%22%3A%20%22abcd-1234%22%2C%20%22host_id%22%3A%201%7D'
    }

    res = handler(webhook, event)
    assert res['statusCode'] == 204


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
         "Unrecognized payload format."),
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


@freeze_time(FROZEN_TIME)
def test_v2_webhook(handler, mocker):

    recording_event = {'body': "{\"payload\":{\"meeting\": {\"uuid\":\"/abc==\", \"host_id\":\"host123\"}},"
                               "\"event\":\"recording_completed\"}"}

    other_event_type = {'body': "{\"payload\":{\"meeting\": {\"uuid\":\"/abc==\", \"host_id\":\"host123\"}},"
                                "\"event\":\"other_event_type\"}"}

    bad_event = {'body': "{,"}

    mock_sqs_send = mocker.patch.object(webhook, 'send_sqs_message')

    resp = handler(webhook, recording_event)
    mock_sqs_send.assert_called_once_with(
        {'uuid': '/abc==', 'correlation_id': '12345-abcde', 'host_id': 'host123', 'received_time': FROZEN_TIME}
    )
    assert resp['statusCode'] == 200

    resp = handler(webhook, other_event_type)
    assert resp['statusCode'] == 204

    resp = handler(webhook, bad_event)
    assert resp['statusCode'] == 400


@freeze_time(FROZEN_TIME)
def test_handler_happy_trail(handler, mocker):

    event = {
        'body': 'type=RECORDING_MEETING_COMPLETED&content=%7B%22uuid%22%3A%20%22abcd-1234%22%2C%20%22host_id%22%3A%201%7D'
    }

    mock_sqs_send = mocker.patch.object(webhook, 'send_sqs_message')
    resp = handler(webhook, event)

    expected = {
        'uuid': 'abcd-1234',
        'correlation_id': '12345-abcde',
        'host_id': 1,
        'received_time': FROZEN_TIME
    }

    mock_sqs_send.assert_called_once_with(expected)
    assert resp['statusCode'] == 200



