import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import pytest
from importlib import import_module
from freezegun import freeze_time
from pytz import timezone
from datetime import datetime
import os
import copy
import json

LOCAL_TIME_ZONE = os.getenv('LOCAL_TIME_ZONE')
webhook = import_module('zoom-webhook')

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(
    tz.localize(datetime(2018, 1, 20, 3, 44, 00, 000000)),
    '%Y-%m-%dT%H:%M:%SZ')

SAMPLE_NOTIFICATION = {
    "payload": {
        "object": {
            "id": 1,
            "uuid": "abc",
            "host_id": "efg",
            "topic": "Class Section Meeting",
            "start_time": "2020-01-09T19:50:46Z",
            "duration": 10,
            "recording_files": [
                {"id": "123456-789",
                 "recording_start": "2020-01-09T19:50:46Z",
                 "recording_end": "2020-01-09T20:50:46Z",
                 "download_url": "https://zoom.us/rec/play/some-long-id",
                 "file_type": "MP4",
                 "recording_type": "shared_screen_with_speaker_view",
                 "status": "completed"}
            ]

        }
    },
    "event": "recording.completed"
}

MOCK_CORRELATION_ID = "12345-abcde"
MOCK_HOST_NAME = "host name"

# file id renamed for sqs message
files = copy.deepcopy(SAMPLE_NOTIFICATION["payload"]["object"]["recording_files"])
files[0]["recording_id"] = files[0].pop("id")

SAMPLE_SQS_MESSAGE_BODY = {
    "uuid": SAMPLE_NOTIFICATION["payload"]["object"]["uuid"],
    "zoom_series_id": SAMPLE_NOTIFICATION["payload"]["object"]["id"],
    "topic": SAMPLE_NOTIFICATION["payload"]["object"]["topic"],
    "start_time": SAMPLE_NOTIFICATION["payload"]["object"]["start_time"],
    "duration": SAMPLE_NOTIFICATION["payload"]["object"]["duration"],
    "host_name": MOCK_HOST_NAME,
    "recording_files": files,
    "correlation_id": MOCK_CORRELATION_ID,
    "received_time": FROZEN_TIME
}


class MockContext():
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
        self.function_name = "zoom-webhook"


def test_missing_body(handler):
    res = handler(webhook, {})
    assert res['statusCode'] == 400
    assert "no body found" in res['body'].lower()


def test_non_json_body(handler):
    res = handler(webhook, {"body": "abc"})
    assert res["statusCode"] == 400
    assert "not valid json" in res["body"]


def test_missing_payload(handler):
    body = json.dumps({"not a payload": 123})
    res = handler(webhook, {"body": body})
    assert res["statusCode"] == 400
    assert "missing payload" in res["body"].lower()


def test_invalid_payload(handler):
    body = json.dumps({"payload": "invalid payload"})
    res = handler(webhook, {"body": body})
    assert res["statusCode"] == 400
    assert "bad data" in res["body"].lower()


def test_started_event(handler):
    recording_started = copy.deepcopy(SAMPLE_NOTIFICATION)
    recording_started["event"] = "recording.started"
    event = {
        'body': json.dumps(recording_started)
    }

    res = handler(webhook, event)
    assert res['statusCode'] == 204


def test_validate_payload():
    minimum_valid_payload = SAMPLE_NOTIFICATION["payload"]

    # should not raise an exception
    webhook.validate_payload(minimum_valid_payload)

    payload_copy = copy.deepcopy(minimum_valid_payload)

    del payload_copy["object"]["recording_files"][0]["id"]
    missing_file_id = copy.deepcopy(payload_copy)

    del payload_copy["object"]["recording_files"]
    missing_recording_files = copy.deepcopy(payload_copy)

    del payload_copy["object"]
    missing_object_field = payload_copy

    payloads = [
        (missing_file_id, "Missing required file field 'id'"),
        (missing_recording_files,
            "Missing required object field 'recording_files'"),
        (missing_object_field, "Missing required payload field 'object'")
    ]

    for payload, msg in payloads:
        with pytest.raises(webhook.BadWebhookData) as exc_info:
            webhook.validate_payload(payload)
        assert exc_info.match(msg)


@freeze_time(FROZEN_TIME)
def test_handler_happy_trail(handler, mocker):
    event = {
        'body': json.dumps(SAMPLE_NOTIFICATION)
    }
    mocker.patch.object(webhook, 'host_name', return_value=MOCK_HOST_NAME)
    mock_sqs_send = mocker.patch.object(webhook, 'send_sqs_message')
    context = MockContext(aws_request_id=MOCK_CORRELATION_ID)

    resp = handler(webhook, event, context)
    mock_sqs_send.assert_called_once_with(SAMPLE_SQS_MESSAGE_BODY)
    assert resp['statusCode'] == 200


@freeze_time(FROZEN_TIME)
def test_delay_seconds(handler, mocker):
    delay_seconds = 5
    notification = copy.deepcopy(SAMPLE_NOTIFICATION)
    notification["payload"]["delay_seconds"] = delay_seconds
    event = {
        "body": json.dumps(notification)
    }
    mocker.patch.object(webhook, 'host_name', return_value=MOCK_HOST_NAME)
    mock_sqs_send = mocker.patch.object(webhook, 'send_sqs_message')
    context = MockContext(aws_request_id=MOCK_CORRELATION_ID)

    resp = handler(webhook, event, context)
    mock_sqs_send.assert_called_once_with(
        SAMPLE_SQS_MESSAGE_BODY, delay=delay_seconds)
    assert resp['statusCode'] == 200
