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
TIMESTAMP_FORMAT = os.getenv('TIMESTAMP_FORMAT')
webhook = import_module('zoom-webhook')

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(tz.localize(datetime.now()), TIMESTAMP_FORMAT)


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


def test_missing_event(handler):
    body = json.dumps({"no event type in here": 123})
    res = handler(webhook, {"body": body})
    assert res["statusCode"] == 400
    assert "no event type" in res["body"].lower()


def test_missing_payload(handler):
    body = json.dumps({"not a payload": 123, "event": "recording.completed"})
    res = handler(webhook, {"body": body})
    assert res["statusCode"] == 400
    assert "missing payload" in res["body"].lower()


def test_invalid_payload(handler):
    body = json.dumps({
        "payload": "invalid payload",
        "event": "recording.completed"
    })
    res = handler(webhook, {"body": body})
    assert res["statusCode"] == 400
    assert "bad data" in res["body"].lower()


def test_started_event(handler, webhook_payload):
    recording_started = webhook_payload()
    recording_started["event"] = "recording.started"
    event = {
        'body': json.dumps(recording_started)
    }

    res = handler(webhook, event)
    assert res['statusCode'] == 204


def test_validate_payload(webhook_payload):

    # should not raise an exception
    minimum_valid_payload = webhook_payload()["payload"]
    webhook.validate_payload(minimum_valid_payload)

    missing_file_id = webhook_payload()["payload"]
    del missing_file_id["object"]["recording_files"][0]["id"]

    missing_recording_files = webhook_payload()["payload"]
    del missing_recording_files["object"]["recording_files"]

    missing_object_field = webhook_payload()["payload"]
    del missing_object_field["object"]

    payloads = [
        (missing_file_id, "Missing required file field 'id'"),
        (missing_recording_files,
            "Missing required object field 'recording_files'"),
        (missing_object_field, "Missing required payload field 'object'")
    ]

    for payload, msg in payloads:
        with pytest.raises(webhook.BadWebhookData) as exc_info:
            webhook.validate_payload(payload)
        assert exc_info.match(msg), msg

@freeze_time(FROZEN_TIME)
def test_no_mp4s_validation(webhook_payload):
    payload = webhook_payload()["payload"]
    payload["object"]["recording_files"][0]["file_type"] = "foo"
    with pytest.raises(webhook.NoMp4Files) as exc_info:
        webhook.validate_payload(payload)
    assert exc_info.match("No mp4 files in recording data")


@freeze_time(FROZEN_TIME)
def test_handler_happy_trail(handler, mocker, webhook_payload,
                             sqs_message_from_webhook_payload):
    event = {
        'body': json.dumps(webhook_payload())
    }
    mock_sqs_send = mocker.patch.object(webhook, 'send_sqs_message')

    resp = handler(webhook, event)
    expected_msg = sqs_message_from_webhook_payload(
        FROZEN_TIME, "recording.completed"
    )
    mock_sqs_send.assert_called_once_with(
        expected_msg, webhook.DEFAULT_MESSAGE_DELAY
    )
    assert resp['statusCode'] == 200


@freeze_time(FROZEN_TIME)
def test_no_mp4s_response(handler, mocker, webhook_payload):
    payload = webhook_payload()

    payload["event"] = "recording.completed"
    payload["payload"]["object"]["recording_files"][0]["file_type"] = "foo"
    event = {"body": json.dumps(payload)}

    resp = handler(webhook, event)
    assert resp["statusCode"] == 204

    payload["event"] = "on.demand.ingest"
    event = {"body": json.dumps(payload)}

    resp = handler(webhook, event)
    assert resp["statusCode"] == 400


@freeze_time(FROZEN_TIME)
def test_on_demand_no_delay(handler, mocker, webhook_payload,
                            sqs_message_from_webhook_payload):
    payload = webhook_payload()
    payload["event"] = "on.demand.ingest"
    event = {
        "body": json.dumps(payload)
    }
    mock_sqs_send = mocker.patch.object(webhook, 'send_sqs_message')

    resp = handler(webhook, event)
    expected_msg = sqs_message_from_webhook_payload(FROZEN_TIME, payload["event"])
    mock_sqs_send.assert_called_once_with(expected_msg, 0)
    assert resp['statusCode'] == 200

