import site
from os.path import dirname, join
import pytest
from importlib import import_module
from freezegun import freeze_time
from pytz import timezone
from datetime import datetime
import os
import json
import requests_mock

site.addsitedir(join(dirname(dirname(__file__)), "functions"))


LOCAL_TIME_ZONE = os.getenv("LOCAL_TIME_ZONE")
TIMESTAMP_FORMAT = os.getenv("TIMESTAMP_FORMAT")
webhook = import_module("zoom-webhook")

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(tz.localize(datetime.now()), TIMESTAMP_FORMAT)


class MockContext:
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
        self.function_name = "zoom-webhook"


def test_missing_body(handler):
    res = handler(webhook, {})
    assert res["statusCode"] == 400
    assert "no body found" in res["body"].lower()


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
    body = json.dumps(
        {"payload": "invalid payload", "event": "recording.completed"}
    )
    res = handler(webhook, {"body": body})
    assert res["statusCode"] == 400
    assert "bad data" in res["body"].lower()


def test_started_event(handler, mocker, webhook_payload):
    recording_started = webhook_payload()
    recording_started["event"] = "recording.started"
    event = {"body": json.dumps(recording_started)}

    res = handler(webhook, event)
    assert res["statusCode"] == 204


def test_validate_payload(webhook_payload):

    # should not raise an exception
    minimum_valid_payload = webhook_payload()["payload"]
    webhook.validate_payload(minimum_valid_payload)

    missing_recording_files = webhook_payload()["payload"]
    del missing_recording_files["object"]["recording_files"]

    missing_object_field = webhook_payload()["payload"]
    del missing_object_field["object"]

    payloads = [
        (
            missing_recording_files,
            "Missing required object field 'recording_files'",
        ),
        (missing_object_field, "Missing required payload field 'object'"),
    ]

    for payload, msg in payloads:
        with pytest.raises(webhook.BadWebhookData) as exc_info:
            webhook.validate_payload(payload)
        assert exc_info.match(msg), msg


def test_validate_recording_files(webhook_payload):
    missing_file_id = webhook_payload()["payload"]["object"]["recording_files"]
    del missing_file_id[0]["id"]

    expected_msg = "Missing required file field 'id'"

    with pytest.raises(webhook.BadWebhookData) as exc_info:
        webhook.validate_recording_files(missing_file_id)
    assert exc_info.match(expected_msg), expected_msg


@freeze_time(FROZEN_TIME)
def test_no_mp4s_validation(mocker, webhook_payload):
    recording_files = webhook_payload()["payload"]["object"]["recording_files"]
    recording_files[0]["file_type"] = "foo"
    with pytest.raises(webhook.NoMp4Files) as exc_info:
        webhook.validate_recording_files(recording_files)
    assert exc_info.match("No mp4 files in recording data")


@freeze_time(FROZEN_TIME)
def test_handler_happy_trail(
    handler,
    mocker,
    webhook_payload,
    sqs_message_from_webhook_payload,
):
    event = {"body": json.dumps(webhook_payload())}
    mock_sqs_send = mocker.patch.object(webhook, "send_sqs_message")

    resp = handler(webhook, event)
    expected_msg = sqs_message_from_webhook_payload(
        FROZEN_TIME,
        webhook_payload(),
    )
    mock_sqs_send.assert_called_once_with(
        expected_msg,
        webhook.DEFAULT_MESSAGE_DELAY,
    )
    assert resp["statusCode"] == 200


@freeze_time(FROZEN_TIME)
def test_no_mp4s_response(handler, mocker, webhook_payload):
    cases = [(False, 204), (True, 400)]

    for on_demand, expected_status_code in cases:
        payload = webhook_payload(on_demand=on_demand)
        payload["payload"]["object"]["recording_files"][0]["file_type"] = "foo"
        event = {"body": json.dumps(payload)}

        resp = handler(webhook, event)
        assert resp["statusCode"] == expected_status_code


@freeze_time(FROZEN_TIME)
def test_on_demand_no_delay(
    handler,
    mocker,
    webhook_payload,
    sqs_message_from_webhook_payload,
):
    payload = webhook_payload(on_demand=True)
    event = {"body": json.dumps(payload)}
    mock_sqs_send = mocker.patch.object(webhook, "send_sqs_message")

    resp = handler(webhook, event)
    expected_msg = sqs_message_from_webhook_payload(FROZEN_TIME, payload)
    mock_sqs_send.assert_called_once_with(expected_msg, 0)
    assert resp["statusCode"] == 200


def test_update_recording_started_paused(
    mocker, mock_webhook_set_pipeline_status
):
    mock_payload = {
        "object": {
            "id": "12345678",
            "uuid": "mock_uuid",
            "start_time": "mock_start_time",
            "topic": "mock_topic",
        }
    }

    cases = [
        ("recording.started", webhook.ZoomStatus.RECORDING_IN_PROGRESS),
        ("recording.paused", webhook.ZoomStatus.RECORDING_PAUSED),
    ]
    for event, expected_status in cases:
        webhook.update_zoom_status(
            event,
            mock_payload,
            "mock_zip_id",
        )
        mock_webhook_set_pipeline_status.assert_called_with(
            "mock_zip_id",
            expected_status,
            meeting_id=mock_payload["object"]["id"],
            recording_id=mock_payload["object"]["uuid"],
            recording_start_time=mock_payload["object"]["start_time"],
            topic=mock_payload["object"]["topic"],
            origin="webhook_notification",
        )


def test_update_recording_stopped(mocker, mock_webhook_set_pipeline_status):
    mock_zip_id = "mock_zip_id"
    mock_payload = {
        "object": {
            "id": "12345678",
            "uuid": "mock_uuid",
            "start_time": "mock_start_time",
            "topic": "mock_topic",
        }
    }

    cases = [
        (404, webhook.ZoomStatus.RECORDING_STOPPED),
        (200, webhook.ZoomStatus.RECORDING_PROCESSING),
    ]

    for http_status, expected_recording_status in cases:
        with requests_mock.mock() as req_mock:
            req_mock.get(
                requests_mock.ANY,
                status_code=http_status,
            )
            webhook.update_zoom_status(
                "recording.stopped",
                mock_payload,
                mock_zip_id,
            )
            mock_webhook_set_pipeline_status.assert_called_with(
                "mock_zip_id",
                expected_recording_status,
                meeting_id=mock_payload["object"]["id"],
                recording_id=mock_payload["object"]["uuid"],
                recording_start_time=mock_payload["object"]["start_time"],
                topic=mock_payload["object"]["topic"],
                origin="webhook_notification",
            )


def test_update_meeting_ended(mocker, mock_webhook_set_pipeline_status):
    #    mock_webhook_set_pipeline_status.reset_mock()
    mock_zip_id = "mock_zip_id"
    mock_payload = {
        "object": {
            "id": "12345678",
            "uuid": "mock_uuid",
            "start_time": "mock_start_time",
            "topic": "mock_topic",
        }
    }

    # Case 1 - Update a tracked meeting.
    mocker.patch.object(
        webhook,
        "record_exists",
        mocker.Mock(return_value=True),
    )

    webhook.update_zoom_status(
        "meeting.ended",
        mock_payload,
        mock_zip_id,
    )

    mock_webhook_set_pipeline_status.assert_called_once_with(
        mock_zip_id,
        webhook.ZoomStatus.RECORDING_PROCESSING,
        meeting_id=mock_payload["object"]["id"],
        recording_id=mock_payload["object"]["uuid"],
        recording_start_time=mock_payload["object"]["start_time"],
        topic=mock_payload["object"]["topic"],
        origin="webhook_notification",
    )

    # Case 2 - Do not update a meeting that is not being tracked.
    # (If meeting.ended is sent from a non-recorded meeting then we
    # don't care about it)
    mock_webhook_set_pipeline_status.reset_mock()
    mocker.patch.object(
        webhook,
        "record_exists",
        mocker.Mock(return_value=False),
    )
    mock_webhook_set_pipeline_status.assert_not_called()
