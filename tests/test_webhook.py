import site
from os.path import dirname, join
import pytest
from freezegun import freeze_time
from pytz import timezone
from datetime import datetime
import os
import json

site.addsitedir(join(dirname(dirname(__file__)), "functions"))
import zoom_webhook as webhook

LOCAL_TIME_ZONE = os.getenv("LOCAL_TIME_ZONE")
TIMESTAMP_FORMAT = os.getenv("TIMESTAMP_FORMAT")

webhook.WEBHOOK_VALIDATION_SECRET_TOKEN = "barbaz67890"

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(tz.localize(datetime.now()), TIMESTAMP_FORMAT)


class MockContext:
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
        self.function_name = "zoom_webhook"


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


def test_started_event(handler, webhook_payload):
    recording_started = webhook_payload()
    recording_started["event"] = "recording.started"
    event = {"body": json.dumps(recording_started)}

    res = handler(webhook, event)
    assert res["statusCode"] == 204


def test_validate_payload(webhook_payload):
    # should not raise an exception
    minimum_valid_payload = webhook_payload()["payload"]
    webhook.validate_payload(minimum_valid_payload, "recording_completed")

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
            webhook.validate_payload(payload, "recording.completed")
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
    handler,
    mock_webhook_set_pipeline_status,
    mock_webhook_set_recording_events,
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
    event_ts = 1578621046000
    for event, expected_status in cases:
        handler(
            webhook,
            {
                "body": json.dumps(
                    {
                        "event": event,
                        "event_ts": event_ts,
                        "payload": mock_payload,
                    }
                )
            },
        )

        mock_webhook_set_pipeline_status.assert_called_with(
            "auto-ingest-mock_uuid",
            expected_status,
            meeting_id=mock_payload["object"]["id"],
            recording_id=mock_payload["object"]["uuid"],
            recording_start_time=mock_payload["object"]["start_time"],
            topic=mock_payload["object"]["topic"],
            origin="webhook_notification",
        )

        mock_webhook_set_recording_events.assert_called_with(
            zoom_uuid="mock_uuid",
            zoom_event=event,
            zoom_event_timestamp=event_ts,
        )


def test_update_meeting_ended(
    handler, mocker, mock_webhook_set_pipeline_status
):
    #    mock_webhook_set_pipeline_status.reset_mock()
    mock_zip_id = "auto-ingest-mock_uuid"
    mock_payload = {
        "object": {
            "id": "12345678",
            "uuid": "mock_uuid",
            "start_time": "mock_start_time",
            "topic": "mock_topic",
        }
    }

    event = {
        "body": json.dumps({"event": "meeting.ended", "payload": mock_payload})
    }

    # Case 1 - Update a tracked meeting.
    mocker.patch.object(
        webhook,
        "record_exists",
        mocker.Mock(return_value=True),
    )

    handler(webhook, event)

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

    handler(webhook, event)
    mock_webhook_set_pipeline_status.assert_not_called()


def test_webhook_validation(handler):
    expected = (
        "aaca35ce2b1ee8e81c06c0e8c9841f4870bfa2b29b1812688459cf5d6cbdb5df"
    )
    event = {
        "body": json.dumps(
            {
                "event": "endpoint.url_validation",
                "payload": {
                    "plainToken": "foobar12345",
                },
            }
        )
    }
    res = handler(webhook, event)
    assert res["statusCode"] == 200
    res_body = json.loads(res["body"])
    assert res_body["encryptedToken"] == expected


def test_webhook_validation_no_secret_token(handler, caplog):
    webhook.WEBHOOK_VALIDATION_SECRET_TOKEN = None
    event = {
        "body": json.dumps(
            {
                "event": "endpoint.url_validation",
                "payload": {
                    "plainToken": "foobar12345",
                },
            }
        )
    }
    res = handler(webhook, event)
    assert res["statusCode"] == 204
    assert "validation not enabled for this endpoint" in caplog.text.lower()
