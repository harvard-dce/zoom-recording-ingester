
import os
import json
import pytest
from datetime import datetime, timedelta

TIMESTAMP_FORMAT = os.getenv('TIMESTAMP_FORMAT')

@pytest.fixture
def aws_request_id():
    return "12345-abcde"

@pytest.fixture
def handler(mocker, aws_request_id):
    """
    this fixture provides a way to call the function handlers so as
    to insert a canned context (which is assumed by the logger setup)
    """
    def _handler(func_module, event, context=None):
        if context is None:
            context = mocker.Mock(aws_request_id=aws_request_id)
        else:
            context.aws_request_id = aws_request_id
        return getattr(func_module, 'handler')(event, context)

    return _handler

@pytest.fixture
def upload_message(mocker):
   def _upload_message_maker(message_data=None):
        msg = {
            "uuid": "abcdefg1234==",
            "zoom_series_id": 123456789,
            "opencast_series_id": "20200299999",
            "host_name": "Angela Amari",
            "topic": "TEST E-50",
            "created": "2020-03-09T23:19:20Z",
            "webhook_received_time": "2020-03-10T01:58:03Z",
            "correlation_id": "1234"
        }
        if message_data is not None:
            msg.update(message_data)
        return mocker.Mock(
            body=json.dumps(msg)
        )
   return _upload_message_maker

def deep_merge(dict1, dict2):
    """
    Recursive merge dictionaries.
    """
    for key, val in dict1.items():
        if isinstance(val, dict):
            dict2_node = dict2.setdefault(key, {})
            deep_merge(val, dict2_node)
        else:
            if key not in dict2:
                dict2[key] = val
    return dict2

@pytest.fixture
def webhook_payload():
    def _payload_maker(payload_extras = None):
        payload = {
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
                         "recording_type": "shared_screen_with_speaker_view"}
                    ]
                }
            },
            "event": "recording.completed",
            "allow_multiple_ingests": False
        }

        if payload_extras is not None:
            payload = deep_merge(payload, payload_extras)
        return payload
    return _payload_maker

@pytest.fixture
def sqs_message_from_webhook_payload(webhook_payload, aws_request_id):
    def _message_maker(frozen_time, zoom_event):
        payload_obj = webhook_payload()["payload"]["object"]
        msg = {
            "uuid": payload_obj["uuid"],
            "zoom_series_id": payload_obj["id"],
            "topic": payload_obj["topic"],
            "start_time": payload_obj["start_time"],
            "duration": payload_obj["duration"],
            "host_id": payload_obj["host_id"],
            "recording_files": payload_obj["recording_files"],
            "received_time": frozen_time,
            "correlation_id": aws_request_id,
            "allow_multiple_ingests": False
        }

        if zoom_event == "recording.completed":
            rec_start = datetime.strptime(msg["start_time"], TIMESTAMP_FORMAT)
            rec_end = rec_start + timedelta(minutes=msg["duration"])
            msg["zoom_processing_minutes"] = (
                (datetime.utcnow() - rec_end).total_seconds() // 60
            )

        for file in msg["recording_files"]:
            file["recording_id"] = file.pop("id")

        return msg

    return _message_maker
