import site
import unittest
import pytest
import json
import copy
from os.path import dirname, join
from importlib import import_module

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

uploader = import_module('zoom-uploader')

SAMPLE_MESSAGE_BODY = {
    "uuid": "abcdefg1234==",
    "zoom_series_id": 123456789,
    "opencast_series_id": "20200299999",
    "host_name": "Angela Amari",
    "topic": "TEST E-50",
    "created": "2020-03-09T23:19:20Z",
    "webhook_received_time": "2020-03-10T01:58:03Z",
    "correlation_id": "1234"
}


class MockContext():
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
        self.function_name = "zoom-uploader"


class MockUploadMessage():
    def __init__(self, body):
        self.body = json.dumps(body)
        self.attributes = None
        self.delete_call_count = 0

    def delete(self):
        self.delete_call_count += 1


"""
Test handler
"""


class TestHandler(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def initfixtures(self, mocker):
        self.mocker = mocker
        self.context = MockContext(aws_request_id="mock-correlation_id")
        self.mock_sqs = unittest.mock.Mock()
        self.mocker.patch.object(uploader, "sqs_resource", self.mock_sqs)

    def test_no_messages_available(self):
        self.mock_sqs().get_queue_by_name \
            .return_value.receive_messages.return_value = []

        with self.assertLogs(level='INFO') as cm:
            resp = uploader.handler({}, self.context)
            log_message = json.loads(cm.output[-1])["message"]
            assert log_message == "No upload queue messages available."
            assert not resp

    def test_workflow_initiated(self):

        mock_wf_id = 92293838
        cases = [
            (mock_wf_id, "Workflow id {} initiated".format(mock_wf_id)),
            (None, "No workflow initiated.")
        ]

        for wf_id, expected_log_msg in cases:

            message = MockUploadMessage(copy.deepcopy(SAMPLE_MESSAGE_BODY))

            self.mock_sqs().get_queue_by_name \
                .return_value.receive_messages.return_value = [message]

            self.mocker.patch.object(
                uploader,
                "process_upload",
                return_value=wf_id
            )

            with self.assertLogs(level="INFO") as cm:
                uploader.handler({}, self.context)
                log_message = json.loads(cm.output[-1])["message"]
                assert log_message == expected_log_msg
                assert message.delete_call_count == 1

    def test_error_while_ingesting(self):
        message = MockUploadMessage(copy.deepcopy(SAMPLE_MESSAGE_BODY))

        self.mock_sqs().get_queue_by_name \
            .return_value.receive_messages.return_value = [message]

        error_message = "failure while ingesting"
        self.mocker.patch.object(
            uploader,
            "process_upload",
            side_effect=Exception(error_message)
        )

        with pytest.raises(Exception) as exc_info:
            uploader.handler({}, self.context)
        assert exc_info.match(error_message)
