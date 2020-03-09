
import json
import pytest


@pytest.fixture
def handler(mocker):
    """
    this fixture provides a way to call the function handlers so as
    to insert a canned context (which is assumed by the logger setup)
    """
    def _handler(func_module, event, context=None):
        if context is None:
            context = mocker.Mock(aws_request_id='12345-abcde')
        else:
            context.aws_request_id = '12345-abcde'
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


