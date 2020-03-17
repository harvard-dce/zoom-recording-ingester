import io
import site
import json
import pytest
from os.path import dirname, join
from importlib import import_module

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

uploader = import_module('zoom-uploader')

def test_too_many_uploads(handler, mocker):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    uploader.sqs.get_queue_by_name = mocker.Mock()
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=10))

    # with max = 5 and fake count = 10 the handler should abort
    # before retrieving messages
    mocker.patch.object(uploader, 'OC_TRACK_UPLOAD_MAX', 5)
    res = handler(uploader, {})
    assert uploader.sqs.get_queue_by_name.call_count == 0

def test_unknown_uploads(handler, mocker):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    uploader.sqs.get_queue_by_name = mocker.Mock()
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=None))

    # with max = 5 and fake count = None the handler should abort
    # before retrieving messages
    mocker.patch.object(uploader, 'OC_TRACK_UPLOAD_MAX', 5)
    res = handler(uploader, {})
    assert uploader.sqs.get_queue_by_name.call_count == 0

def test_upload_count_ok(handler, mocker):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    receive_messages = mocker.Mock(return_value=[])
    uploader.sqs.get_queue_by_name \
        .return_value.receive_messages = receive_messages
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=3))

    # with max = 3 and fake count = 5 the handler should proceed
    # to retrieving messages
    mocker.patch.object(uploader, 'OC_TRACK_UPLOAD_MAX', 5)
    res = handler(uploader, {})
    assert receive_messages.call_count == 1

def test_no_messages_available(handler, mocker, caplog):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=3))
    uploader.sqs.get_queue_by_name \
        .return_value.receive_messages \
        .return_value = []
    res = handler(uploader, {})
    assert caplog.messages[-1] == "No upload queue messages available."

def test_ingestion_error(handler, mocker, upload_message):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=3))
    message = upload_message()
    uploader.sqs.get_queue_by_name \
        .return_value.receive_messages \
        .return_value = [message]
    uploader.process_upload = mocker.Mock(
        side_effect=Exception("boom!"))
    with pytest.raises(Exception) as exc_info:
        res = handler(uploader, {})
    assert exc_info.match("boom!")
    # make sure the message doesn't get deleted
    assert message.delete.call_count == 0

def test_bad_message_body(handler, mocker):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=3))
    message = mocker.Mock(body="this is definitely not json")
    uploader.sqs.get_queue_by_name \
        .return_value.receive_messages \
        .return_value = [message]
    with pytest.raises(Exception) as exc_info:
        res = handler(uploader, {})
    assert exc_info.typename == "JSONDecodeError"
    # make sure the message doesn't get deleted
    assert message.delete.call_count == 0

def test_workflow_initiated(handler, mocker, upload_message, caplog):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=3))
    message = upload_message()
    uploader.sqs.get_queue_by_name \
        .return_value.receive_messages \
        .return_value = [message]
    uploader.process_upload = mocker.Mock(return_value=12345)
    res = handler(uploader, {})
    assert "12345 initiated" in caplog.messages[-1]

def test_workflow_not_initiated(handler, mocker, upload_message, caplog):
    mocker.patch.object(uploader, 'sqs', mocker.Mock())
    mocker.patch.object(uploader, 'get_current_upload_count',
                        mocker.Mock(return_value=3))
    message = upload_message()
    uploader.sqs.get_queue_by_name \
        .return_value.receive_messages \
        .return_value = [message]
    uploader.process_upload = mocker.Mock(return_value=None)
    res = handler(uploader, {})
    assert "No workflow initiated." == caplog.messages[-1]

def test_get_current_upload_count(mocker):
    uploader.aws_lambda = mocker.Mock()
    cases = [
        (10, {"track": 5, "uri-track": 5}),
        (10, {"track": 5, "uri-track": 5, "foo": 3, "bar": 9}),
        (35, {"track": 35, "bar": 9})
    ]
    for count, count_data in cases:
        uploader.aws_lambda.invoke.return_value = {
            "Payload": io.StringIO(json.dumps(count_data))
        }
        assert uploader.get_current_upload_count() == count

    uploader.aws_lambda.invoke.return_value = {
        "Payload": io.StringIO("no json here either")
    }
    assert uploader.get_current_upload_count() == None
