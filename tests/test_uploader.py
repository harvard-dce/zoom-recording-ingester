import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import json
from unittest.mock import Mock
from importlib import import_module

uploader = import_module('zoom-uploader')


def test_uploader_handler(handler, monkeypatch):

    mock_sqs = Mock()
    mock_process_upload = Mock(side_effect=['abcd1234','xyz789','456-789'])
    message = Mock(body=json.dumps({'foo': 1}))

    mock_sqs.get_queue_by_name \
        .return_value.receive_messages.return_value = [message]

    monkeypatch.setattr(uploader, 'sqs', mock_sqs)
    monkeypatch.setattr(uploader, 'process_upload', mock_process_upload)

    handler(uploader, {'num_uploads': 3})

    assert message.delete.call_count == 3

