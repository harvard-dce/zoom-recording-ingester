import site
from os.path import dirname
site.addsitedir(dirname(dirname(__file__)))

import json
import pytest
import boto3
from moto import mock_sqs
from unittest.mock import patch, Mock, MagicMock
from importlib import import_module

uploader = import_module('functions.zoom-uploader', 'functions')


def test_uploader_handler(monkeypatch):

    mock_sqs = Mock()
    mock_process_upload = Mock(side_effect=['abcd1234','xyz789','456-789'])
    message = Mock(body=json.dumps({'foo': 1}))

    mock_sqs.return_value.get_queue_by_name \
        .return_value.receive_messages.return_value = [message]

    monkeypatch.setattr(uploader.boto3, 'resource', mock_sqs)
    monkeypatch.setattr(uploader, 'process_upload', mock_process_upload)

    res = uploader.handler({'num_uploads': 3}, None)

    assert message.delete.call_count == 3

