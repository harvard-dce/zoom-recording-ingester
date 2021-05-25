import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

import json
from urllib.parse import urlencode
from importlib import import_module

slack_handler = uploader = import_module("slack.handler")


def test_invalid_slack_request(handler, mocker):
    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(return_value=False),
    )
    r = handler(slack_handler, {})
    assert r["statusCode"] == 200
    assert "Slack request verification failed." in r["body"]


def test_error_validating_request(handler, mocker):
    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(side_effect=Exception("boom!")),
    )
    r = handler(slack_handler, {})
    assert r["statusCode"] == 200
    assert "Error while validating Slack request." in r["body"]


def test_ignore_action(handler, mocker):
    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(return_value=True),
    )
    interaction_payload = {
        "response_url": "url",
        "channel": {
            "name": "channel_name",
        },
        "user": {"id": "user_id", "name": "username"},
        "actions": [
            {
                "value": json.dumps({"version": 1}),
            }
        ],
    }

    payload = {"payload": json.dumps(interaction_payload)}

    event = {"body": urlencode(payload)}

    r = handler(slack_handler, event)
    assert r["statusCode"] == 204
