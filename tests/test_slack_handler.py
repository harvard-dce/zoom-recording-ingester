import site
from os import getenv
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

import json
from freezegun import freeze_time
from datetime import datetime, timedelta
from pytz import utc
from unittest.mock import call
import pytest
import hmac
import hashlib
from urllib.parse import urlencode
from importlib import import_module

slack_handler = uploader = import_module("slack.handler")

FROZEN_TIME = utc.localize(datetime.now())
TIMESTAMP_FORMAT = getenv("TIMESTAMP_FORMAT")


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


def test_unauthorized_user(handler, mocker):
    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(return_value=True),
    )
    mocker.patch.object(
        slack_handler,
        "allowed_user",
        mocker.Mock(return_value=False),
    )

    query = {
        "command": "/zip",
        "response_url": "url",
        "channel_name": "channel",
        "user_id": "id",
        "user_name": "name",
    }

    event = {"body": urlencode(query)}

    r = handler(slack_handler, event)
    assert r["statusCode"] == 200
    assert "You are not authorized to use command /zip." in r["body"]


def test_unauthorized_channel(handler, mocker):
    mocker.patch.object(
        slack_handler,
        "SLACK_ZIP_CHANNEL",
        "authorized_channel",
    )

    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(return_value=True),
    )

    mocker.patch.object(
        slack_handler,
        "allowed_user",
        mocker.Mock(return_value=True),
    )

    query = {
        "command": "/zip",
        "response_url": "url",
        "channel_name": "unauthorized_channel",
        "user_id": "id",
        "user_name": "name",
    }

    event = {"body": urlencode(query)}

    r = handler(slack_handler, event)
    assert r["statusCode"] == 200
    assert (
        "The command /zip can only be used in DM or in the #authorized_channel channel"
        in r["body"]
    )


def setup_valid_request(mocker):
    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(return_value=True),
    )

    mocker.patch.object(
        slack_handler,
        "allowed_user",
        mocker.Mock(return_value=True),
    )

    mocker.patch.object(
        slack_handler,
        "status_by_mid",
        mocker.Mock(return_value={}),
    )


def test_help_menu(handler, mocker):
    setup_valid_request(mocker)

    mocker.patch.object(
        slack_handler,
        "slack_help_menu_blocks",
        mocker.Mock(return_value="help_menu"),
    )

    for cmd in ["/zip", "/zip help"]:
        query = {
            "command": cmd,
            "response_url": "url",
            "channel_name": "directmessage",
            "user_id": "id",
            "user_name": "name",
        }

        event = {"body": urlencode(query)}

        r = handler(slack_handler, event)
        assert r["statusCode"] == 200
        body = json.loads(r["body"])

        assert body == {"response_type": "ephemeral", "blocks": "help_menu"}


def test_mid_formats(handler, mocker):
    setup_valid_request(mocker)

    commands = [
        # valid meeting ids
        ("1234567890", True),
        ("123 456 7890", True),
        ("123-456-7890", True),
        ("*1234567890*", True),
        # invalid meeting ids
        ("123", False),
        ("abcefghijk", False),
    ]

    for mid, valid in commands:
        mock_results_blocks = mocker.patch.object(
            slack_handler,
            "slack_results_blocks",
        )

        query = {
            "command": "/zip",
            "text": mid,
            "response_url": "url",
            "channel_name": "directmessage",
            "user_id": "id",
            "user_name": "name",
        }

        event = {"body": urlencode(query)}

        r = handler(slack_handler, event)
        assert r["statusCode"] == 200

        if valid:
            # make sure slack_Results_blocks called with correct mid
            mock_results_blocks.assert_called_once_with(1234567890, {})
        else:
            assert "Please specify a valid Zoom meeting ID." in r["body"]


def test_slash_command_response(handler, mocker):
    setup_valid_request(mocker)

    mocker.patch.object(
        slack_handler,
        "slack_results_blocks",
        mocker.Mock(return_value="results_blocks"),
    )

    query = {
        "command": "/zip",
        "text": "1234567890",
        "response_url": "url",
        "channel_name": "directmessage",
        "user_id": "id",
        "user_name": "name",
    }

    event = {"body": urlencode(query)}

    r = handler(slack_handler, event)
    assert r["statusCode"] == 200

    body = json.loads(r["body"])
    assert body == {"response_type": "in_channel", "blocks": "results_blocks"}


def test_interaction_response(handler, mocker):
    setup_valid_request(mocker)

    max_records_per_msg = 5

    mocker.patch.object(
        slack_handler,
        "MAX_RECORDS_PER_MSG",
        max_records_per_msg,
    )

    mocker.patch.object(
        slack_handler,
        "slack_results_blocks",
        mocker.Mock(return_value="results_blocks"),
    )

    cases = [
        (max_records_per_msg, True),
        (max_records_per_msg - 1, False),
        (max_records_per_msg + 1, False),
    ]

    for count, add_new_message in cases:
        mock_send_interaction_response = mocker.patch.object(
            slack_handler, "send_interaction_response"
        )

        blocks = ["block1", "block2", "divider", "button"]

        interaction_payload = {
            "response_url": "mock_response_url",
            "channel": {
                "name": "directmessage",
            },
            "user": {"id": "user_id", "name": "username"},
            "actions": [
                {
                    "value": json.dumps(
                        {
                            "version": 1,
                            "mid": "1234567890",
                            "newest_start_time": "start_time",
                            "start_index": 0,
                            "count": count,
                        }
                    ),
                }
            ],
            "message": {
                "blocks": blocks,
            },
        }

        payload = {"payload": json.dumps(interaction_payload)}

        event = {"body": urlencode(payload)}

        r = handler(slack_handler, event)
        assert r["statusCode"] == 204

        if add_new_message:
            mock_send_interaction_response.assert_has_calls(
                [
                    call(
                        "mock_response_url",
                        blocks[:-2],
                        replace_original=True,
                    ),
                    call(
                        "mock_response_url",
                        "results_blocks",
                        replace_original=False,
                    ),
                ]
            )
        else:
            mock_send_interaction_response.assert_called_once_with(
                "mock_response_url",
                "results_blocks",
                replace_original=True,
            )


def test_allowed_user(mocker):
    def mock_slack_api_request(endpoint):
        if endpoint == "usergroups.list":
            return {
                "usergroups": [
                    {
                        "id": "staff_group_id",
                        "handle": "staff_group_handle",
                    },
                ]
            }
        elif "usergroups.users.list?usergroup=" in endpoint:
            group_id = endpoint.split("=")[-1]
            if group_id == "staff_group_id":
                return {"users": ["allowed_user"]}
            else:
                return {"users": ["non_allowed_user"]}

    mocker.patch.object(
        slack_handler,
        "slack_api_request",
        mock_slack_api_request,
    )

    with pytest.raises(Exception, match="No slack allowed groups found"):
        slack_handler.allowed_user("allowed_user")

    mocker.patch.object(
        slack_handler,
        "SLACK_ALLOWED_GROUPS",
        "staff_group_handle",
    )

    assert slack_handler.allowed_user("allowed_user")
    assert not slack_handler.allowed_user("non_allowed_user")


@freeze_time(FROZEN_TIME)
def test_valid_slack_request(mocker):

    # User-Agent must be Slackbot
    assert not slack_handler.valid_slack_request(
        {"headers": {"User-Agent": "user_agent"}}
    )

    mock_secret = "mock_secret"
    mock_body = "mock_body"

    valid_hash = hmac.new(
        bytes(mock_secret, "UTF-8"),
        bytes(f"1:{FROZEN_TIME.timestamp()}:{mock_body}", "UTF-8"),
        hashlib.sha256,
    ).hexdigest()

    valid_signature = f"1={valid_hash}"

    mocker.patch.object(
        slack_handler,
        "SLACK_SIGNING_SECRET",
        mock_secret,
    )

    cases = [
        # Reject requests older than 5 minutes
        (301, "signature=", False),
        # Invalid signature
        (0, "invalid_signature", False),
        # Valid secret
        (0, valid_signature, True),
    ]

    for age_in_seconds, signature, expected in cases:
        event = {
            "headers": {
                "User-Agent": "Slackbot",
                "X-Slack-Request-Timestamp": (
                    FROZEN_TIME + timedelta(seconds=age_in_seconds)
                ).timestamp(),
                "X-Slack-Signature": signature,
            },
            "body": mock_body,
        }
        assert slack_handler.valid_slack_request(event) == expected
