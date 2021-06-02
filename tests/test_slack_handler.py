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
        slack_handler, "SLACK_ZIP_CHANNEL", "authorized_channel"
    )

    mocker.patch.object(
        slack_handler,
        "valid_slack_request",
        mocker.Mock(return_value=True),
    )

    mocker.patch.object(
        slack_handler, "allowed_user", mocker.Mock(return_value=True)
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
        slack_handler, "allowed_user", mocker.Mock(return_value=True)
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

    mocker.patch.object(
        slack_handler, "status_by_mid", mocker.Mock(return_value={})
    )

    commands = [
        # valid meeting ids
        ("/zip", "1234567890", True),
        ("/zip", "123 456 7890", True),
        ("/zip", "123-456-7890", True),
        ("/zip", "*1234567890*", True),
        # invalid meeting ids
        ("/zip", "123", False),
        ("/zip", "abcefghijk", False),
    ]

    for cmd, cmd_arg, valid in commands:
        mock_results_blocks = mocker.patch.object(
            slack_handler, "slack_results_blocks"
        )

        query = {
            "command": cmd,
            "text": cmd_arg,
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


def test_slash_command_response(mocker):
    pass


def test_interaction_response(mocker):
    pass


def test_allowed_user(mocker):
    pass
