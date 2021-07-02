from utils import (
    setup_logging,
    status_by_mid,
    getenv,
)
import json
from urllib.parse import parse_qs
import time
import hmac
import hashlib
import requests
import traceback
from .blocks import (
    slack_help_menu_blocks,
    slack_results_blocks,
    RESULTS_PER_REQUEST,
    MAX_RECORDS_PER_MSG,
)

import logging

logger = logging.getLogger()

SLACK_SIGNING_SECRET = getenv("SLACK_SIGNING_SECRET")
STACK_NAME = getenv("STACK_NAME")
SLACK_ZIP_CHANNEL = getenv("SLACK_ZIP_CHANNEL", required=False)
SLACK_API_TOKEN = getenv("SLACK_API_TOKEN")
SLACK_ALLOWED_GROUPS = getenv("SLACK_ALLOWED_GROUPS").split(",")
ZOOM_MID_LENGTHS = [10, 11]


class SlackApiRequestError(Exception):
    pass


def resp_204(msg):
    logger.info(f"http 204 response: {msg}")
    return {"statusCode": 204, "headers": {}, "body": ""}  # 204 = no content


def resp_400(msg):
    logger.error(f"http 400 response: {msg}")
    return {"statusCode": 400, "headers": {}, "body": msg}


def slack_error_response(msg):
    logger.error(f"Slack error response: {msg}")
    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps({"response_type": "ephemeral", "text": msg}),
    }


def slack_help_response(cmd):
    help_menu_blocks = slack_help_menu_blocks(cmd)
    logger.info({"help_menu": help_menu_blocks})
    return {
        "statusCode": 200,
        "headers": {},
        "body": json.dumps(
            {
                "response_type": "ephemeral",
                "blocks": help_menu_blocks,
            }
        ),
    }


@setup_logging
def handler(event, context):
    logger.info(event)
    try:
        if not valid_slack_request(event):
            return slack_error_response("Slack request verification failed.")
    except Exception:
        return slack_error_response("Error while validating Slack request.")

    query = parse_qs(event["body"])
    logger.info(query)

    if "command" in query:
        slash_command = query["command"][0]
        slash_command_arg = query["text"][0] if "text" in query else None
        response_url = query["response_url"][0]
        channel_name = query["channel_name"][0]
        user_id = query["user_id"][0]
        username = query["user_name"][0]
    else:
        slash_command = None
        payload = json.loads(query["payload"][0])
        logger.info({"interaction_payload": payload})

        response_url = payload["response_url"]
        channel_name = payload["channel"]["name"]
        user_id = payload["user"]["id"]
        username = payload["user"]["name"]

        required_action_fields = [
            "version",
            "mid",
            "newest_start_time",
            "start_index",
            "count",
        ]
        action_value = json.loads(payload["actions"][0]["value"])
        if not all(field in action_value for field in required_action_fields):
            logger.warning("Action missing required field.")
            return resp_204("Ignore action. Action missing required field.")

        meeting_id = action_value["mid"]
        prev_msg = {
            "newest_start_time": action_value["newest_start_time"],
            "start_index": action_value["start_index"],
            "rec_count": action_value["count"],
            "blocks": payload["message"]["blocks"],
        }
        logger.info(f"Interaction value: {action_value}")

    # User group validation
    if not allowed_user(user_id):
        logger.warning(
            f"User {username}, id: {user_id}, not in authorized slack groups."
        )
        return slack_error_response(
            f"You are not authorized to use command {slash_command}."
        )

    # Channel validation
    dm = channel_name == "directmessage"
    authorized_channel = SLACK_ZIP_CHANNEL and (
        channel_name == SLACK_ZIP_CHANNEL
    )
    if not dm and not authorized_channel:
        logger.warning(
            f"Channel name {channel_name} not in authorized slack channels."
        )
        if slash_command:
            return slack_error_response(
                f"The command {slash_command} can only be used in DM"
                f" or in the #{SLACK_ZIP_CHANNEL} channel"
            )
        else:
            return slack_error_response(
                f"Requests from channel {channel_name} not authorized."
            )

    if slash_command:
        if not slash_command_arg or slash_command_arg == "help":
            return slack_help_response(slash_command)

        # Accept mid that includes spaces, dashes or is bold
        # and has a valid number of digits
        mid_txt = (
            slash_command_arg.replace("-", "")
            .replace(" ", "")
            .replace("*", "")
        )
        if not valid_mid(mid_txt):
            return slack_error_response(
                "Please specify a valid Zoom meeting ID."
            )
        meeting_id = int(mid_txt)

    logger.info("call status_by_mid...")
    meeting_status_data = status_by_mid(meeting_id)
    logger.info({"meeting_data": meeting_status_data})

    try:
        if slash_command:
            blocks = slack_results_blocks(meeting_id, meeting_status_data)
            # Response type must be "in_channel" for the "more results"
            # button to work. (Ephemeral messages cannot be updated.)
            response = {"response_type": "in_channel", "blocks": blocks}
            logger.info({"send_response": response})
            return {
                "statusCode": 200,
                "headers": {},
                "body": json.dumps(response),
            }
        else:
            add_new_message = prev_msg["rec_count"] % MAX_RECORDS_PER_MSG == 0
            if add_new_message:
                # Remove button and divider from previous message
                send_interaction_response(
                    response_url,
                    prev_msg["blocks"][:-2],
                    replace_original=True,
                )
                # Put new results in new message
                blocks = slack_results_blocks(
                    meeting_id,
                    meeting_status_data,
                    newest_start_time=prev_msg["newest_start_time"],
                    start_index=prev_msg["start_index"]
                    + prev_msg["rec_count"],
                    max_results=RESULTS_PER_REQUEST,
                    interaction=True,
                )
                send_interaction_response(
                    response_url,
                    blocks,
                    replace_original=False,
                )
            else:
                # Replace original message with more results
                blocks = slack_results_blocks(
                    meeting_id,
                    meeting_status_data,
                    newest_start_time=prev_msg["newest_start_time"],
                    start_index=prev_msg["start_index"],
                    max_results=prev_msg["rec_count"] + RESULTS_PER_REQUEST,
                    interaction=True,
                )
                send_interaction_response(
                    response_url,
                    blocks,
                    replace_original=True,
                )
            return resp_204("Interaction response successful")
    except Exception as e:
        track = traceback.format_exc()
        logger.error(f"Error generating slack response. {str(e)} {track}")
        if slash_command:
            return slack_error_response(
                "We're sorry! There was an error when handling your request: "
                f"{slash_command} {slash_command_arg}"
            )
        else:
            return resp_400("Error handling interaction.")


def valid_mid(mid):
    return mid and mid.isnumeric() and len(mid) in ZOOM_MID_LENGTHS


def valid_slack_request(event):
    if "Slackbot" not in event["headers"]["User-Agent"]:
        return False

    slack_ts = event["headers"]["X-Slack-Request-Timestamp"]
    signature = event["headers"]["X-Slack-Signature"]
    version = signature.split("=")[0]
    body = event["body"]

    basestring = f"{version}:{slack_ts}:{body}"

    # rejecting request more than 5 minutes old
    if abs(int(slack_ts) - time.time()) > 300:
        return False

    h = hmac.new(
        bytes(SLACK_SIGNING_SECRET, "UTF-8"),
        bytes(basestring, "UTF-8"),
        hashlib.sha256,
    )

    return hmac.compare_digest(f"{version}={str(h.hexdigest())}", signature)


def slack_api_request(endpoint):
    r = requests.get(
        f"https://slack.com/api/{endpoint}",
        headers={"Authorization": f"Bearer {SLACK_API_TOKEN}"},
    )
    r.raise_for_status()

    data = r.json()
    if "error" in data:
        raise SlackApiRequestError(f"Slack API request error: {data['error']}")

    return data


def allowed_user(user_id):
    data = slack_api_request("usergroups.list")
    logger.info(data)

    allowed_group_ids = []
    if "usergroups" in data:
        for group in data["usergroups"]:
            if group["handle"] in SLACK_ALLOWED_GROUPS:
                allowed_group_ids.append(group["id"])

    if not allowed_group_ids:
        raise Exception("No slack allowed groups found")

    for group_id in allowed_group_ids:
        data = slack_api_request(f"usergroups.users.list?usergroup={group_id}")
        if user_id in data["users"]:
            return True

    return False


def send_interaction_response(response_url, blocks, replace_original=True):
    response = {
        "response_type": "in_channel",
        "replace_original": replace_original,
        "blocks": blocks,
    }
    logger.info(
        {
            "Send interaction response": {
                "response_url": response_url,
                "response": response,
            }
        }
    )
    r = requests.post(response_url, json=response)
    logger.info(
        f"Response url returned status: {r.status_code} content: {str(r.content)}"
    )
    r.raise_for_status()
