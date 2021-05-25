import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

from urllib.parse import quote
from slack import blocks

STACK_NAME = "mock-stack-name"


def test_slack_results_blocks(mocker):
    # patch retreive schedule
    mock_id = 1234567890
    topic = "Personal Meeting Room"
    mock_uuid = "gkABCDEbbbbbbbkPuA=="

    mock_status_data = {
        "meeting_id": mock_id,
        "topic": topic,
        "recordings": [
            {
                "recording_id": mock_uuid,
                "start_time": "2021-05-19T12:08:47Z",
                "zip_ingests": [
                    {
                        "last_updated": "2021-05-19T13:10:06Z",
                        "status": "IGNORED",
                        "origin": "webhook_notification",
                        "reason": "No opencast series match",
                        "ingest_request_time": "2021-05-19T12:49:50Z",
                        "oc_series_id": "20200229999",
                    }
                ],
            }
        ],
    }

    mocker.patch.object(blocks, "STACK_NAME", STACK_NAME)
    mocker.patch.object(
        blocks,
        "retrieve_schedule",
        mocker.Mock(return_value=None),
    )

    results_blocks = blocks.slack_results_blocks(mock_id, mock_status_data)

    assert results_blocks == [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": topic,
            },
        },
        {
            "type": "context",
            "elements": [
                {"type": "plain_text", "text": f"Source: {STACK_NAME}"}
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Zoom Meeting ID:* 123 456 7890 ",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "This Zoom meeting is not configured for ZIP ingests.",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":movie_camera: *Recording on Wednesday, May 19, 2021 at 8:08AM*\n",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Automated Ingest*\n> Status: Ignored by ZIP. No opencast series match (since 05/19/21 9:10AM)\n",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<https://zoom.us/recording/management/detail?meeting_id={quote(mock_uuid)}|*View in Zoom*>",
            },
        },
    ]


def test_results_schedule_block(mocker):
    mock_id = 1234567890
    topic = "Personal Meeting Room"
    mock_uuid = "gkABCDEbbbbbbbkPuA=="

    mock_status_data = {
        "meeting_id": mock_id,
        "topic": topic,
        "recordings": [
            {
                "recording_id": mock_uuid,
                "start_time": "2021-05-19T12:08:47Z",
                "zip_ingests": [
                    {
                        "last_updated": "2021-05-19T13:10:06Z",
                        "status": "IGNORED",
                        "origin": "webhook_notification",
                        "reason": "No opencast series match",
                        "ingest_request_time": "2021-05-19T12:49:50Z",
                        "oc_series_id": "20200229999",
                    }
                ],
            }
        ],
    }
    mock_schedule = {
        "zoom_series_id": "1234567890",
        "opencast_series_id": "20210199999",
        "events": [
            {"title": "Lecture", "day": "T", "time": "10:30"},
            {"title": "Lecture", "day": "R", "time": "10:30"},
        ],
        "course_code": "APMA E-115",
    }

    mocker.patch.object(
        blocks,
        "retrieve_schedule",
        mocker.Mock(return_value=mock_schedule),
    )

    results_blocks = blocks.slack_results_blocks(mock_id, mock_status_data)

    schedule_block = results_blocks[3]
    assert schedule_block == {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": ":clock3: APMA E-115 Lecture on Tuesdays at 10:30AM\n:clock3: APMA E-115 Lecture on Thursdays at 10:30AM",
        },
    }
