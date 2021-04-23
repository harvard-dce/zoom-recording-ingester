import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

from os import getenv
from freezegun import freeze_time
from datetime import datetime, timedelta
from pytz import utc
from utils import status

TIMESTAMP_FORMAT = getenv("TIMESTAMP_FORMAT")
FROZEN_TIME = utc.localize(datetime.now())
FROZEN_EXPIRATION = int((FROZEN_TIME + timedelta(days=7)).timestamp())
FROZEN_UPDATE_DATE, FROZEN_UPDATE_TIME = status.ts_to_date_and_seconds(
    FROZEN_TIME
)


@freeze_time(FROZEN_TIME)
def test_set_pipeline_status_webhook_received(mocker):
    mocker.patch.object(
        status, "zip_status_table", mocker.Mock(return_value="status_table")
    )
    mock_update_status_table = mocker.patch.object(
        status,
        "update_status_table",
    )

    status.set_pipeline_status(
        "correlation_id",
        status.PipelineStatus.WEBHOOK_RECEIVED,
        origin="webhook_notification",
        meeting_id="123456789",
        recording_id="test_recording_id",
        recording_start_time="test_recording_start",
        topic="test_topic",
        oc_series_id="20210199999",
    )

    mock_update_status_table.assert_called_with(
        "status_table",
        "correlation_id",
        (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state, "
            "ingest_request_time=:ingest_request_time, "
            "meeting_id=:meeting_id, "
            "recording_id=:recording_id, "
            "origin=:origin, "
            "recording_start_time=:recording_start_time, "
            "topic=:topic, "
            "oc_series_id=:oc_series_id"
        ),
        {
            ":update_date": FROZEN_UPDATE_DATE,
            ":update_time": FROZEN_UPDATE_TIME,
            ":expiration": FROZEN_EXPIRATION,
            ":pipeline_state": "WEBHOOK_RECEIVED",
            ":ingest_request_time": FROZEN_TIME.strftime(TIMESTAMP_FORMAT),
            ":meeting_id": 123456789,
            ":recording_id": "test_recording_id",
            ":origin": "webhook_notification",
            ":recording_start_time": "test_recording_start",
            ":topic": "test_topic",
            ":oc_series_id": "20210199999",
        },
        None,
    )


@freeze_time(FROZEN_TIME)
def test_set_pipeline_status_update_status(mocker):
    mocker.patch.object(
        status, "zip_status_table", mocker.Mock(return_value="status_table")
    )
    mock_update_status_table = mocker.patch.object(
        status,
        "update_status_table",
    )

    status.set_pipeline_status(
        "correlation_id",
        status.PipelineStatus.UPLOADER_RECEIVED,
    )

    mock_update_status_table.assert_called_with(
        "status_table",
        "correlation_id",
        (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state"
        ),
        {
            ":update_date": FROZEN_UPDATE_DATE,
            ":update_time": FROZEN_UPDATE_TIME,
            ":expiration": FROZEN_EXPIRATION,
            ":pipeline_state": "UPLOADER_RECEIVED",
        },
        "attribute_exists(meeting_id) AND attribute_exists(origin)",
    )


@freeze_time(FROZEN_TIME)
def set_pipeline_status_recording_processing(mocker):
    mocker.patch.object(
        status, "zip_status_table", mocker.Mock(return_value="status_table")
    )
    mock_update_status_table = mocker.patch.object(
        status,
        "update_status_table",
    )

    status.set_pipeline_status(
        "correlation_id",
        status.PipelineStatus.RECORDING_PROCESSING,
        origin="webhook_notification",
        meeting_id="123456789",
        recording_id="test_recording_id",
        recording_start_time="test_recording_start",
        topic="test_topic",
    )

    mock_update_status_table.assert_called_with(
        "correlation_id",
        (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state, "
            "meeting_id=:meeting_id, "
            "recording_id=:recording_id, "
            "origin=:origin, "
            "recording_start_time=:recording_start_time, "
            "topic=:topic"
        ),
        {
            ":update_date": FROZEN_UPDATE_DATE,
            ":update_time": FROZEN_UPDATE_TIME,
            ":expiration": FROZEN_EXPIRATION,
            ":pipeline_state": "RECORDING_PROCESSING",
            ":meeting_id": 123456789,
            ":recording_id": "test_recording_id",
            ":origin": "webhook_notification",
            ":recording_start_time": "test_recording_start",
            ":topic": "test_topic",
        },
        ":pipeline_state = RECORDING_IN_PROGRESS OR :pipeline_state = RECORDING_PAUSED OR :pipeline_state = RECORDING_STOPPED",
    )


@freeze_time(FROZEN_TIME)
def set_pipeline_status_recording_stopped(mocker):
    mocker.patch.object(
        status, "zip_status_table", mocker.Mock(return_value="status_table")
    )
    mock_update_status_table = mocker.patch.object(
        status,
        "update_status_table",
    )

    status.set_pipeline_status(
        "correlation_id",
        status.PipelineStatus.RECORDING_STOPPED,
        origin="webhook_notification",
        meeting_id="123456789",
        recording_id="test_recording_id",
        recording_start_time="test_recording_start",
        topic="test_topic",
    )

    mock_update_status_table.assert_called_with(
        "correlation_id",
        (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state, "
            "meeting_id=:meeting_id, "
            "recording_id=:recording_id, "
            "origin=:origin, "
            "recording_start_time=:recording_start_time, "
            "topic=:topic"
        ),
        {
            ":update_date": FROZEN_UPDATE_DATE,
            ":update_time": FROZEN_UPDATE_TIME,
            ":expiration": FROZEN_EXPIRATION,
            ":pipeline_state": "RECORDING_STOPPED",
            ":meeting_id": 123456789,
            ":recording_id": "test_recording_id",
            ":origin": "webhook_notification",
            ":recording_start_time": "test_recording_start",
            ":topic": "test_topic",
        },
        ":pipeline_state <> RECORDING_PROCESSING",
    )


def test_format_status_records_empty():
    test_items = []
    expected = {"meetings": []}

    assert status.format_status_records(test_items) == expected


def test_format_status_records():
    test_items = [
        {
            "origin": "webhook_notification",
            "update_time": 65075,
            "correlation_id": "auto-ingest-mEN4a+AfQ1i9TYhk0gkqdg==",
            "topic": "Class Topic",
            "recording_id": "recording_uuid",
            "expiration": 1619805875,
            "pipeline_state": "RECORDING_IN_PROGRESS",
            "meeting_id": 123456789,
            "recording_start_time": "2021-04-23T17:45:24Z",
            "update_date": "2021-04-23",
        }
    ]
    expected = {
        "meetings": [
            {
                "meeting_id": 123456789,
                "topic": "Class Topic",
                "recordings": [
                    {
                        "recording_id": "recording_uuid",
                        "start_time": "2021-04-23T17:45:24Z",
                        "zip_ingests": [
                            {
                                "last_updated": "2021-04-23T18:04:35Z",
                                "status": "RECORDING_IN_PROGRESS",
                                "origin": "webhook_notification",
                            }
                        ],
                    }
                ],
            }
        ]
    }

    assert status.format_status_records(test_items) == expected
