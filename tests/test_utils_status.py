import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

from os import getenv
from freezegun import freeze_time
from datetime import datetime, timedelta
from pytz import utc
import pytest
import boto3
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from utils import status

TIMESTAMP_FORMAT = getenv("TIMESTAMP_FORMAT")
FROZEN_TIME = utc.localize(datetime.now())
FROZEN_EXPIRATION = int((FROZEN_TIME + timedelta(days=7)).timestamp())
FROZEN_UPDATE_DATE, FROZEN_UPDATE_TIME = status.ts_to_date_and_seconds(
    FROZEN_TIME
)


@freeze_time(FROZEN_TIME)
def test_set_pipeline_status_webhook_received(mocker):
    mocker.patch.object(status, "status_table", "mock_status_table")
    mock_update_status_table = mocker.patch.object(
        status,
        "update_status_table",
    )

    status.set_pipeline_status(
        "zip_id",
        status.PipelineStatus.WEBHOOK_RECEIVED,
        origin="webhook_notification",
        meeting_id="123456789",
        recording_id="test_recording_id",
        recording_start_time="test_recording_start",
        topic="test_topic",
        oc_series_id="20210199999",
    )

    mock_update_status_table.assert_called_with(
        "mock_status_table",
        "zip_id",
        (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state, "
            "ingest_request_time=:ingest_request_time, "
            "meeting_id=:meeting_id, "
            "recording_id=:recording_id, "
            "reason=:reason, "
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
            ":reason": "",
        },
        None,
    )


@freeze_time(FROZEN_TIME)
def test_set_pipeline_status_update_status(mocker):
    mocker.patch.object(status, "status_table", "mock_status_table")
    mock_update_status_table = mocker.patch.object(
        status,
        "update_status_table",
    )

    status.set_pipeline_status(
        "zip_id",
        status.PipelineStatus.UPLOADER_RECEIVED,
    )

    mock_update_status_table.assert_called_with(
        "mock_status_table",
        "zip_id",
        (
            "set update_date=:update_date, "
            "update_time=:update_time, "
            "expiration=:expiration, "
            "pipeline_state=:pipeline_state, "
            "reason=:reason"
        ),
        {
            ":update_date": FROZEN_UPDATE_DATE,
            ":update_time": FROZEN_UPDATE_TIME,
            ":expiration": FROZEN_EXPIRATION,
            ":pipeline_state": "UPLOADER_RECEIVED",
            ":reason": "",
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
        "zip_id",
        status.PipelineStatus.RECORDING_PROCESSING,
        origin="webhook_notification",
        meeting_id="123456789",
        recording_id="test_recording_id",
        recording_start_time="test_recording_start",
        topic="test_topic",
    )

    mock_update_status_table.assert_called_with(
        "zip_id",
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
        "zip_id",
        status.PipelineStatus.RECORDING_STOPPED,
        origin="webhook_notification",
        meeting_id="123456789",
        recording_id="test_recording_id",
        recording_start_time="test_recording_start",
        topic="test_topic",
    )

    mock_update_status_table.assert_called_with(
        "zip_id",
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


def test_conditional_update_failed(mocker):
    dynamo = boto3.resource("dynamodb")
    mock_table = dynamo.Table("MOCK_TABLE")
    stubber = Stubber(mock_table.meta.client)
    stubber.add_client_error(
        "update_item",
        service_error_code="ConditionalCheckFailedException",
    )
    stubber.add_client_error("update_item", service_error_code="AnyOtherError")
    stubber.activate()

    mocker.patch.object(status, "status_table", mock_table)

    # Expect no exception for ClientError code
    # ConditionalCheckFailedException
    status.set_pipeline_status(
        "zip_id",
        status.PipelineStatus.SENT_TO_UPLOADER,
    )

    # Expect exception for any other ClientError code
    with pytest.raises(ClientError):
        status.set_pipeline_status(
            "zip_id",
            status.PipelineStatus.SENT_TO_UPLOADER,
        )


def test_status_by_seconds_invalid(mocker):
    mocker.patch.object(status, "status_table", "mock_status_table")

    # Must be an hour or less
    with pytest.raises(status.InvalidStatusQuery):
        status.status_by_seconds(status.SECONDS_PER_HOUR + 1)


@freeze_time(
    datetime.strptime("2021-02-14", status.DATE_FORMAT).replace(second=1)
)
def test_status_by_seconds_day_overlap(mocker):
    mocker.patch.object(status, "status_table", "mock_status_table")

    mock_recent_items = mocker.patch.object(status, "request_recent_items")
    mocker.patch.object(status, "format_status_records", mocker.Mock())

    status.status_by_seconds(10)

    mock_recent_items.assert_has_calls(
        calls=[
            mocker.call("mock_status_table", "2021-02-14", 0),
            mocker.call(
                "mock_status_table",
                "2021-02-13",
                status.SECONDS_PER_DAY - 9,
            ),
        ]
    )


@freeze_time(
    datetime.strptime("2021-02-14", status.DATE_FORMAT).replace(second=59)
)
def test_status_by_seconds_no_overlap(mocker):
    mocker.patch.object(status, "status_table", "mock_status_table")

    mock_recent_items = mocker.patch.object(status, "request_recent_items")
    mocker.patch.object(status, "format_status_records", mocker.Mock())

    status.status_by_seconds(10)

    mock_recent_items.assert_has_calls(
        calls=[
            mocker.call("mock_status_table", "2021-02-14", 49),
        ]
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
            "zip_id": "auto-ingest-mEN4a+AfQ1i9TYhk0gkqdg==",
            "topic": "Class Topic",
            "recording_id": "recording_uuid",
            "expiration": 1619805875,
            "pipeline_state": "WEBHOOK_FAILED",
            "meeting_id": 123456789,
            "recording_start_time": "2021-04-23T17:45:24Z",
            "update_date": "2021-04-23",
            "reason": "no mp4 files",
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
                                "status": "WEBHOOK_FAILED",
                                "origin": "webhook_notification",
                                "reason": "no mp4 files",
                            }
                        ],
                    }
                ],
            }
        ]
    }

    assert status.format_status_records(test_items) == expected
