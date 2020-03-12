import site
import os
import json
from os.path import dirname, join
import pytest
from importlib import import_module
import requests_mock
from datetime import datetime
from pytz import timezone
from mock import patch
import unittest
from freezegun import freeze_time
import copy

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

downloader = import_module('zoom-downloader')
downloader.DOWNLOAD_MESSAGES_PER_INVOCATION = 10
downloader.CLASS_SCHEDULE_TABLE = "mock-schedule-table"

LOCAL_TIME_ZONE = os.getenv('LOCAL_TIME_ZONE')
TIMESTAMP_FORMAT = os.getenv('TIMESTAMP_FORMAT')

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(tz.localize(datetime.now()), TIMESTAMP_FORMAT)

SAMPLE_MESSAGE_BODY = {
    "duration": 30,
    "start_time": datetime.strftime(datetime.now(), TIMESTAMP_FORMAT)
}

DAYS = ["M", "T", "W", "R", "F", "Sa", "Sn"]

SAMPLE_SCHEDULE = {
        "Days": [
            DAYS[datetime.strptime(FROZEN_TIME, TIMESTAMP_FORMAT).weekday()]
        ],
        "zoom_series_id": "123456789",
        "opencast_series_id": "20200299999",
        "Time": [
            datetime.strftime(
                datetime.strptime(FROZEN_TIME, TIMESTAMP_FORMAT),
                "%H:%M"
            )
        ],
        "opencast_subject": "TEST E-61"
    }


class MockContext():
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
        self.function_name = "zoom-downloader"


class MockDownloadMessage():
    def __init__(self, body):
        self.body = json.dumps(body)
        self.delete_call_count = 0

    def delete(self):
        self.delete_call_count += 1


"""
Test handler
"""


class TestHandler(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def initfixtures(self, mocker):
        self.mocker = mocker
        self.context = MockContext(aws_request_id="mock-correlation_id")
        self.mock_sqs = unittest.mock.Mock()
        self.mock_sqs().get_queue_by_name.return_value = "mock_queue_name"
        self.mocker.patch.object(downloader, "sqs_resource", self.mock_sqs)

    def test_no_messages_available(self):
        self.mocker.patch.object(
            downloader, "retrieve_message", return_value=None
        )

        with self.assertLogs(level='INFO') as cm:
            resp = downloader.handler({}, self.context)
            log_message = json.loads(cm.output[-1])["message"]
            assert log_message == "No download queue messages available."
            assert not resp

    @freeze_time(FROZEN_TIME)
    def test_no_schedule_match(self):
        """
        Test that after retrieving DOWNLOAD_MESSAGES_PER_INVOCATION
        messages that don't match, the downloader lambda deletes each
        message and returns.
        """

        message = MockDownloadMessage(copy.deepcopy(SAMPLE_MESSAGE_BODY))

        self.mocker.patch.object(
            downloader, "retrieve_message", return_value=message
        )
        self.mocker.patch.object(
            downloader.Download, "_class_schedule", return_value={}
        )

        with self.assertLogs(level="INFO") as cm:
            resp = downloader.handler({}, self.context)

            assert not resp
            assert message.delete_call_count == downloader.DOWNLOAD_MESSAGES_PER_INVOCATION

            log_message = json.loads(cm.output[-1])["message"]
            expected = "No available recordings match the class schedule."
            assert log_message == expected

    def test_schedule_matched(self):
        """
        Test that after a message is found, the handler stops
        retrieving messages.
        """

        no_match_message = MockDownloadMessage(
            copy.deepcopy(SAMPLE_MESSAGE_BODY)
        )

        match_message = MockDownloadMessage(
            copy.deepcopy(SAMPLE_MESSAGE_BODY)
        )

        messages = [no_match_message] * downloader.DOWNLOAD_MESSAGES_PER_INVOCATION
        messages[-1] = match_message

        def mock_messages(*args):
            return messages.pop(0)

        self.mocker.patch.object(
            downloader,
            "retrieve_message",
            side_effect=mock_messages
        )

        series_found = [False] * downloader.DOWNLOAD_MESSAGES_PER_INVOCATION
        series_found[-1] = True

        def mock_series_found(*args):
            return series_found.pop(0)

        self.mocker.patch.object(
            downloader.Download,
            "oc_series_found",
            side_effect=mock_series_found
        )

        self.mocker.patch.object(
            downloader,
            "get_admin_token",
            return_value="mock-admin-token"
        )

        self.mocker.patch.object(downloader.Download, "upload_to_s3")

        mock_upload_msg = {"mock_upload_message": 1234}
        self.mocker.patch.object(
            downloader.Download,
            "send_to_uploader_queue",
            return_value=mock_upload_msg
        )

        with self.assertLogs(level="INFO") as cm:
            resp = downloader.handler({}, self.context)

            assert not resp
            assert no_match_message.delete_call_count == downloader.DOWNLOAD_MESSAGES_PER_INVOCATION - 1
            assert match_message.delete_call_count == 1

            log_message = json.loads(cm.output[-1])["message"]
            expected = {"Sent to uploader": mock_upload_msg}
            assert log_message == expected

    def test_error_while_downloading(self):

        message = MockDownloadMessage(copy.deepcopy(SAMPLE_MESSAGE_BODY))
        self.mocker.patch.object(
            downloader,
            "retrieve_message",
            return_value=message
        )
        self.mocker.patch.object(
            downloader.Download,
            "oc_series_found",
            return_value=True
        )
        self.mocker.patch.object(downloader, "get_admin_token")

        error_msg = "Error while uploading to S3"
        self.mocker.patch.object(
            downloader.Download,
            "upload_to_s3",
            side_effect=downloader.PermanentDownloadError(error_msg)
        )

        mock_deadletter_msg = {
            "Error": error_msg,
            "Sent to deadletter": SAMPLE_MESSAGE_BODY
        }
        self.mocker.patch.object(
            downloader.Download,
            "send_to_deadletter_queue",
            return_value=SAMPLE_MESSAGE_BODY
        )

        with self.assertLogs(level="INFO") as cm:
            with pytest.raises(downloader.PermanentDownloadError) as exc_info:
                downloader.handler({}, self.context)
            assert exc_info.match(error_msg)
            deadletter_log_message = json.loads(cm.output[-2])["message"]
            handler_failed_log_message = json.loads(cm.output[-1])["message"]
            assert deadletter_log_message == mock_deadletter_msg
            assert handler_failed_log_message == "handler failed!"


"""
Tests for class Downloader
"""


class TestDownloader(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def initfixtures(self, mocker):
        self.mocker = mocker
        self.context = MockContext(aws_request_id="mock-correlation_id")
        self.mock_sqs = unittest.mock.Mock()
        self.mock_sqs().get_queue_by_name.return_value = "mock_queue_name"
        self.mocker.patch.object(downloader, "sqs_resource", self.mock_sqs)

    def local_hour(self, timestamp):
        hour = datetime.strptime(
            timestamp, TIMESTAMP_FORMAT) \
            .replace(tzinfo=timezone(LOCAL_TIME_ZONE)).hour
        return "{:02}:00".format(hour)

    def test_series_id_from_schedule(self):
        """
        Test whether schedule matching determines expected opencast series id.
        """
        # all times in GMT but schedule times are local
        monday_start_time = "2019-12-30T10:00:05Z"
        wednesday_start_time = "2020-01-01T16:00:05Z"
        thursday_start_time = "2020-01-02T16:00:05Z"
        saturday_start_time = "2020-01-04T16:00:05Z"

        monday_schedule = {
                "Days": ["M"],
                "Time": [self.local_hour(monday_start_time)],
                "opencast_series_id": "monday_good",
            }
        bad_monday_schedule = {
                "Days": ["M"],
                "Time": [],
                "opencast_series_id": "monday_bad",
            }
        thursday_schedule = {
                "Days": ["R"],
                "Time": [self.local_hour(thursday_start_time)],
                "opencast_series_id": "thursday_good",
            }
        bad_thursday_schedule = {
                "Days": ["Th"],
                "Time": ["11:00"],
                "opencast_series_id": "thursday_bad",
            }
        saturday_schedule = {
                "Days": ["Sa"],
                "Time": [self.local_hour(saturday_start_time)],
                "opencast_series_id": "saturday_good",
            }
        bad_saturday_schedule = {
                "Days": ["Sa"],
                "Time": ["20:00"],
                "opencast_series_id": "saturday_bad",
            }
        multiple_day_schedule = {
                "Days": ["M", "W"],
                "Time": [self.local_hour(wednesday_start_time)],
                "opencast_series_id": "multiple_days",
            }

        cases = [
            # Mondays
            (monday_start_time, monday_schedule,
                monday_schedule["opencast_series_id"]),
            (monday_start_time, bad_monday_schedule, None),
            # Thursdays
            (thursday_start_time, thursday_schedule,
                thursday_schedule["opencast_series_id"]),
            (thursday_start_time, bad_thursday_schedule, None),
            # Saturdays
            (saturday_start_time, saturday_schedule,
                saturday_schedule["opencast_series_id"]),
            (saturday_start_time, bad_saturday_schedule, None),
            # Multiple days
            (wednesday_start_time, multiple_day_schedule,
                multiple_day_schedule["opencast_series_id"])
        ]

        for start_time, schedule, expected in cases:
            time_object = datetime.strptime(start_time, TIMESTAMP_FORMAT)
            with patch.object(
                    downloader.Download, "_class_schedule", schedule):
                with patch.object(
                     downloader.Download, "_created_local", time_object):
                    dl = downloader.Download(None, None)
                    assert(dl._series_id_from_schedule == expected)

    """
    Tests for downloader.oc_series_found()
    """

    def test_oc_series_found(self):
        downloader.MINIMUM_DURATION = 2
        override_id = 12345678
        schedule_id = 44444444
        default_id = 20200299999

        skipped_msg = "Recording duration shorter than {} minutes".format(
                        downloader.MINIMUM_DURATION
                       )

        override_msg = ("Using override series id '{}'"
                        .format(override_id))

        schedule_match_msg = ("Matched with opencast series '{}'!"
                              .format(schedule_id))

        default_series_id_msg = ("Using default series id {}"
                                 .format(default_id))

        no_match_msg = "No opencast series found."

        # data, oc_series_found args, default series,
        # expected ret val, expected log message
        cases = [
            # duration filtering cases
            ({"duration": 1}, (override_id, False), None, False, skipped_msg),
            ({"duration": 2}, (override_id, False), None, True, override_msg),
            ({"duration": 3}, (override_id, False), None, True, override_msg),
            ({"duration": 100}, (override_id, False), None, True, override_msg),

            # override series id with ignore schedule
            (SAMPLE_MESSAGE_BODY, (override_id, True), None, True, override_msg),
            # override series id without ignore schedule
            (SAMPLE_MESSAGE_BODY, (override_id, False), None, True, override_msg),
            # override series id with ignore schedule and default series
            (SAMPLE_MESSAGE_BODY, (override_id, True), default_id, True,
             override_msg),

            # ignore schedule without default series
            (SAMPLE_MESSAGE_BODY, (None, True), None, False, no_match_msg),
            # ignore schedule with default series
            (SAMPLE_MESSAGE_BODY,
             (None, True), default_id, True, default_series_id_msg),

            # regular schedule match
            (SAMPLE_MESSAGE_BODY, (None, False), None, True, schedule_match_msg),

            # default series id when no schedule matched
            (SAMPLE_MESSAGE_BODY, (None, False), default_id, True,
             default_series_id_msg),

            # no match found
            (SAMPLE_MESSAGE_BODY, (None, False), None, False, no_match_msg)
        ]

        for data, args, default_id, expected_ret, expected_msg in cases:
            with self.assertLogs(level='INFO') as cm:
                downloader.DEFAULT_SERIES_ID = default_id
                if expected_msg == schedule_match_msg:
                    self.mocker.patch.object(
                        downloader.Download,
                        "_series_id_from_schedule",
                        schedule_id
                    )
                else:
                    self.mocker.patch.object(
                        downloader.Download,
                        "_series_id_from_schedule",
                        None
                    )

                dl = downloader.Download(None, data)

                ret = dl.oc_series_found(
                            override_series_id=args[0],
                            ignore_schedule=args[1]
                         )
                assert ret == expected_ret

                log_message = cm.output[-1].split(":")[-1]
                assert log_message == expected_msg


"""
Tests for class ZoomFile
"""


def test_zoom_filename(mocker):
    """
    Test whether the zoom's name for the recording file can be extracted from
    the stream header and check for appropriate error messages.
    """
    mp4_file = "zoom_file.mp4"
    location_header = {
        "Location": "https://zoom.us/{}?123456".format(mp4_file)
    }
    content_disposition_header = {
        "Content-Disposition": "key={}".format(mp4_file)
    }
    html_content = {
        "Content-Type": "text/html"
    }

    cases = [
        # get zoom filename from header
        ({"header": location_header}, mp4_file, None),
        # don't get zoom filename from header
        ({"header": content_disposition_header}, mp4_file, None),
        ({}, downloader.PermanentDownloadError,
         "Zoom name not found in headers"),
        # invalid content type returned
        ({"header": html_content}, downloader.PermanentDownloadError,
         "Zoom returned stream with content type text/html"),
        # returned html error page
        ({"header": html_content, "content": b'Error'},
         downloader.PermanentDownloadError, "Zoom returned an HTML error page")
    ]

    file_data = {
        "meeting_uuid": 1234,
        "download_url": "https://example.com/stream",
        "recording_id": 1234,
        "file_type": "mp4",
        "recording_start": "2020-01-09T19:50:46Z",
        "recording_end": "2020-01-09T21:50:46Z",
        "recording_type": "speaker"
    }

    for http_resp, expected, msg in cases:
        header = http_resp["header"] if "header" in http_resp else {}
        content = http_resp["content"] if "content" in http_resp else b''
        downloader.ADMIN_TOKEN = "super-secret-admin-token"
        with requests_mock.mock() as req_mock:
            req_mock.get(
                requests_mock.ANY,
                status_code=200,
                headers=header,
                content=content
            )
            file = downloader.ZoomFile(file_data, 1)
            if isinstance(expected, type) and expected.__base__ == Exception:
                with pytest.raises(expected) as exc_info:
                    file.zoom_filename
                if msg is not None:
                    assert exc_info.match(msg)
            else:
                assert(file.zoom_filename == expected)
