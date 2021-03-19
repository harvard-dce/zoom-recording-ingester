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

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

downloader = import_module("zoom-downloader")
downloader.DOWNLOAD_MESSAGES_PER_INVOCATION = 10
downloader.CLASS_SCHEDULE_TABLE = "mock-schedule-table"

LOCAL_TIME_ZONE = os.getenv("LOCAL_TIME_ZONE")
TIMESTAMP_FORMAT = os.getenv("TIMESTAMP_FORMAT")

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(tz.localize(datetime.now()), TIMESTAMP_FORMAT)

SAMPLE_MESSAGE_BODY = {
    "correlation_id": "abc",
    "duration": 30,
    "start_time": datetime.strftime(datetime.now(), TIMESTAMP_FORMAT),
}

DAYS = ["M", "T", "W", "R", "F", "S", "U"]


class MockContext:
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
        self.function_name = "zoom-downloader"


class MockDownloadMessage:
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

        with self.assertLogs(level="INFO") as cm:
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
        mock_set_pipeline_status = self.mocker.Mock(return_value=None)
        self.mocker.patch.object(
            downloader, "set_pipeline_status", mock_set_pipeline_status
        )

        with self.assertLogs(level="INFO") as cm:
            resp = downloader.handler({}, self.context)

            assert not resp
            assert (
                message.delete_call_count
                == downloader.DOWNLOAD_MESSAGES_PER_INVOCATION
            )

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

        match_message = MockDownloadMessage(copy.deepcopy(SAMPLE_MESSAGE_BODY))

        mock_set_pipeline_status = self.mocker.Mock(return_value=None)
        self.mocker.patch.object(
            downloader, "set_pipeline_status", mock_set_pipeline_status
        )

        messages = [
            no_match_message
        ] * downloader.DOWNLOAD_MESSAGES_PER_INVOCATION
        messages[-1] = match_message

        def mock_messages(*args):
            return messages.pop(0)

        self.mocker.patch.object(
            downloader, "retrieve_message", side_effect=mock_messages
        )

        series_found = [False] * downloader.DOWNLOAD_MESSAGES_PER_INVOCATION
        series_found[-1] = True

        def mock_series_found(*args):
            return series_found.pop(0)

        self.mocker.patch.object(
            downloader.Download,
            "oc_series_found",
            side_effect=mock_series_found,
        )

        self.mocker.patch.object(
            downloader, "get_admin_token", return_value="mock-admin-token"
        )

        self.mocker.patch.object(downloader.Download, "upload_to_s3")

        mock_upload_msg = {"mock_upload_message": 1234}
        self.mocker.patch.object(
            downloader.Download,
            "send_to_uploader_queue",
            return_value=mock_upload_msg,
        )

        with self.assertLogs(level="INFO") as cm:
            resp = downloader.handler({}, self.context)

            assert not resp
            assert (
                no_match_message.delete_call_count
                == downloader.DOWNLOAD_MESSAGES_PER_INVOCATION - 1
            )
            assert match_message.delete_call_count == 1

            log_message = json.loads(cm.output[-1])["message"]
            expected = {"sqs_message": mock_upload_msg}
            assert log_message == expected

    def test_error_while_downloading(self):

        message = MockDownloadMessage(copy.deepcopy(SAMPLE_MESSAGE_BODY))
        self.mocker.patch.object(
            downloader, "retrieve_message", return_value=message
        )
        self.mocker.patch.object(
            downloader.Download, "oc_series_found", return_value=True
        )
        self.mocker.patch.object(downloader, "get_admin_token")
        mock_set_pipeline_status = self.mocker.Mock(return_value=None)
        self.mocker.patch.object(
            downloader, "set_pipeline_status", mock_set_pipeline_status
        )

        error_msg = "Error while uploading to S3"
        self.mocker.patch.object(
            downloader.Download,
            "upload_to_s3",
            side_effect=downloader.PermanentDownloadError(error_msg),
        )

        mock_deadletter_msg = {
            "Error": error_msg,
            "Sent to deadletter": SAMPLE_MESSAGE_BODY,
        }
        self.mocker.patch.object(
            downloader.Download,
            "send_to_deadletter_queue",
            return_value=SAMPLE_MESSAGE_BODY,
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
        hour = (
            datetime.strptime(timestamp, TIMESTAMP_FORMAT)
            .replace(tzinfo=timezone(LOCAL_TIME_ZONE))
            .hour
        )
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
            "events": [
                {
                    "day": "M",
                    "time": self.local_hour(monday_start_time),
                    "title": "Lecture",
                }
            ],
            "opencast_series_id": "monday_good",
        }
        thursday_schedule = {
            "events": [
                {
                    "day": "R",
                    "time": self.local_hour(thursday_start_time),
                    "title": "Lecture",
                }
            ],
            "opencast_series_id": "thursday_good",
        }
        bad_thursday_schedule = {
            "events": [{"day": "Th", "time": "11:00", "title": "Lecture"}],
            "opencast_series_id": "thursday_bad",
        }
        saturday_schedule = {
            "events": [
                {
                    "day": "S",
                    "time": self.local_hour(saturday_start_time),
                    "title": "Lecture",
                }
            ],
            "opencast_series_id": "saturday_good",
        }
        bad_saturday_schedule = {
            "events": [{"day": "S", "time": "20:00", "title": "Lecture"}],
            "opencast_series_id": "saturday_bad",
        }
        multiple_day_schedule = {
            "events": [
                {
                    "day": "M",
                    "time": self.local_hour(wednesday_start_time),
                    "title": "Lecture",
                },
                {
                    "day": "W",
                    "time": self.local_hour(wednesday_start_time),
                    "title": "Lecture",
                },
            ],
            "opencast_series_id": "multiple_days",
        }

        cases = [
            # Mondays
            (
                monday_start_time,
                monday_schedule,
                monday_schedule["opencast_series_id"],
            ),
            # Thursdays
            (
                thursday_start_time,
                thursday_schedule,
                thursday_schedule["opencast_series_id"],
            ),
            (thursday_start_time, bad_thursday_schedule, None),
            # Saturdays
            (
                saturday_start_time,
                saturday_schedule,
                saturday_schedule["opencast_series_id"],
            ),
            (saturday_start_time, bad_saturday_schedule, None),
            # Multiple days
            (
                wednesday_start_time,
                multiple_day_schedule,
                multiple_day_schedule["opencast_series_id"],
            ),
        ]

        for start_time, schedule, expected in cases:
            time_object = datetime.strptime(start_time, TIMESTAMP_FORMAT)
            with patch.object(
                downloader.Download, "_class_schedule", schedule
            ):
                with patch.object(
                    downloader.Download, "_created_local", time_object
                ):
                    dl = downloader.Download(None, None)
                    assert dl._series_id_from_schedule == expected

    """
    Tests for downloader.oc_series_found()
    """

    def test_oc_series_found(self):
        downloader.MINIMUM_DURATION = 2
        override_id = 12345678
        schedule_id = 44444444
        default_id = 20200299999

        override_msg = "Using override series id '{}'".format(override_id)

        schedule_match_msg = "Matched with opencast series '{}'!".format(
            schedule_id
        )

        default_series_id_msg = "Using default series id {}".format(default_id)

        no_match_msg = "No opencast series found."

        # data, oc_series_found args, default series,
        # expected ret val, expected log message
        cases = [
            # duration filtering cases
            ({"duration": 2}, (override_id, False), None, True, override_msg),
            ({"duration": 3}, (override_id, False), None, True, override_msg),
            (
                {"duration": 100},
                (override_id, False),
                None,
                True,
                override_msg,
            ),
            # override series id with ignore schedule
            (
                SAMPLE_MESSAGE_BODY,
                (override_id, True),
                None,
                True,
                override_msg,
            ),
            # override series id without ignore schedule
            (
                SAMPLE_MESSAGE_BODY,
                (override_id, False),
                None,
                True,
                override_msg,
            ),
            # override series id with ignore schedule and default series
            (
                SAMPLE_MESSAGE_BODY,
                (override_id, True),
                default_id,
                True,
                override_msg,
            ),
            # ignore schedule without default series
            (SAMPLE_MESSAGE_BODY, (None, True), None, False, no_match_msg),
            # ignore schedule with default series
            (
                SAMPLE_MESSAGE_BODY,
                (None, True),
                default_id,
                True,
                default_series_id_msg,
            ),
            # regular schedule match
            (
                SAMPLE_MESSAGE_BODY,
                (None, False),
                None,
                True,
                schedule_match_msg,
            ),
            # default series id when no schedule matched
            (
                SAMPLE_MESSAGE_BODY,
                (None, False),
                default_id,
                True,
                default_series_id_msg,
            ),
            # no match found, no default series id
            (SAMPLE_MESSAGE_BODY, (None, False), None, False, no_match_msg),
            (SAMPLE_MESSAGE_BODY, (None, False), "None", False, no_match_msg),
            (SAMPLE_MESSAGE_BODY, (None, False), "", False, no_match_msg),
        ]

        for data, args, default_id, expected_ret, expected_msg in cases:
            with self.assertLogs(level="INFO") as cm:
                downloader.DEFAULT_SERIES_ID = default_id
                if expected_msg == schedule_match_msg:
                    self.mocker.patch.object(
                        downloader.Download,
                        "_series_id_from_schedule",
                        schedule_id,
                    )
                else:
                    self.mocker.patch.object(
                        downloader.Download, "_series_id_from_schedule", None
                    )

                dl = downloader.Download(None, data)

                ret = dl.oc_series_found(
                    override_series_id=args[0], ignore_schedule=args[1]
                )
                assert ret == expected_ret

                log_message = cm.output[-1].split(":")[-1]
                assert log_message == expected_msg

    def test_upload_to_s3(self):

        now = datetime.strftime(datetime.now(), TIMESTAMP_FORMAT)
        data = {
            "uuid": "abc",
            "zoom_series_id": "02334",
            "start_time": now,
            "recording_files": [
                {"recording_start": now, "recording_type": "view_type_1"},
                {"recording_start": now, "recording_type": "view_type_2"},
                {"recording_start": now, "recording_type": "view_type_3"},
            ],
        }

        cases = [
            # all download links valid
            (["working", "working", "working"], 3, None, None),
            # 2 of 3 valid download links
            (["working", "broken", "working"], 2, None, None),
            # 1 of 3 valid download links
            (["working", "broken", "broken"], 1, None, None),
            # no valid download links
            (
                ["broken", "broken", "broken"],
                0,
                downloader.PermanentDownloadError,
                "No files could be downloaded for this recording",
            ),
        ]

        for links, expected_count, expected_error, error_msg in cases:

            def mock_stream_file_to_s3(*args):
                link = links.pop(0)
                if link == "broken":
                    raise downloader.ZoomDownloadLinkError

            self.mocker.patch.object(
                downloader.ZoomFile,
                "stream_file_to_s3",
                side_effect=mock_stream_file_to_s3,
            )

            dl = downloader.Download(None, data)
            if expected_error:
                with pytest.raises(expected_error) as exc_info:
                    dl.upload_to_s3()
                assert exc_info.match(error_msg)
            else:
                dl.upload_to_s3()
                assert len(dl.downloaded_files) == expected_count


"""
Tests for class ZoomFile
"""


def test_zoom_filename(mocker):
    """
    Test whether the zoom's name for the recording file can be extracted from
    the stream header and check for broken download link.
    """
    mp4_file = "zoom_file.mp4"
    location_header = {
        "Location": "https://zoom.us/{}?123456".format(mp4_file)
    }
    content_disposition_header = {
        "Content-Disposition": "key={}".format(mp4_file)
    }
    html_content_header = {"Content-Type": "text/html;charset=utf-8"}

    cases = [
        # get zoom filename from header
        ({"header": location_header}, mp4_file, None),
        # don't get zoom filename from header
        ({"header": content_disposition_header}, mp4_file, None),
        (
            {},
            downloader.PermanentDownloadError,
            "Zoom name not found in headers",
        ),
        # broken download link (returns html error page instead of stream)
        (
            {"header": html_content_header},
            downloader.ZoomDownloadLinkError,
            None,
        ),
    ]

    file_data = {
        "meeting_uuid": 1234,
        "download_url": "https://example.com/stream",
        "recording_id": 1234,
        "file_type": "mp4",
        "recording_start": "2020-01-09T19:50:46Z",
        "recording_end": "2020-01-09T21:50:46Z",
        "recording_type": "speaker",
    }

    for http_resp, expected, msg in cases:
        header = http_resp["header"] if "header" in http_resp else {}
        content = http_resp["content"] if "content" in http_resp else b""
        downloader.ADMIN_TOKEN = "super-secret-admin-token"
        with requests_mock.mock() as req_mock:
            req_mock.get(
                requests_mock.ANY,
                status_code=200,
                headers=header,
                content=content,
            )
            file = downloader.ZoomFile(file_data, 1)
            if isinstance(expected, type) and expected.__base__ == Exception:
                with pytest.raises(expected) as exc_info:
                    file.zoom_filename
                if msg is not None:
                    assert exc_info.match(msg)
            else:
                assert file.zoom_filename == expected


def test_handler_duration_check(handler, mocker):
    downloader.DOWNLOAD_MESSAGES_PER_INVOCATION = 1
    mocker.patch.object(downloader, "sqs_resource", mocker.Mock())
    mocker.patch.object(
        downloader.Download, "oc_series_found", mocker.Mock(return_value=False)
    )
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        downloader, "set_pipeline_status", mock_set_pipeline_status
    )

    # duration should be good
    mock_msg = mocker.Mock(
        body=json.dumps({"duration": 10, "correlation_id": "abc"})
    )
    mocker.patch.object(
        downloader, "retrieve_message", mocker.Mock(return_value=mock_msg)
    )
    handler(downloader, {})

    # if we got here it means we passed the duration check
    assert downloader.Download.oc_series_found.call_count == 1
    assert mock_msg.delete.call_count == 1

    # Reset
    downloader.Download.oc_series_found.reset_mock()

    # duration should be too short
    mock_msg = mocker.Mock(
        body=json.dumps({"duration": 1, "correlation_id": "abc"})
    )
    mocker.patch.object(
        downloader, "retrieve_message", mocker.Mock(return_value=mock_msg)
    )
    handler(downloader, {})

    # should not have gotten here past the duration check
    assert downloader.Download.oc_series_found.call_count == 0
    assert mock_msg.delete.call_count == 1


def test_ignore_duration_check_for_on_demand(handler, mocker):
    downloader.DOWNLOAD_MESSAGES_PER_INVOCATION = 1
    mocker.patch.object(downloader, "sqs_resource", mocker.Mock())
    mocker.patch.object(
        downloader.Download, "oc_series_found", mocker.Mock(return_value=False)
    )
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        downloader, "set_pipeline_status", mock_set_pipeline_status
    )

    # duration too short
    mock_msg = mocker.Mock(
        body=json.dumps(
            {
                "on_demand_series_id": 1234,
                "duration": 0,
                "correlation_id": "abc",
            }
        )
    )
    mocker.patch.object(
        downloader, "retrieve_message", mocker.Mock(return_value=mock_msg)
    )
    handler(downloader, {})

    # if we got here it means we passed the duration check
    # should pass because the ingest is an on demand ingest
    assert downloader.Download.oc_series_found.call_count == 1
    assert mock_msg.delete.call_count == 1


@pytest.fixture
def download(mocker):
    return downloader.Download(
        sqs=mocker.Mock(),
        data={
            "uuid": "abcde-12345",
            "zoom_series_id": "567890",
            "start_time": "2020-01-01T00:00:00Z",
        },
    )


def test_recording_files(download):
    download.data.update(
        {
            "recording_files": [
                {
                    "recording_start": "2020-03-31T12:00:00Z",
                    "recording_end": "2020-03-31T11:00:00Z",
                    "recording_type": "foo",
                },
                {
                    "recording_start": "2020-03-31T12:00:00Z",
                    "recording_end": "2020-03-31T11:00:00Z",
                    "recording_type": "bar",
                },
            ]
        }
    )

    zoom_files = download.recording_files
    assert len(zoom_files) == 2
    assert all(x.segment_num == 0 for x in zoom_files)


def test_recording_files_multi_set(download):
    test_data = [
        ("2020-03-31T12:00:00Z", "2020-03-31T13:30:00Z", "foo"),
        ("2020-03-31T12:00:00Z", "2020-03-31T13:30:00Z", "bar"),
        ("2020-03-31T13:00:00Z", "2020-03-31T13:30:00Z", "foo"),
        ("2020-03-31T13:00:00Z", "2020-03-31T13:30:00Z", "bar"),
        ("2020-03-31T13:00:00Z", "2020-03-31T13:30:00Z", "baz"),
    ]
    download.data.update(
        {
            "recording_files": [
                {"recording_start": x, "recording_end": y, "recording_type": z}
                for x, y, z in test_data
            ]
        }
    )

    zoom_files = download.recording_files
    assert len(zoom_files) == 5
    assert sum(1 for x in zoom_files if x.segment_num == 1) == 3


@pytest.fixture
def zoomfile(mocker):
    return downloader.ZoomFile({"recording_type": "speaker"}, 0)


def test_zoom_filename_ext(mocker, zoomfile):
    for filename, expected in [
        ("/foo/bar/baz", ""),
        ("http://well.what.have.we/here.tar.gz", "gz"),
        ("path/to/some.mp4", "mp4"),
        ("https://s3.amazonaws.com/bucket/object.html", "html"),
        ("/wtf/this.path/has a dot/in-the-midd.le", "le"),
    ]:
        zoomfile._zoom_filename = filename
        assert zoomfile.file_extension == expected
