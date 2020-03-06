import site
import os
from os.path import dirname, join
import pytest
from importlib import import_module
import requests_mock
from datetime import datetime, timedelta
from pytz import timezone
from mock import patch

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

downloader = import_module('zoom-downloader')

LOCAL_TIME_ZONE = os.getenv('LOCAL_TIME_ZONE')
TIMESTAMP_FORMAT = os.getenv('TIMESTAMP_FORMAT')

tz = timezone(LOCAL_TIME_ZONE)
FROZEN_TIME = datetime.strftime(tz.localize(datetime.now()), TIMESTAMP_FORMAT)


"""
Tests for class Downloader
"""


def local_hour(timestamp):
    hour = datetime.strptime(
        timestamp, TIMESTAMP_FORMAT) \
        .replace(tzinfo=timezone(LOCAL_TIME_ZONE)).hour
    return "{:02}:00".format(hour)


def test_opencast_series_id(mocker):
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
            "Time": [local_hour(monday_start_time)],
            "opencast_series_id": "monday_good",
        }
    bad_monday_schedule = {
            "Days": ["M"],
            "Time": [],
            "opencast_series_id": "monday_bad",
        }
    thursday_schedule = {
            "Days": ["R"],
            "Time": [local_hour(thursday_start_time)],
            "opencast_series_id": "thursday_good",
        }
    bad_thursday_schedule = {
            "Days": ["Th"],
            "Time": ["11:00"],
            "opencast_series_id": "thursday_bad",
        }
    saturday_schedule = {
            "Days": ["Sa"],
            "Time": [local_hour(saturday_start_time)],
            "opencast_series_id": "saturday_good",
        }
    bad_saturday_schedule = {
            "Days": ["Sa"],
            "Time": ["20:00"],
            "opencast_series_id": "saturday_bad",
        }
    multiple_day_schedule = {
            "Days": ["M", "W"],
            "Time": [local_hour(wednesday_start_time)],
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
