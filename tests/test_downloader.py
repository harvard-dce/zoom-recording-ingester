import site
import os
from os.path import dirname, join
import pytest
from importlib import import_module
import requests
import requests_mock
from datetime import datetime, timedelta
from pytz import timezone
from mock import patch

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

downloader = import_module('zoom-downloader')

LOCAL_TIME_ZONE = os.getenv('LOCAL_TIME_ZONE')
TIMESTAMP_FORMAT = os.getenv('TIMESTAMP_FORMAT')


def test_overlapping_recording_segments():

    now = datetime.now()
    one_minute_ago = (now - timedelta(minutes=1)).strftime(TIMESTAMP_FORMAT)
    two_minutes_ago = (now - timedelta(minutes=2)).strftime(TIMESTAMP_FORMAT)
    now = now.strftime(TIMESTAMP_FORMAT)

    tracks = [
        (None, {'recording_start': two_minutes_ago, 'recording_end': one_minute_ago}, False),
        ({'recording_start': one_minute_ago, 'recording_end': now},
         {'recording_start': one_minute_ago, 'recording_end': now}, False),
        ({'recording_start': two_minutes_ago, 'recording_end': one_minute_ago},
         {'recording_start': one_minute_ago, 'recording_end': now}, True),
        ({'recording_start': two_minutes_ago, 'recording_end': now},
         {'recording_start': one_minute_ago, 'recording_end': now}, downloader.PermanentDownloadError),
        ({'recording_start': two_minutes_ago, 'recording_end': one_minute_ago},
         {'recording_start': two_minutes_ago, 'recording_end': now}, downloader.PermanentDownloadError)
    ]

    for prev_file, file, expected in tracks:
        if isinstance(expected, type):
            with pytest.raises(expected):
                downloader.next_track_sequence(prev_file, file)
        else:
            assert downloader.next_track_sequence(prev_file, file) == expected


def test_recording_data_request(mocker):

    mocker.patch.object(downloader, 'gen_token', return_value=b'foobarbaz')

    # test that auth token gets in the headers properly
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200, json={})
        api_data = downloader.api_request('https://api.example.com/v2/abcd-1234')
        assert api_data == {}
        assert 'Authorization' in req_mock.last_request.headers
        assert req_mock.last_request.headers['Authorization'] == 'Bearer foobarbaz'

    # test simple response returns ok
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200, json={'code': 9999, 'foo': 'bar'})
        api_data = downloader.api_request('https://api.example.com/v2/xyz-789')
        assert api_data['foo'] == 'bar'


def test_verify_recording_status():

    rec_data = [
        ({}, downloader.PermanentDownloadError, "No recordings for this meeting."),
        ({'recording_files': []}, downloader.PermanentDownloadError, "No recordings for this meeting."),
        ({'recording_files': [{'id': 1, 'status': 'not completed'}]}, False, None),
        ({'recording_files': [{'id': 1, 'status': 'completed'}]},
         downloader.ApiResponseParsingFailure, 'missing a download_url'),
        ({'recording_files': [{'id': 1, 'status': 'completed', 'download_url': 'http://example.com/video.mp4'}]},
         True, None)
    ]

    for data, expected, msg in rec_data:
        if isinstance(expected, type) and expected.__base__ == Exception:
            with pytest.raises(expected):
                downloader.verify_recording_status(data)
        else:
            assert downloader.verify_recording_status(data) == expected


def test_get_api_data_timeout_retries(mocker):

    mock_api_request = mocker.patch.object(downloader, 'api_request')
    mocker.patch.object(downloader, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    # fail > retry times
    mock_api_request.side_effect = [
        requests.ConnectTimeout()
        for i in range(downloader.MEETING_LOOKUP_RETRIES + 1)
    ]

    with pytest.raises(downloader.ApiLookupFailure) as exc_info:
        downloader.get_api_data('https://api.example.com/v2/abcd-1234')

    assert mock_api_request.call_count == 3
    assert 'No more retries' in str(exc_info.value)
    return


def test_get_recording_data_validate_retries(mocker):

    mock_api_request = mocker.patch.object(downloader, 'api_request')
    mocker.patch.object(downloader, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    with pytest.raises(downloader.ApiLookupFailure) as exc_info:
        validator = lambda x: False
        downloader.get_api_data('https://api.example.com/v2/abcd-1234', validate_callback=validator)

    assert mock_api_request.call_count == 3
    assert 'No more retries' in str(exc_info.value)
    return


def test_get_recording_data_retries(mocker):

    mock_api_request = mocker.patch.object(downloader, 'api_request')
    mocker.patch.object(downloader, 'MEETING_LOOKUP_RETRY_DELAY', new=0)

    mock_api_request.side_effect = [
        requests.ConnectTimeout(),
        requests.ConnectTimeout(),
        {'foo': 1}
    ]

    resp = downloader.get_api_data('https://api.example.com/v2/abcd-1234')

    assert mock_api_request.call_count == 3
    assert resp['foo'] == 1


def test_recording_data(mocker):
    """
    Test get_recording_data to make sure forward slashes survive url and endpoint url is correct.
    """

    mock_get_api_data = mocker.patch.object(downloader, 'get_api_data')
    mocker.patch.object(downloader, 'remove_incomplete_metadata')
    mocker.patch.object(downloader, 'verify_recording_status')
    calls = [('tCh9CNwpQ4xfRJmPpyWQ==', 'https://api.zoom.us/v2/meetings/tCh9CNwpQ4xfRJmPpyWQ==/recordings'),
             ('/Ch9CNwpQ4xfRJmPpyWQ9/', 'https://api.zoom.us/v2/meetings/%252FCh9CNwpQ4xfRJmPpyWQ9%252F/recordings')]

    for call in calls:

        def side_effect(*args, **kwargs):
            assert (args[0] == call[1])

        mock_get_api_data.side_effect = side_effect
        dl = downloader.Download({'uuid': call[0]})
        dl.recording_data()


def test_duration(mocker):

    now = datetime.now()

    thirty_seconds_ago = (
            now - timedelta(seconds=30)
        ).strftime(TIMESTAMP_FORMAT)

    one_hour_ago = (
            now - timedelta(hours=1) - timedelta(minutes=1)
        ).strftime(TIMESTAMP_FORMAT)
    one_hour = "1:01:00"

    four_hours_ago = (
            now - timedelta(hours=4) - timedelta(minutes=35)
        ).strftime(TIMESTAMP_FORMAT)
    four_hours = "04:35:00"

    now = now.strftime(TIMESTAMP_FORMAT)

    cases = [
        ({'duration': one_hour,
          'start_time': thirty_seconds_ago,
          'end_time': now},
         one_hour,
         'Duration formatted HH:MM:SS should return unchanged.'),
        ({'duration': one_hour,
          'start_time': one_hour_ago,
          'end_time': now},
         one_hour,
         'Duration formatted HH:MM:SS should return unchanged'),
        ({'duration': '',
          'start_time': four_hours_ago,
          'end_time': now},
         four_hours,
         'Duration calculated incorrectly from start_time - end_time'),
        ({'duration': None,
          'start_time': None,
          'end_time': None},
         None,
         'No duration, start_time, or end_time '
         'should return a duration of None'),
        ({'duration': '55:08',
          'start_time': None,
          'end_time': None},
         '00:55:08',
         'Duration formatted MM:SS should return duration formatted HH:MM:SS'),
        ({'duration': 'some invalid duration',
          'start_time': None,
          'end_time': None},
         None,
         'Invalid duration and no start or end time should return None'),
        ({'duration': 'some invalid duration',
          'start_time': thirty_seconds_ago,
          'end_time': None},
         None,
         'Invalid duration and no start time should return None'),
        ({'duration': 'some invalid duration',
          'start_time': thirty_seconds_ago,
          'end_time': now},
         "00:00:{:02d}".format(30),
         'Invalid duration and valid start and end time should return duration'
         ' between start and end in the format HH:MM:SS'),
        ({'duration': 'some invalid duration',
          'start_time': 'abc',
          'end_time': 'def'},
         None,
         'Malformed data should return None for duration'),
        ({'duration': '24:00:00',
          'start_time': None,
          'end_time': None},
         None,
         'The duration format 24:00:00 is invalid, should set duration=None')
    ]

    for data, expected, msg in cases:
        dl = downloader.Download(data)
        assert (dl.duration == expected), msg


def test_shorter_than_minimum_duration():
    cases = [
        ({"duration": "02:45:01"}, False),
        ({"duration": "02:01:21"}, False),
        ({"duration": "00:02:00"}, False),
        ({"duration": "00:01:59"}, True),
        ({"duration": "00:00:35"}, True),
        ({"duration": ""}, False),
    ]

    for data, expected in cases:
        dl = downloader.Download(data)
        assert (dl.shorter_than_minimum_duration == expected)


def local_hour(utc_timestamp):
    utc = datetime.strptime(
        utc_timestamp, TIMESTAMP_FORMAT) \
        .replace(tzinfo=timezone('UTC'))
    hour = utc.astimezone(timezone(LOCAL_TIME_ZONE)).hour
    return "{:02}:00".format(hour)


def test_series_id_from_schedule(mocker):
    # all times in GMT but schedule times are local
    monday = "2019-12-30T10:00:05Z"
    wednesday = "2020-01-01T16:00:05Z"
    thursday = "2020-01-02T16:00:05Z"
    saturday = "2020-01-04T16:00:05Z"

    monday_schedule = {
            "Days": ["M"],
            "Time": [local_hour(monday)],
            "opencast_series_id": "monday_good",
        }
    bad_monday_schedule = {
            "Days": ["M"],
            "Time": [],
            "opencast_series_id": "monday_bad",
        }
    thursday_schedule = {
            "Days": ["R"],
            "Time": [local_hour(thursday)],
            "opencast_series_id": "thursday_good",
        }
    bad_thursday_schedule = {
            "Days": ["Th"],
            "Time": ["11:00"],
            "opencast_series_id": "thursday_bad",
        }
    saturday_schedule = {
            "Days": ["Sa"],
            "Time": [local_hour(saturday)],
            "opencast_series_id": "saturday_good",
        }
    bad_saturday_schedule = {
            "Days": ["Sa"],
            "Time": ["20:00"],
            "opencast_series_id": "saturday_bad",
        }
    multiple_day_schedule = {
            "Days": ["M", "W"],
            "Time": [local_hour(wednesday)],
            "opencast_series_id": "multiple_days",
        }

    cases = [
        # Mondays
        ({"start_time": monday}, monday_schedule,
            monday_schedule["opencast_series_id"]),
        ({"start_time": monday}, bad_monday_schedule, None),
        # Thursdays
        ({"start_time": thursday}, thursday_schedule,
            thursday_schedule["opencast_series_id"]),
        ({"start_time": thursday}, bad_thursday_schedule, None),
        # Saturdays
        ({"start_time": saturday}, saturday_schedule,
            saturday_schedule["opencast_series_id"]),
        ({"start_time": saturday}, bad_saturday_schedule, None),
        # Multiple days
        ({"start_time": wednesday}, multiple_day_schedule,
            multiple_day_schedule["opencast_series_id"])
    ]

    for data, schedule, expected in cases:
        with patch.object(downloader.Download, "class_schedule", schedule):
            dl = downloader.Download(data)
            assert(dl.series_id_from_schedule == expected)


def test_get_stream(mocker):
    mocker.patch.object(downloader, "get_admin_token", return_value="1234")

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
        ({"header": location_header}, mp4_file, None),
        ({"header": content_disposition_header}, mp4_file, None),
        ({}, downloader.PermanentDownloadError,
         "Zoom name not found in headers"),
        ({"header": html_content}, downloader.PermanentDownloadError,
         "Zoom returned stream with content type text/html"),
        ({"header": html_content, "content": b'Error'},
         downloader.PermanentDownloadError, "Zoom returned an HTML error page")
    ]

    for input, expected, msg in cases:
        url = "https://example.com/stream"
        header = input["header"] if "header" in input else {}
        content = input["content"] if "content" in input else b''
        with requests_mock.mock() as req_mock:
            req_mock.get(
                requests_mock.ANY,
                status_code=200,
                headers=header,
                content=content
            )
            if isinstance(expected, type) and expected.__base__ == Exception:
                with pytest.raises(expected) as exc_info:
                    downloader.get_stream(url)
                if msg is not None:
                    assert exc_info.match(msg)
            else:
                stream, zoom_name = downloader.get_stream(url)
                assert(zoom_name == expected)


def test_remove_incomplete_metadata(mocker):
    mp4_complete = {
        "id": "123",
        "meeting_id": "abc",
        "recording_start": "2019-01-31T17:09:11Z",
        "recording_end": "2019-01-31T17:11:21Z",
        "file_type": "MP4",
        "download_url": "url",
        "status": "completed",
        "recording_type": "shared_screen_with_speaker_view"}

    mp4_incomplete = {
        "meeting_id": "abc",
        "recording_start": "2019-01-31T17:09:11Z",
        "recording_end": "2019-01-31T17:11:21Z",
        "file_type": "MP4",
        "download_url": "url",
        "status": "completed",
        "recording_type": "shared_screen_with_speaker_view"}

    non_mp4_complete = {
        "id": "123",
        "meeting_id": "abc",
        "recording_start": "2019-01-31T17:09:11Z",
        "recording_end": "2019-01-31T17:11:21Z",
        "file_type": "CHAT",
        "download_url": "url",
        "status": "completed",
        "recording_type": "shared_screen_with_speaker_view"}

    non_mp4_incomplete = {
        "meeting_id": "abc",
        "recording_start": "2019-01-31T17:09:11Z",
        "recording_end": "2019-01-31T17:11:21Z",
        "file_type": "CHAT",
        "download_url": "url",
        "status": "completed",
        "recording_type": "shared_screen_with_speaker_view"}

    cases = [
        ([mp4_complete, non_mp4_complete, non_mp4_incomplete],
            [mp4_complete, non_mp4_complete], None),
        ([mp4_incomplete], downloader.PermanentDownloadError,
            "MP4 file missing required metadata. {}".format(mp4_incomplete))
    ]

    for files, expected, msg in cases:
        param = {'recording_files': files}
        if isinstance(expected, type):
            with pytest.raises(expected) as exc_info:
                downloader.remove_incomplete_metadata(param)
            if msg is not None:
                assert exc_info.match(msg)
        else:
            assert downloader.remove_incomplete_metadata(param)['recording_files'] == expected


def test_filter_and_sort(mocker):

    now = datetime.now()
    one_minute_ago = (now - timedelta(minutes=1)).strftime(TIMESTAMP_FORMAT)
    two_minutes_ago = (now - timedelta(minutes=2)).strftime(TIMESTAMP_FORMAT)
    now = now.strftime(TIMESTAMP_FORMAT)

    files = [
            {'recording_start': now,
             'file_type': 'MP4',
             'recording_type': 'shared_screen_with_speaker_view'},
            {'recording_start': now,
             'file_type': 'MP4',
             'recording_type': 'shared_screen'},
            {'recording_start': now,
             'file_type': 'MP4',
             'recording_type': 'active_speaker'}
        ]

    expected = [
            {'recording_start': now,
             'file_type': 'MP4',
             'recording_type': 'shared_screen_with_speaker_view'}
        ]

    assert downloader.filter_and_sort(files) == expected

    files = [
        {'recording_start': now,
         'file_type': 'MP4',
         'recording_type': 'shared_screen_with_speaker_view'},
        {'recording_start': one_minute_ago,
         'file_type': 'MP4',
         'recording_type': 'shared_screen'},
        {'recording_start': now,
         'file_type': 'MP4',
         'recording_type': 'active_speaker'}
    ]

    expected = [
        {'recording_start': one_minute_ago,
         'file_type': 'MP4',
         'recording_type': 'shared_screen'},
        {'recording_start': now,
         'file_type': 'MP4',
         'recording_type': 'shared_screen_with_speaker_view'}
    ]

    assert downloader.filter_and_sort(files) == expected

    files = [
        {'recording_start': now,
         'file_type': 'MP4',
         'recording_type': 'shared_screen_with_speaker_view'},
        {'recording_start': one_minute_ago,
         'file_type': 'MP4',
         'recording_type': 'shared_screen'},
        {'recording_start': two_minutes_ago,
         'file_type': 'MP4',
         'recording_type': 'active_speaker'}
    ]

    expected = [
        {'recording_start': two_minutes_ago,
         'file_type': 'MP4',
         'recording_type': 'active_speaker'},
        {'recording_start': one_minute_ago,
         'file_type': 'MP4',
         'recording_type': 'shared_screen'},
        {'recording_start': now,
         'file_type': 'MP4',
         'recording_type': 'shared_screen_with_speaker_view'}
    ]

    assert downloader.filter_and_sort(files) == expected
