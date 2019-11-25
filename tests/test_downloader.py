import site
from os.path import dirname, join
import pytest
from importlib import import_module
import requests
import requests_mock
from datetime import datetime, timedelta

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

downloader = import_module('zoom-downloader')

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


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

    short_duration_start = (
            now - timedelta(seconds=30)
        ).strftime(TIMESTAMP_FORMAT)
    short_duration = "00:00:30"

    long_duration_start = (
            now - timedelta(hours=4) - timedelta(minutes=35)
        ).strftime(TIMESTAMP_FORMAT)
    long_duration = "04:35:00"

    now = now.strftime(TIMESTAMP_FORMAT)

    cases = [
        ({'duration': '02:35:15',
          'start_time': short_duration_start,
          'end_time': now},
         '02:35:15',
         'Duration formatted HH:MM:SS should return unchanged.'),
        ({'duration': '',
          'start_time': long_duration_start,
          'end_time': now},
         long_duration,
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
          'start_time': short_duration_start,
          'end_time': None},
         None,
         'Invalid duration and no start time should return None'),
        ({'duration': 'some invalid duration',
          'start_time': short_duration_start,
          'end_time': now},
         "00:00:{:02d}".format(30),
         'Invalid duration and valid start and end time should return duration'
         ' between start and end in the format HH:MM:SS'),
        ({'duration': 'some invalid duration',
          'start_time': 'abc',
          'end_time': 'def'},
         None,
         'Malformed data should return None for duration'),
    ]

    for data, expected, msg in cases:
        dl = downloader.Download(data)
        assert (dl.duration == expected), msg


def test_shorter_than_minimum_duration(mocker):
    cases = [
        ({'duration': "02:45:01"}, False),
        ({'duration': "00:02:00"}, False),
        ({'duration': "00:01:59"}, True),
        ({'duration': "00:00:35"}, True),
        ({'duration': ""}, False),
    ]

    for data, expected in cases:
        dl = downloader.Download(data)
        assert (dl.shorter_than_minimum_duration == expected)


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
