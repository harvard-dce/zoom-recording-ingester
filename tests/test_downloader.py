import site
from os.path import dirname, join
import pytest
from importlib import import_module

import requests
import requests_mock
from datetime import datetime, timedelta

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

downloader = import_module('zoom-downloader')


def test_overlapping_recording_segments():

    now = datetime.now()
    one_minute_ago = (now - timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    two_minutes_ago = (now - timedelta(minutes=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
    now = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    tracks = [
        (None, {'recording_start': two_minutes_ago, 'recording_end': one_minute_ago}, False),
        ({'recording_start': one_minute_ago, 'recording_end': now},
         {'recording_start': one_minute_ago,'recording_end': now}, False),
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


def test_get_host_data(mocker):

    mock_get_api_data = mocker.patch.object(downloader, 'get_api_data')
    mock_get_api_data.return_value = {
        'first_name': 'Testy',
        'last_name': 'McTesterson',
        'email': 'testy@mctesterson.com'
    }

    host_data = downloader.get_host_data('https://api.example.com/v2/tm1234')
    assert host_data == {"host_name": "Testy McTesterson", "host_email": "testy@mctesterson.com"}
    mock_get_api_data.called_once_with('https://api.example.com/v2/tm1234')


def test_get_recording_data(mocker):
    """
    Test get_recording_data to make sure forward slashes survive url and endpoint url is correct.
    """

    mock_get_api_data = mocker.patch.object(downloader, 'get_api_data')
    calls = [('tCh9CNwpQ4xfRJmPpyWQ==', 'https://api.zoom.us/v2/meetings/tCh9CNwpQ4xfRJmPpyWQ==/recordings'),
             ('/Ch9CNwpQ4xfRJmPpyWQ9/', 'https://api.zoom.us/v2/meetings//Ch9CNwpQ4xfRJmPpyWQ9//recordings')]

    for call in calls:

        def side_effect(*args, **kwargs):
            assert (args[0] == call[1])

        mock_get_api_data.side_effect = side_effect
        downloader.get_recording_data(call[0])
