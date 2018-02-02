import site
from os.path import dirname, join
import pytest
from importlib import import_module
from datetime import datetime, timedelta

site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

downloader = import_module('zoom-downloader')

EVENT_TEMPLATE = {'Records': [
    {'eventName': 'INSERT',
     'dynamodb': {
         'Keys': {'meeting_uuid': {'S': 'some_uuid'}},
         'NewImage': {
             'meeting_uuid': {'S': 'some_uuid'},
             'recording_data': {'S': 'json_recording_data'}
         }}
     }]}


def test_empty_event(handler):
    res = handler(downloader, {})
    assert res['statusCode'] == 400
    assert res['body'] == "No records in event."


def test_multiple_records(handler):
    res = handler(downloader, {'Records': [1, 2]})
    assert res['statusCode'] == 400
    assert res['body'] == "DynamoDB stream should be set to BatchSize: 1"


def test_ignored_event_types(handler):
    for event_type in ['MODIFY', 'REMOVE']:
        res = handler(downloader, {'Records': [{'eventName': event_type}]})
        assert res['statusCode'] == 204


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
         {'recording_start': one_minute_ago, 'recording_end': now}, downloader.RecordingSegmentsOverlap),
        ({'recording_start': two_minutes_ago, 'recording_end': one_minute_ago},
         {'recording_start': two_minutes_ago, 'recording_end': now}, downloader.RecordingSegmentsOverlap)
    ]

    for prev_file, file, expected in tracks:
        if isinstance(expected, type):
            with pytest.raises(expected):
                downloader.next_track_sequence(prev_file, file)
        else:
            assert downloader.next_track_sequence(prev_file, file) == expected
