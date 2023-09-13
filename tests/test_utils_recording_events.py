import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

from freezegun import freeze_time
from datetime import datetime, timedelta
from pytz import utc
from utils import recording_events

FROZEN_TIME = utc.localize(datetime.now())
FROZEN_EXPIRATION = int((FROZEN_TIME + timedelta(days=120)).timestamp())


@freeze_time(FROZEN_TIME)
def test_set_recording_events(mocker):
    """
    Test that all event types are recorded to the db
    """
    mock_recording_events_table = mocker.Mock()
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    mock_update_recording_events_table = mocker.patch.object(
        recording_events,
        "update_recording_events_table",
    )

    zoom_uuid = "zip_id"
    zoom_event_ts = 1578621046000
    zoom_events = [
        "recording.started",
        "recording.paused",
        "recording.resumed",
        "recording.stopped",
    ]
    meeting_id = "12345678"

    for event in zoom_events:
        recording_events.set_recording_events(
            zoom_uuid,
            event,
            zoom_event_ts,
            meeting_id,
        )

        new_event_time = [
            {
                "event": event,
                "timestamp": int(zoom_event_ts / 1000),  # in secs
            }
        ]
        mock_update_recording_events_table.assert_called_with(
            mock_recording_events_table,
            "zip_id",
            (
                "SET "
                "expiration = :expiration, "
                "recording_event_times = list_append("
                "if_not_exists(recording_event_times, :empty_list), "
                ":new_recording_event"
                ")"
                ", meeting_id = :meeting_id"
            ),
            {
                ":expiration": FROZEN_EXPIRATION,
                ":new_recording_event": new_event_time,
                ":empty_list": [],
                ":meeting_id": meeting_id,
            },
        )


@freeze_time(FROZEN_TIME)
def test_set_recording_events_no_meeting_id(mocker):
    """
    Test that it's ok not to have a meeting id and that empty meeting id will not
    be saved to the db.
    """
    mock_recording_events_table = mocker.Mock()
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    mock_update_recording_events_table = mocker.patch.object(
        recording_events,
        "update_recording_events_table",
    )

    zoom_uuid = "zip_id"
    zoom_event_ts = 1578621046000
    zoom_events = [
        "recording.started",
        "recording.paused",
        "recording.resumed",
        "recording.stopped",
    ]

    for event in zoom_events:
        recording_events.set_recording_events(
            zoom_uuid,
            event,
            zoom_event_ts,
        )

        new_event_time = [
            {
                "event": event,
                "timestamp": int(zoom_event_ts / 1000),  # in secs
            }
        ]
        mock_update_recording_events_table.assert_called_with(
            mock_recording_events_table,
            "zip_id",
            (
                "SET "
                "expiration = :expiration, "
                "recording_event_times = list_append("
                "if_not_exists(recording_event_times, :empty_list), "
                ":new_recording_event"
                ")"
            ),
            {
                ":expiration": FROZEN_EXPIRATION,
                ":new_recording_event": new_event_time,
                ":empty_list": [],
            },
        )


def test_set_recording_events_event_unknown(mocker):
    """
    Test that an unknown event type will be ignored
    """
    mock_update_recording_events_table = mocker.patch.object(
        recording_events,
        "update_recording_events_table",
    )

    zoom_uuid = "zip_id"
    zoom_event_ts = 1578621046000
    zoom_event = ["meeting.ended"]

    recording_events.set_recording_events(
        zoom_uuid,
        zoom_event,
        zoom_event_ts,
    )

    assert not mock_update_recording_events_table.called


def test_get_recording_segments_from_start_stop(mocker):
    """
    Test that, if no events are recorded via webhook, it will use the start/stop times
    from the recording.
    """
    mock_recording_events_table = mocker.Mock()
    mock_recording_events_table.get_item.return_value = None
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    start_stop_segments = {
        0: {
            "start": "2023-09-13T16:29:32Z",
            "stop": "2023-09-13T18:07:26Z",
        },
        1: {
            "start": "2023-09-13T15:29:32Z",
            "stop": "2023-09-13T16:07:00Z",
        },
    }

    recording_times = recording_events.get_recording_segments(
        "zip_id", start_stop_segments
    )
    assert (
        recording_times
        == "2023-09-13T15:29:32Z_2023-09-13T16:07:00Z,2023-09-13T16:29:32Z_2023-09-13T18:07:26Z"
    )


def test_get_recording_segments_from_all_events(mocker):
    """
    Test that events are merged with start/stop times from recordings.
    """
    mock_recording_events_table = mocker.Mock()
    mock_recording_events_table.get_item.return_value = {
        "Item": {
            "zoom_uuid": "zip_id",
            "recording_event_times": [
                {
                    "event": "recording.started",
                    "timestamp": 1694619861,  # 2023-09-13 15:44:21
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1694619908,  # 2023-09-13 15:45:08
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694619917,  # 2023-09-13 15:45:17
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1694619937,  # 2023-09-13 15:45:37
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694619952,  # 2023-09-13 15:45:52
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1694619969,  # 2023-09-13 15:46:09
                },
                {
                    "event": "recording.started",
                    "timestamp": 1694619983,  # 2023-09-13 15:46:23
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1694619997,  # 2023-09-13 15:46:37
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694620007,  # 2023-09-13 15:46:47
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1694620044,  # 2023-09-13 15:47:24
                },
            ],
        }
    }
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    start_stop_segments = {
        0: {
            "start": "2023-09-13T15:44:21Z",
            "stop": "2023-09-13T15:46:09Z",
        },
        1: {
            "start": "2023-09-13T15:46:23Z",
            "stop": "2023-09-13T15:47:24Z",
        },
    }

    recording_times = recording_events.get_recording_segments(
        "zip_id", start_stop_segments
    )
    assert recording_times == (
        "2023-09-13T15:44:21Z_2023-09-13T15:45:08Z,"
        "2023-09-13T15:45:17Z_2023-09-13T15:45:37Z,"
        "2023-09-13T15:45:52Z_2023-09-13T15:46:09Z,"
        "2023-09-13T15:46:23Z_2023-09-13T15:46:37Z,"
        "2023-09-13T15:46:47Z_2023-09-13T15:47:24Z"
    )


def test_get_recording_segments_unsorted(mocker):
    """
    Test event notifications received in a random order.
    """
    mock_recording_events_table = mocker.Mock()
    mock_recording_events_table.get_item.return_value = {
        "Item": {
            "zoom_uuid": "zip_id",
            "recording_event_times": [
                {
                    "event": "recording.paused",
                    "timestamp": 1694619908,  # 2023-09-13 15:45:08
                },
                {
                    "event": "recording.started",
                    "timestamp": 1694619861,  # 2023-09-13 15:44:21
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1694619937,  # 2023-09-13 15:45:37
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694619917,  # 2023-09-13 15:45:17
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694619952,  # 2023-09-13 15:45:52
                },
                {
                    "event": "recording.started",
                    "timestamp": 1694619983,  # 2023-09-13 15:46:23
                },
                {
                    "event": "recording.started",
                    "timestamp": 1694619983,  # 2023-09-13 15:46:23
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1694620044,  # 2023-09-13 15:47:24
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1694620044,  # 2023-09-13 15:47:24
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1694619969,  # 2023-09-13 15:46:09
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694620007,  # 2023-09-13 15:46:47
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1694619997,  # 2023-09-13 15:46:37
                },
            ],
        }
    }
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    start_stop_segments = {
        0: {
            "start": "2023-09-13T15:44:21Z",
            "stop": "2023-09-13T15:46:09Z",
        },
        1: {
            "start": "2023-09-13T15:46:23Z",
            "stop": "2023-09-13T15:47:24Z",
        },
    }

    recording_times = recording_events.get_recording_segments(
        "zip_id", start_stop_segments
    )
    assert recording_times == (
        "2023-09-13T15:44:21Z_2023-09-13T15:45:08Z,"
        "2023-09-13T15:45:17Z_2023-09-13T15:45:37Z,"
        "2023-09-13T15:45:52Z_2023-09-13T15:46:09Z,"
        "2023-09-13T15:46:23Z_2023-09-13T15:46:37Z,"
        "2023-09-13T15:46:47Z_2023-09-13T15:47:24Z"
    )


def test_get_recording_segments_some_events_missing(mocker):
    """
    Test that we will return the best we can if some events are missing e.g.
    start/stop can be gotten from the recording start/stop, but we can't do
    much if a pause/resume event is missing...
    """
    mock_recording_events_table = mocker.Mock()
    mock_recording_events_table.get_item.return_value = {
        "Item": {
            "zoom_uuid": "zip_id",
            "recording_event_times": [
                {
                    "event": "recording.paused",
                    "timestamp": 1694619908,  # 2023-09-13 15:45:08
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1694619937,  # 2023-09-13 15:45:37
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694619952,  # 2023-09-13 15:45:52
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1694620007,  # 2023-09-13 15:46:47
                },
            ],
        }
    }
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    start_stop_segments = {
        0: {
            "start": "2023-09-13T15:44:21Z",
            "stop": "2023-09-13T15:46:09Z",
        },
        1: {
            "start": "2023-09-13T15:46:23Z",
            "stop": "2023-09-13T15:47:24Z",
        },
    }

    recording_times = recording_events.get_recording_segments(
        "zip_id", start_stop_segments
    )
    assert recording_times == (
        "2023-09-13T15:44:21Z_2023-09-13T15:45:08Z,"
        "2023-09-13T15:45:52Z_2023-09-13T15:46:09Z,"
        "2023-09-13T15:46:23Z_2023-09-13T15:47:24Z"
    )


def test_get_recording_segments_discard_first_segment(mocker):
    """
    Test that we get the times right if the first segment was too short and was discarded.
    """
    mock_recording_events_table = mocker.Mock()
    mock_recording_events_table.get_item.return_value = {
        "Item": {
            "zoom_uuid": "zip_id",
            "recording_event_times": [
                {
                    "event": "recording.started",
                    "timestamp": 1695045606,  # 2023-09-18 14:00:06
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1695045636,  # 2023-09-18 14:00:36
                },
                {
                    "event": "recording.started",
                    "timestamp": 1695045664,  # 2023-09-18 14:01:04
                },
                {
                    "event": "recording.paused",
                    "timestamp": 1695045726,  # 2023-09-18 14:02:06
                },
                {
                    "event": "recording.resumed",
                    "timestamp": 1695045743,  # 2023-09-18 14:02:23
                },
                {
                    "event": "recording.stopped",
                    "timestamp": 1695045797,  # 2023-09-18 14:03:17
                },
            ],
        }
    }
    mocker.patch.object(
        recording_events, "recording_events_table", mock_recording_events_table
    )
    start_stop_segments = {
        1: {
            "start": "2023-09-18T14:00:42Z",
            "stop": "2023-09-18T14:03:16Z",
        },
    }

    recording_times = recording_events.get_recording_segments(
        "zip_id", start_stop_segments
    )
    assert recording_times == (
        "2023-09-18T14:00:42Z_2023-09-18T14:02:06Z,"
        "2023-09-18T14:02:23Z_2023-09-18T14:03:16Z"
    )
