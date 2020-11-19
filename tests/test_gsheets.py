import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))
from importlib import import_module
import gsheets
import pytest


def test_schedule_successful_parsing(mocker):
    mock_table_name = "mock_table_name"
    mock_json_to_dynamo = mocker.patch.object(
        gsheets, "schedule_json_to_dynamo"
    )

    schedule1_expected = {
        "0123456789": {
            "course_code": "BIOS E-18",
            "events": [
                {"day": "T", "time": "20:10", "title": "Section"}
            ],
            "opencast_series_id": "20210112345",
            "zoom_series_id": "0123456789"
        },
        "9876543210": {
            "course_code": "BIOS E-18",
            "events": [
                {"day": "M", "time": "19:40", "title": "Lecture"},
                {"day": "W", "time": "19:40", "title": "Lecture"}
            ],
            "opencast_series_id": "20210155555",
            "zoom_series_id": "9876543210"
        },
        "555555555": {
            "course_code": "CSCI E-61",
            "events": [
                {"day": "T", "time": "09:15", "title": "Lecture"},
                {"day": "W", "time": "12:00", "title": "Staff Meeting"},
                {"day": "R", "time": "20:00", "title": "Section"},
                {"day": "F", "time": "13:15", "title": "Lecture"}
            ],
            "opencast_series_id": "20210161616",
            "zoom_series_id": "555555555"
        },
        "123123123": {
            "course_code": "MATH E-55",
            "events": [
                {"day": "T", "time": "14:30", "title": "Lecture"},
                {"day": "R", "time": "14:30", "title": "Lecture"}
            ],
            "opencast_series_id": "20210155155",
            "zoom_series_id": "123123123"
        },
        "100100100": {
            "course_code": "STAT S-100",
            "events": [
                {"day": "S", "time": "18:30", "title": "Section"},
                {"day": "U", "time": "18:30", "title": "Section"}
            ],
            "opencast_series_id": "20210110010",
            "zoom_series_id": "100100100"
        }
    }

    # pass something into schedule csv
    gsheets.schedule_csv_to_dynamo(mock_table_name, "tests/input/schedule1.csv")
    mock_json_to_dynamo.assert_called_with(
        mock_table_name, schedule_data=schedule1_expected
    )


def test_schedule_missing_req_field(mocker):
    with pytest.raises(Exception, match="Missing required field"):
        gsheets.schedule_csv_to_dynamo(
            "mock_table_name", "tests/input/schedule2.csv"
        )


def test_schedule_missing_zoom_link(mocker):
    mock_table_name = "mock_table_name"
    mock_json_to_dynamo = mocker.patch.object(
        gsheets, "schedule_json_to_dynamo"
    )

    # 2 meetings but 1 does not have a Zoom link so only 1 meeting added
    schedule3_expected = {
        "9876543210": {
            "course_code": "BIOS E-18",
            "events": [
                {"day": "M", "time": "19:40", "title": "Lecture"},
                {"day": "W", "time": "19:40", "title": "Lecture"}
            ],
            "opencast_series_id": "20210155555",
            "zoom_series_id": "9876543210"
        }
    }

    gsheets.schedule_csv_to_dynamo(mock_table_name, "tests/input/schedule3.csv")
    mock_json_to_dynamo.assert_called_with(
        "mock_table_name", schedule_data=schedule3_expected
    )


def test_schedule_invalid_zoom_link(mocker):
    mock_table_name = "mock_table_name"
    mock_json_to_dynamo = mocker.patch.object(
        gsheets, "schedule_json_to_dynamo"
    )

    # 2 meetings but 1 has an invalid Zoom link so only 1 meeting added
    schedule4_expected = {
        "9876543210": {
            "course_code": "BIOS E-18",
            "events": [
                {"day": "M", "time": "19:40", "title": "Lecture"},
                {"day": "W", "time": "19:40", "title": "Lecture"}
            ],
            "opencast_series_id": "20210155555",
            "zoom_series_id": "9876543210"
        }
    }

    gsheets.schedule_csv_to_dynamo(mock_table_name, "tests/input/schedule4.csv")
    mock_json_to_dynamo.assert_called_with(
        "mock_table_name", schedule_data=schedule4_expected
    )


def test_schedule_missing_oc_series(mocker):
    mock_table_name = "mock_table_name"
    mock_json_to_dynamo = mocker.patch.object(
        gsheets, "schedule_json_to_dynamo"
    )

    # 2 meetings but 1 has no opencast series id so only 1 meeting added
    schedule5_expected = {
        "9876543210": {
            "course_code": "BIOS E-18",
            "events": [
                {"day": "M", "time": "19:40", "title": "Lecture"},
                {"day": "W", "time": "19:40", "title": "Lecture"}
            ],
            "opencast_series_id": "20210155555",
            "zoom_series_id": "9876543210"
        }
    }

    gsheets.schedule_csv_to_dynamo(mock_table_name, "tests/input/schedule5.csv")
    mock_json_to_dynamo.assert_called_with(
        "mock_table_name", schedule_data=schedule5_expected
    )
