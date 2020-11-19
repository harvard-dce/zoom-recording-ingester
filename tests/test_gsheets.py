import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))
from importlib import import_module
import gsheets


def test_schedule_parsing(mocker):
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
        }
    }

    schedule2_expected = {
        "0123456789": {
            "course_code": "BIOS E-18",
            "events": [
                {"day": "T", "time": "20:10", "title": "Section"}
            ],
            "opencast_series_id": "20210112345",
            "zoom_series_id": "0123456789"
        }
    }

    # pass something into schedule csv
    gsheets.schedule_csv_to_dynamo(mock_table_name, "tests/input/schedule1.csv")
    mock_json_to_dynamo.assert_called_with(
        mock_table_name, schedule_data=schedule1_expected
    )

    gsheets.schedule_csv_to_dynamo(mock_table_name, "tests/input/schedule2.csv")
    mock_json_to_dynamo.assert_called_with(
        mock_table_name, schedule_data=schedule2_expected
    )
