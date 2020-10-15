import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))
from importlib import import_module
import gsheets


def test_schedule_parsing(mocker):
    mock_json_to_dynamo = mocker.patch.object(
        gsheets, "schedule_json_to_dynamo"
    )

    schedule1_expected = {
        "0123456789": {
            "Days": ["T"],
            "Time": ["20:10"],
            "opencast_series_id": "20210112345",
            "opencast_subject": "BIOS E-18 - Section",
            "zoom_series_id": "0123456789"
        },
        "9876543210": {
            "Days": ["M", "W"],
            "Time": ["19:40"],
            "opencast_series_id": "20210155555",
            "opencast_subject": "BIOS E-18 - Lecture",
            "zoom_series_id": "9876543210"
        },
    }

    # pass something into schedule csv
    gsheets.schedule_csv_to_dynamo("mock_table", "tests/input/schedule1.csv")

    mock_json_to_dynamo.assert_called_once_with(
        "mock_table", schedule_data=schedule1_expected
    )

    # assert schedule json called with...
    pass
