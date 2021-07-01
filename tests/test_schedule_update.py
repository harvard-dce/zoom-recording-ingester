import site
from os.path import dirname, join
from importlib import import_module

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

schedule_update = import_module("schedule-update")


def test_handler(mocker, handler):
    schedule_update.GSHEETS_DOC_ID = "mock_doc_id"
    mock_gsheets = mocker.patch.object(schedule_update, "GSheet")

    cases = [
        ("ZIP", ["ZIP"]),
        ("Sheet1,Sheet2", ["Sheet1", "Sheet2"]),
        ("foo , bar,, baz", ["foo", "bar", "baz"]),
    ]

    for sheet_names, expected in cases:
        schedule_update.GSHEETS_SHEET_NAMES = sheet_names

        res = handler(schedule_update, {})
        assert res["statusCode"] == 200

        calls = [mocker.call(sheet) for sheet in expected]

        mock_gsheets.return_value.import_to_dynamo.assert_has_calls(
            calls=calls
        )
