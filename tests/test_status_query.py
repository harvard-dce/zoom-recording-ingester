import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

from utils import InvalidStatusQuery

import status_query


def test_successful_request(handler, mocker):
    mocker.patch.object(
        status_query,
        "status_by_mid",
        mocker.Mock(return_value={}),
    )

    event = {"queryStringParameters": {"meeting_id": "123456789"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 200

    mocker.patch.object(
        status_query,
        "status_by_seconds",
        mocker.Mock(return_value={}),
    )

    event = {"queryStringParameters": {"seconds": "5"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 200


def test_missing_query_param(handler, mocker):
    # No query params
    event = {"queryStringParameters": None}
    res = handler(status_query, event)
    assert res["statusCode"] == 400

    # Unrecognized query params
    event = {"queryStringParameters": {"invalid_param": "invalid_param_value"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 400


def test_invalid_meeting_id(handler, mocker):
    # Must be a number
    event = {"queryStringParameters": {"meeting_id": "abc"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 400

    # Must exist in the status table
    invalid_status_query_mock = mocker.Mock()
    invalid_status_query_mock.side_effect = InvalidStatusQuery
    mocker.patch.object(
        status_query,
        "status_by_mid",
        invalid_status_query_mock,
    )

    event = {"queryStringParameters": {"meeting_id": "123456789"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 400


def test_invalid_seconds(handler, mocker):
    # Must be a number
    event = {"queryStringParameters": {"seconds": "abc"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 400

    # Must exist in the status table
    invalid_status_query_mock = mocker.Mock()
    invalid_status_query_mock.side_effect = InvalidStatusQuery
    mocker.patch.object(
        status_query,
        "status_by_seconds",
        invalid_status_query_mock,
    )

    event = {"queryStringParameters": {"seconds": "-5"}}
    res = handler(status_query, event)
    assert res["statusCode"] == 400
