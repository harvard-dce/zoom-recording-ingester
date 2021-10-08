import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

import pytest
import jwt
import time
import requests
import requests_mock
import utils
import logging
from datetime import datetime


@pytest.mark.parametrize(
    "key,secret,seconds_valid",
    [
        ("foo", "bar", 10),
        ("abcd-1234", "my-secret-key", 60),
        ("23kljh4jh3jkh_asd008", "asdlkufh9080a9sdufkjn80989sdf", 1000),
    ],
)
def test_gen_token(key, secret, seconds_valid):
    token = utils.common.gen_token(key, secret, seconds_valid=seconds_valid)
    payload = jwt.decode(token, secret, algorithms=["HS256"])
    assert payload["iss"] == key

    # should be within a second
    now = int(time.time())
    assert payload["exp"] - (now + seconds_valid) in [0, -1]


def test_zoom_api_request_missing_endpoint():
    with pytest.raises(Exception) as exc_info:
        utils.zoom_api_request(endpoint=None)
    assert exc_info.match("Call to zoom_api_request missing endpoint")


def test_zoom_api_request_missing_creds():
    utils.common.APIGEE_KEY = None

    # (ZOOM_API_KEY, ZOOM_API_SECRET)
    cases = [(None, None), ("key", None), (None, "secret")]

    for key, secret in cases:
        utils.common.ZOOM_API_KEY = key
        utils.common.ZOOM_API_SECRET = secret
        with pytest.raises(Exception) as exc_info:
            utils.zoom_api_request("meetings")
        assert exc_info.match("Missing api credentials.")


def test_apigee_key(caplog):
    utils.common.APIGEE_KEY = "apigee_key"
    utils.common.ZOOM_API_KEY = None
    utils.common.ZOOM_API_SECRET = None
    caplog.set_level(logging.INFO)
    utils.zoom_api_request("meetings")
    assert "apigee request" in caplog.text.lower()

    # Should still use apigee key even if zoom api key/secret defined
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    utils.zoom_api_request("meetings")
    assert "apigee request" in caplog.text.lower()


def test_zoom_api_key(caplog):
    utils.common.APIGEE_KEY = None
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    caplog.set_level(logging.INFO)
    utils.zoom_api_request("meetings")
    assert "zoom api request" in caplog.text.lower()

    utils.common.APIGEE_KEY = ""
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    caplog.set_level(logging.INFO)
    utils.zoom_api_request("meetings")
    assert "zoom api request" in caplog.text.lower()


def test_url_construction(caplog):
    utils.common.APIGEE_KEY = None
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    caplog.set_level(logging.INFO)

    cases = [
        ("https://www.foo.com", "meetings", "https://www.foo.com/meetings"),
        ("https://www.foo.com/", "meetings", "https://www.foo.com/meetings"),
        ("https://www.foo.com/", "/meetings", "https://www.foo.com/meetings"),
    ]
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY, status_code=200, json={"mock_payload": 123}
        )
        for url, endpoint, expected in cases:
            utils.common.ZOOM_API_BASE_URL = url
            utils.zoom_api_request(endpoint)
            assert (
                "zoom api request to https://www.foo.com/meetings"
                in caplog.text.lower()
            )


def test_zoom_api_request_success():
    # test successful call
    cases = [
        (None, "zoom_key", "zoom_secret"),
        ("", "zoom_key", "zoom_secret"),
    ]
    for apigee_key, zoom_key, zoom_secret in cases:
        utils.common.APIGEE_KEY = apigee_key
        utils.common.ZOOM_API_KEY = zoom_key
        utils.common.ZOOM_API_SECRET = zoom_secret

        with requests_mock.mock() as req_mock:
            req_mock.get(
                requests_mock.ANY, status_code=200, json={"mock_payload": 123}
            )
            r = utils.zoom_api_request("meetings")
            assert "mock_payload" in r.json()


def test_zoom_api_request_failures():
    utils.common.APIGEE_KEY = None
    utils.common.ZOOM_API_KEY = "zoom_key"
    utils.common.ZOOM_API_SECRET = "zoom_secret"
    utils.common.ZOOM_API_BASE_URL = "https://api.zoom.us/v2/"
    # test failed call that returns
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY, status_code=400, json={"mock_payload": 123}
        )
        r = utils.zoom_api_request("meetings", ignore_failure=True)
        assert r.status_code == 400

    # test failed call that raises
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY, status_code=400, json={"mock_payload": 123}
        )
        error_msg = "400 Client Error"
        with pytest.raises(requests.exceptions.HTTPError, match=error_msg):
            utils.zoom_api_request("meetings", ignore_failure=False, retries=0)

    # test ConnectionError handling
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY, exc=requests.exceptions.ConnectionError
        )
        error_msg = "Error requesting https://api.zoom.us/v2/meetings"
        with pytest.raises(utils.common.ZoomApiRequestError, match=error_msg):
            utils.zoom_api_request("meetings")

    # test ConnectTimeout handling
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, exc=requests.exceptions.ConnectTimeout)
        error_msg = "Error requesting https://api.zoom.us/v2/meetings"
        with pytest.raises(utils.common.ZoomApiRequestError, match=error_msg):
            utils.zoom_api_request("meetings")


def test_buffer_minutes(monkeypatch):
    start_time = datetime.now().replace(hour=15, minute=0)
    event_day = list(utils.schedule_days.keys())[start_time.weekday()]
    schedule = {
        "events": [
            {
                "title": "Foo",
                "day": event_day,
                "time": start_time.strftime("%H:%M"),
            },
        ],
    }
    # should match exactly
    match = utils.schedule_match(schedule, start_time)
    assert match["title"] == "Foo"

    # missed schedule by one hour
    match = utils.schedule_match(
        schedule,
        start_time.replace(hour=16),
    )
    assert match is None

    # 40 minutes off with buffer set to 30 minutes (no match)
    with monkeypatch.context() as mp:
        mp.setattr("utils.common.BUFFER_MINUTES", 30)
        match = utils.schedule_match(
            schedule,
            start_time.replace(hour=14, minute=20),
        )
        assert match is None

        # extend the buffer to 45m and try again
        mp.setattr("utils.common.BUFFER_MINUTES", 45)
        match = utils.schedule_match(
            schedule,
            start_time.replace(hour=14, minute=20),
        )
        assert match["title"] == "Foo"
