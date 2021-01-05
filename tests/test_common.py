import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import pytest
import jwt
import time
import requests
import requests_mock
import common
import logging


@pytest.mark.parametrize("key,secret,seconds_valid", [
    ('foo', 'bar', 10),
    ('abcd-1234', 'my-secret-key', 60),
    ('23kljh4jh3jkh_asd008', 'asdlkufh9080a9sdufkjn80989sdf', 1000)
])
def test_gen_token(key, secret, seconds_valid):
    token = common.gen_token(key, secret, seconds_valid=seconds_valid)
    payload = jwt.decode(token, secret, algorithms=['HS256'])
    assert payload['iss'] == key

    # should be within a second
    now = int(time.time())
    assert payload['exp'] - (now + seconds_valid) in [0, -1]


def test_zoom_api_request_missing_endpoint():
    with pytest.raises(Exception) as exc_info:
        common.zoom_api_request(endpoint=None)
    assert exc_info.match("Call to zoom_api_request missing endpoint")


def test_zoom_api_request_missing_creds():
    common.APIGEE_KEY = None

    # (ZOOM_API_KEY, ZOOM_API_SECRET)
    cases = [(None, None), ("key", None), (None, "secret")]

    for key, secret in cases:
        common.ZOOM_API_KEY = key
        common.ZOOM_API_SECRET = secret
        with pytest.raises(Exception) as exc_info:
            common.zoom_api_request("meetings")
        assert exc_info.match("Missing api credentials.")


def test_apigee_key(caplog):
    common.APIGEE_KEY = "apigee_key"
    common.ZOOM_API_KEY = None
    common.ZOOM_API_SECRET = None
    caplog.set_level(logging.INFO)
    common.zoom_api_request("meetings")
    assert "apigee request" in caplog.text.lower()

    # Should still use apigee key even if zoom api key/secret defined
    common.ZOOM_API_KEY = "key"
    common.ZOOM_API_SECRET = "secret"
    common.zoom_api_request("meetings")
    assert "apigee request" in caplog.text.lower()


def test_zoom_api_key(caplog):
    common.APIGEE_KEY = None
    common.ZOOM_API_KEY = "key"
    common.ZOOM_API_SECRET = "secret"
    caplog.set_level(logging.INFO)
    common.zoom_api_request("meetings")
    assert "zoom api request" in caplog.text.lower()


def test_zoom_api_request_success():
    # test successful call
    common.APIGEE_KEY = None
    common.ZOOM_API_KEY = "zoom_key"
    common.ZOOM_API_SECRET = "zoom_secret"
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=200,
            json={"mock_payload": 123}
        )
        r = common.zoom_api_request("meetings")
        assert "mock_payload" in r.json()


def test_zoom_api_request_failures():
    common.APIGEE_KEY = None
    common.ZOOM_API_KEY = "zoom_key"
    common.ZOOM_API_SECRET = "zoom_secret"
    # test failed call that returns
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=400,
            json={"mock_payload": 123}
        )
        r = common.zoom_api_request("meetings", ignore_failure=True)
        assert r.status_code == 400

    # test failed call that raises
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=400,
            json={"mock_payload": 123}
        )
        error_msg = "400 Client Error"
        with pytest.raises(requests.exceptions.HTTPError, match=error_msg):
            common.zoom_api_request("meetings", ignore_failure=False, retries=0)

    # test ConnectionError handling
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            exc=requests.exceptions.ConnectionError
        )
        error_msg = "Error requesting https://api.zoom.us/v2/meetings"
        with pytest.raises(common.ZoomApiRequestError, match=error_msg):
            common.zoom_api_request("meetings")

    # test ConnectTimeout handling
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            exc=requests.exceptions.ConnectTimeout
        )
        error_msg = "Error requesting https://api.zoom.us/v2/meetings"
        with pytest.raises(common.ZoomApiRequestError, match=error_msg):
            common.zoom_api_request("meetings")
