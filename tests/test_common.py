import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import pytest
import jwt
import time
import requests
import requests_mock
from common import gen_token, zoom_api_request, ZoomApiRequestError


@pytest.mark.parametrize("key,secret,seconds_valid", [
    ('foo', 'bar', 10),
    ('abcd-1234', 'my-secret-key', 60),
    ('23kljh4jh3jkh_asd008', 'asdlkufh9080a9sdufkjn80989sdf', 1000)
])
def test_gen_token(key, secret, seconds_valid):
    token = gen_token(key, secret, seconds_valid=seconds_valid)
    payload = jwt.decode(token, secret, algorithms=['HS256'])
    assert payload['iss'] == key

    # should be within a second
    now = int(time.time())
    assert payload['exp'] - (now + seconds_valid) in [0, -1]


def test_zoom_api_request_missing_endpoint():
    with pytest.raises(Exception) as exc_info:
        zoom_api_request(endpoint=None)
    assert exc_info.match("missing required param 'endpoint'")


def test_zoom_api_request_success():
    # test successful call
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=200,
            json={"mock_payload": 123}
        )
        r = zoom_api_request("meetings")
        assert "mock_payload" in r.json()


def test_zoom_api_request_failures():

    # test failed call that returns
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=400,
            json={"mock_payload": 123}
        )
        r = zoom_api_request("meetings", ignore_failure=True)
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
            zoom_api_request("meetings", ignore_failure=False, retries=0)

    # test ConnectionError handling
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            exc=requests.exceptions.ConnectionError
        )
        error_msg = "Error requesting https://api.zoom.us/v2/meetings"
        with pytest.raises(ZoomApiRequestError, match=error_msg):
            zoom_api_request("meetings")

    # test ConnectTimeout handling
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            exc=requests.exceptions.ConnectTimeout
        )
        error_msg = "Error requesting https://api.zoom.us/v2/meetings"
        with pytest.raises(ZoomApiRequestError, match=error_msg):
            zoom_api_request("meetings")
