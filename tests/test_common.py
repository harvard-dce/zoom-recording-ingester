import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

import pytest
import jwt
import time
import requests
import requests_mock
from common import ZoomAPIRequests


@pytest.mark.parametrize("key,secret,seconds_valid", [
    ('foo', 'bar', 10),
    ('abcd-1234', 'my-secret-key', 60),
    ('23kljh4jh3jkh_asd008', 'asdlkufh9080a9sdufkjn80989sdf', 1000)
])
def test_gen_token(key, secret, seconds_valid):
    zoom_api = ZoomAPIRequests(key, secret)
    token = zoom_api.gen_token(seconds_valid=seconds_valid)
    payload = jwt.decode(token, secret, algorithms=['HS256'])
    assert payload['iss'] == key

    # should be within a second
    now = int(time.time())
    assert payload['exp'] - (now + seconds_valid) in [0, -1]


def test_zoom_api_get_request():
    # test missing api endpoint
    zoom_api = ZoomAPIRequests("key", "secret")
    with pytest.raises(Exception) as exc_info:
        zoom_api.get(endpoint=None)
    assert exc_info.match("missing required param 'path'")

    # test successful call
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=200,
            json={"mock_payload": 123}
        )
        r = zoom_api.get(endpoint="meetings")
        assert "mock_payload" in r.json()

    # test failed call that returns
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=400,
            json={"mock_payload": 123}
        )
        r = zoom_api.get(endpoint="meetings", ignore_failure=True)
        assert r.status_code == 400

    # test failed call that raises
    with requests_mock.mock() as req_mock:
        req_mock.get(
            requests_mock.ANY,
            status_code=400,
            json={"mock_payload": 123}
        )
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            zoom_api.get(endpoint="meetings", ignore_failure=False)
            assert exc_info.match("400 Client Error")
