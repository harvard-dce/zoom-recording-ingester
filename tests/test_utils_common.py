import site
from os.path import dirname, join

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

import pytest
import jwt
import json
import re
import sys
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
    utils.common.ZOOM_API_BASE_URL = "https://www.foo.com"
    utils.common.APIGEE_KEY = "apigee_key"
    utils.common.ZOOM_API_KEY = None
    utils.common.ZOOM_API_SECRET = None
    caplog.set_level(logging.INFO)
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200)
        utils.zoom_api_request("meetings", ignore_failure=True)
        assert "apigee request" in caplog.text.lower()

    # Should still use apigee key even if zoom api key/secret defined
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200)
        utils.zoom_api_request("meetings")
        assert "apigee request" in caplog.text.lower()


def test_zoom_api_key(caplog):
    utils.common.ZOOM_API_BASE_URL = "https://www.foo.com"
    utils.common.APIGEE_KEY = None
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    caplog.set_level(logging.INFO)
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200)
        utils.zoom_api_request("meetings")
        assert "zoom api request" in caplog.text.lower()

    utils.common.APIGEE_KEY = ""
    utils.common.ZOOM_API_KEY = "key"
    utils.common.ZOOM_API_SECRET = "secret"
    caplog.set_level(logging.INFO)
    with requests_mock.mock() as req_mock:
        req_mock.get(requests_mock.ANY, status_code=200)
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


# logging setup / formatter tests (ZIP-104)


def _make_record(msg, level=logging.INFO, exc_info=None):
    return logging.LogRecord(
        name="test.module",
        level=level,
        pathname=__file__,
        lineno=42,
        msg=msg,
        args=(),
        exc_info=exc_info,
        func="myfunc",
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, False),
        ("", False),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
    ],
)
def test_env_flag(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("SOME_FLAG", raising=False)
    else:
        monkeypatch.setenv("SOME_FLAG", value)
    assert utils.common._env_flag("SOME_FLAG") is expected


def test_formatter_basic_fields():
    formatter = utils.common._JsonLambdaFormatter()
    record = _make_record("hello there")
    record.aws_request_id = "req-123"
    formatted = formatter.format(record)
    # the trailing newline is the record terminator; without it successive
    # records get concatenated into a single CloudWatch event (ZIP-104)
    assert formatted.endswith("\n")
    assert "\n" not in formatted[:-1]
    parsed = json.loads(formatted)
    assert parsed["level"] == "INFO"
    assert parsed["location"] == "test.module.myfunc:42"
    assert parsed["aws_request_id"] == "req-123"
    assert parsed["message"] == "hello there"
    # ISO 8601 UTC with millisecond precision
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", parsed["timestamp"]
    )


def test_formatter_dict_message():
    formatter = utils.common._JsonLambdaFormatter()
    record = _make_record({"foo": {"bar": 1}})
    parsed = json.loads(formatter.format(record))
    assert parsed["message"] == {"foo": {"bar": 1}}


def test_formatter_json_string_message():
    formatter = utils.common._JsonLambdaFormatter()
    record = _make_record('{"foo": 1}')
    parsed = json.loads(formatter.format(record))
    assert parsed["message"] == {"foo": 1}


def test_formatter_exception():
    formatter = utils.common._JsonLambdaFormatter()
    try:
        raise ValueError("boom")
    except ValueError:
        record = _make_record("failed", exc_info=sys.exc_info())
    parsed = json.loads(formatter.format(record))
    assert "ValueError: boom" in parsed["exception"]


def test_formatter_metric_filter_contract():
    """
    The cdk metric filters (cdk/function.py) match on these json paths;
    the formatter must keep emitting them.
    """
    formatter = utils.common._JsonLambdaFormatter()

    cases = [
        (
            {"payload": {"status": "RECORDING_MEETING_COMPLETED"}},
            lambda m: m["payload"]["status"] == "RECORDING_MEETING_COMPLETED",
        ),
        ({"duration": 12.5}, lambda m: m["duration"] > 0),
        ({"minutes_in_pipeline": 5}, lambda m: m["minutes_in_pipeline"] > 0),
    ]
    for msg, check in cases:
        parsed = json.loads(formatter.format(_make_record(msg)))
        assert check(parsed["message"])


@pytest.fixture
def preserved_root_handlers():
    root_logger = logging.getLogger()
    saved = root_logger.handlers[:]
    yield root_logger
    root_logger.handlers = saved


def test_setup_logging_reuses_existing_handler(preserved_root_handlers):
    """
    The lambda runtime's pre-installed handler must be kept (its transport
    guarantees one CloudWatch event per record), not replaced.
    """
    root_logger = preserved_root_handlers
    runtime_handler = logging.StreamHandler()
    root_logger.handlers = [runtime_handler]

    utils.common._setup_logging()
    assert root_logger.handlers == [runtime_handler]
    assert isinstance(
        runtime_handler.formatter, utils.common._JsonLambdaFormatter
    )
    assert utils.common._REQUEST_ID_FILTER in runtime_handler.filters

    # repeat invocation (warm container) must not stack handlers/filters
    utils.common._setup_logging()
    assert root_logger.handlers == [runtime_handler]
    assert runtime_handler.filters.count(utils.common._REQUEST_ID_FILTER) == 1


def test_setup_logging_creates_handler_when_none(preserved_root_handlers):
    root_logger = preserved_root_handlers
    root_logger.handlers = []

    utils.common._setup_logging()
    assert len(root_logger.handlers) == 1
    assert isinstance(
        root_logger.handlers[0].formatter, utils.common._JsonLambdaFormatter
    )


def test_request_id_filter_defers_to_existing_id():
    """
    In lambda, the runtime's own filter stamps a fresher request id before
    ours runs; ours must only fill in when no id is present.
    """
    f = utils.common._AwsRequestIdFilter()
    f.set_request_id("stale-id")

    record = _make_record("hello")
    record.aws_request_id = "runtime-id"
    f.filter(record)
    assert record.aws_request_id == "runtime-id"

    # empty string (runtime init phase) and absent id fall back to ours
    record = _make_record("hello")
    record.aws_request_id = ""
    f.filter(record)
    assert record.aws_request_id == "stale-id"

    record = _make_record("hello")
    f.filter(record)
    assert record.aws_request_id == "stale-id"
