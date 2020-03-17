import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))
from importlib import import_module

import json
import requests_mock

on_demand = import_module('zoom-on-demand')

def test_missing_body(handler):
    res = handler(on_demand, {})
    assert res["statusCode"] == 400
    resp_body = json.loads(res["body"])
    assert "No body" in resp_body["message"]

def test_malformed_body(handler):
    res = handler(on_demand, {"body": "no json here"})
    assert res["statusCode"] == 400
    resp_body = json.loads(res["body"])
    assert "not valid json" in resp_body["message"]

def test_missing_uuid(handler):
    res = handler(on_demand, {"body": "{}"})
    assert res["statusCode"] == 400
    resp_body = json.loads(res["body"])
    assert "Missing recording uuid" in resp_body["message"]

def test_bad_uuid_url(handler):
    res = handler(on_demand, {"body": json.dumps({"uuid": "https://blahblah"})})
    assert res["statusCode"] == 400
    resp_body = json.loads(res["body"])
    assert "Bad url" in resp_body["message"]

def test_good_uuid_url(handler, mocker, caplog):
    # patch the zoom api requester so we don't actually make any calls
    mock_zoom_api_request = mocker.Mock(return_value=None)
    mocker.patch.object(on_demand, 'zoom_api_request', mock_zoom_api_request)

    good_url = "https://foo.com?meeting_id=abcde12345"
    res = handler(on_demand, {"body": json.dumps({"uuid": good_url})})
    assert any(
        item == "Got recording uuid: 'abcde12345'"
        for item in caplog.messages
    )

def test_recordings_not_completed(handler, mocker):
    mock_zoom_resp = mocker.Mock()
    mock_zoom_resp.json.return_value = {
        "recording_files": [
            { "status": "not done" },
            { "status": "completed" }
        ]
    }
    mock_zoom_api_request = mocker.Mock(return_value=mock_zoom_resp)
    mocker.patch.object(on_demand, 'zoom_api_request', mock_zoom_api_request)
    res = handler(on_demand, { "body": json.dumps({"uuid": "foo"})})
    assert res["statusCode"] == 503
    resp_body = json.loads(res["body"])
    assert "Not all recorded files" in resp_body["message"]

def test_on_demand_happy_trail(handler, mocker):
    mock_zoom_resp = mocker.Mock()
    mock_zoom_resp.json.return_value = {
        "recording_files": [
            { "status": "completed" },
            { "status": "completed" }
        ]
    }
    mock_zoom_api_request = mocker.Mock(return_value=mock_zoom_resp)
    mocker.patch.object(on_demand, 'zoom_api_request', mock_zoom_api_request)
    mocker.patch.object(on_demand, 'WEBHOOK_ENDPOINT_URL', 'mock://foo.com')
    with requests_mock.mock() as m:
        m.post('mock://foo.com', status_code=200)
        handler_event = {
            "body": json.dumps({"uuid": "foo", "oc_series_id": "bar"})
        }
        res = handler(on_demand, handler_event)
    assert m.called
    mock_req = m.request_history[0]
    assert mock_req.method == "POST"
    req_payload = json.loads(mock_req.text)
    assert req_payload["event"] == "on.demand.ingest"
    assert len(req_payload["payload"]["object"]["recording_files"]) == 2
    assert req_payload["payload"]["on_demand_series_id"] == "bar"

def test_on_demand_ingest_not_accepted(handler, mocker):
    mock_zoom_resp = mocker.Mock()
    mock_zoom_resp.json.return_value = {
        "recording_files": [
            { "status": "completed" },
            { "status": "completed" }
        ]
    }
    mock_zoom_api_request = mocker.Mock(return_value=mock_zoom_resp)
    mocker.patch.object(on_demand, 'zoom_api_request', mock_zoom_api_request)
    mocker.patch.object(on_demand, 'WEBHOOK_ENDPOINT_URL', 'mock://foo.com')
    with requests_mock.mock() as m:
        m.post('mock://foo.com', status_code=204)
        handler_event = {
            "body": json.dumps({"uuid": "foo", "oc_series_id": "bar"})
        }
        res = handler(on_demand, handler_event)
    assert res["statusCode"] == 500
    resp_body = json.loads(res["body"])
    assert "ingest not accepted" in resp_body["message"]
