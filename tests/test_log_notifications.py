import site
from unittest.mock import patch
from os.path import dirname, join
from importlib import import_module

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

log_notifications = import_module("zoom-log-notifications")


def test_log_notifications_handler(mocker, handler, monkeypatch):
    """
    raw_data is a gzip-compressed, base64-encoded representation of this structure
    {
      "messageType": "DATA_MESSAGE",
      "owner": "542186135646",
      "logGroup": "/aws/lambda/foo-zoom-ingester-zoom-foo-function",
      "logStream": "2018/06/19/[33]abcd1234",
      "subscriptionFilters": [
        "jluker-zoom-ingester-ZoomUploaderLogSubscriptionFilter-SJXIJ5HTP9QJ"
      ],
      "logEvents": [
        {
          "id": "12345",
          "timestamp": 1529421105754,
          "message": "{\"hello\": \"world!\"}"
        },
        {
          "id": "67890",
          "timestamp": 1529421105756,
          "message": "{\"foo\": \"bar\", \"baz\": 54}",
          "exception": "\nTraceback (most recent call last):\n  File \"<doctest...>\", line 10, in <module>\n    lumberjack()\n  File \"<doctest...>\", line 4, in lumberjack\n    bright_side_of_death()\nIndexError: tuple index out of range\n"
        }
      ]
    }
    """

    raw_data = "H4sIABK/KlsC/4WRb2/aMBDGv4rnV60EJIEkBVRVQhrrijZpE0ya1iDkxAe4dXyR7YyuKN99Pmi3\nF5W2d74/z+/uHh95Dc6JHax+NcCnjL+frWabz/PlcnY75z3G8WDAUiFLh8k4T0ZZnuZU0Li7tdg2\nVIvEwUVa1KUU0Rax/4xY95XZgfNgzxGlt62pvELzIl96C6Im/TBOxlGcR8kkuh+N1qKsZDIcpdTn\n2tJVVjWk+6B04LmguOcPun18Zf+Z9CNE3xqNQoL9FAa80faXi+93i+zj6svk64Kvz3vMf4LxJ+qR\nK0n70PCMpnsV7PGipiuTbDgJHiRxdpWlofZiHPUfC74HrbEIQcEPaLV8V/COdz32isyvxpP4X8j8\nDTJYdgaWwhZBSo9nymRpRyR4quB0G/UXZmVFBaWoHtlFjc4zC1W4i1VCa6aF85fTwjAWfIBAupZY\n+bDHYDC4IbZWBlgS95gy7LpG2Wq4oXbGdFuXYB8C9+Lyf4D0pP+rOBNKq3Z7v3FKwga3GwnC74l1\nZyQ8za1FO2W+bQJWUYZh6xlumRXhWwvDu3X3G2HcfOOmAgAA\n"

    event = {"awslogs": {"data": raw_data}}

    mock_sns = mocker.Mock()
    monkeypatch.setattr(log_notifications, "sns", mock_sns)
    monkeypatch.setattr(log_notifications, "SNS_TOPIC_ARN", "MYTOPIC")

    context = mocker.Mock(
        invoked_function_arn="arn:aws:lambda:us-east-1:123456789:function:foo"
    )

    handler(log_notifications, event, context)

    publish_args = mock_sns.publish.call_args[1]

    assert publish_args["TopicArn"] == "MYTOPIC"
    assert (
        publish_args["Subject"]
        == "[ERROR] foo-zoom-ingester-zoom-foo-function"
    )

    log_stream_url = "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=/aws/lambda/foo-zoom-ingester-zoom-foo-function;stream=2018/06/19/[33]abcd1234"

    assert log_stream_url in publish_args["Message"]
    assert "IndexError: tuple index out of range" in publish_args["Message"]
