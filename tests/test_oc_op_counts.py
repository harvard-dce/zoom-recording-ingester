import site
from os.path import dirname, join
from importlib import import_module

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

oc_op_counts = import_module("opencast-op-counts")


def test_parse_db_url(mocker):
    url = "mysql://foo:bar@baz.com:3000/blergh"
    mocker.patch.object(oc_op_counts, "OPENCAST_DB_URL", url)
    parsed = oc_op_counts.parse_db_url()
    assert parsed["host"] == "baz.com"
    assert parsed["port"] == 3000
    assert parsed["user"] == "foo"
    assert parsed["password"] == "bar"
    assert parsed["database"] == "blergh"


def test_oc_op_counts_happy_trail(handler, mocker, caplog):
    mocker.patch.object(
        oc_op_counts,
        "parse_db_url",
        mocker.Mock(return_value={"host": "foo.com"}),
    )
    mock_mysql = mocker.Mock()
    mock_cnx = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_mysql.connector = mocker.Mock()
    mock_mysql.connector.connect.return_value = mock_cnx
    mock_cnx.cursor.return_value = mock_cursor
    mock_cursor.fetchall = mocker.Mock(return_value=[("one", 1), ("two", 2)])

    mocker.patch.object(oc_op_counts, "mysql", mock_mysql)

    res = handler(oc_op_counts, {})
    assert res == {"one": 1, "two": 2}
