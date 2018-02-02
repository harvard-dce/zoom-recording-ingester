
import pytest


@pytest.fixture
def handler(mocker):
    """
    this fixture provides a way to call the function handlers so as
    to insert a canned context (which is assumed by the logger setup)
    """
    def _handler(func_module, event):
        context = mocker.Mock(aws_request_id='12345-abcde')
        return getattr(func_module, 'handler')(event, context)

    return _handler

