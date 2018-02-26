
import logging
import aws_lambda_logging
from os import getenv as env
from pyloggly import LogglyBulkHandler

LOG_LEVEL = env('DEBUG') and 'DEBUG' or 'INFO'
BOTO_LOG_LEVEL = env('BOTO_DEBUG') and 'DEBUG' or 'INFO'
LOGGLY_TOKEN = env('LOGGLY_TOKEN')
LOGGLY_TAGS = env('LOGGLY_TAGS')

def setup_logging(handler_func):

    def wrapped_func(event, context):

        extra_info = {'aws_request_id': context.aws_request_id}
        aws_lambda_logging.setup(
            level=LOG_LEVEL,
            boto_level=BOTO_LOG_LEVEL,
            **extra_info
        )

        # do this second otherwise aws_lambda_logging clobbers our formatter
        if LOGGLY_TOKEN:
            tags = LOGGLY_TAGS and ",".join(LOGGLY_TAGS.split()) or None
            logger = logging.getLogger()
            loggly_handler = LogglyBulkHandler(LOGGLY_TOKEN, 'logs-01.loggly.com', tags)
            loggly_handler.level = logging.getLevelName(LOG_LEVEL)
            logger.addHandler(loggly_handler)
            logger.debug("Logging to loggly!")
        else:
            loggly_handler = None

        # now call the handler
        try:
            retval = handler_func(event, context)
        except Exception as e:
            logger.exception("handler failed!")
            raise
        finally:
            if loggly_handler:
                loggly_handler.flush()

        return retval

    wrapped_func.__name__ = handler_func.__name__
    return wrapped_func

