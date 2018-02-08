
import aws_lambda_logging
from os import getenv as env

LOG_LEVEL = env('DEBUG') and 'DEBUG' or 'INFO'
BOTO_LOG_LEVEL = env('BOTO_DEBUG') and 'DEBUG' or 'INFO'

def setup_logging(context):
    aws_lambda_logging.setup(
        level=LOG_LEVEL,
        boto_level=BOTO_LOG_LEVEL,
        aws_request_id=context.aws_request_id
    )

