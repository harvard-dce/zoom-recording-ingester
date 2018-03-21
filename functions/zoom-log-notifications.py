
import json
import zlib
import base64
import boto3
from botocore.exceptions import ClientError
from os import getenv as env

import logging
from common import setup_logging
logger = logging.getLogger()

STACK_NAME = env('STACK_NAME')
SNS_TOPIC_ARN = env('SNS_TOPIC_ARN')
LOG_RETENTION_DAYS = 180
SUBSCRIPTION_FILTER_PATTERN = '{ $.level = "ERROR" }'

cwlogs = boto3.client('logs')
lmbda = boto3.client('lambda')
sns = boto3.client('sns')


@setup_logging
def handler(event, context):

    # we were triggered by a CreateLogGroup event from cloudtrail
    if 'source' in event and event['source'] == 'aws.logs':
        create_log_subscription(event, context)

    # we were triggered by the log group subscription
    elif 'awslogs' in event:
        events2sns(event, context)


def events2sns(event, context):

        logger.info("processing subscribed events")

        # log event data comes base64 encoded & gzipped
        raw_data = zlib.decompress(
            base64.b64decode(event['awslogs']['data']),
            wbits=16
        )
        log_data = json.loads(raw_data.decode())
        logger.debug({'log data': log_data})

        log_group = log_data['logGroup']
        log_stream = log_data['logStream']
        log_events = log_data['logEvents']

        for idx in range(len(log_events)):
            log_events[idx]['message'] = json.loads(log_events[idx]['message'])

        region = context.invoked_function_arn.split(':')[3]
        function_name = log_group.split('/')[-1]

        message = """
Error events in log group {}

Log Stream: https://console.aws.amazon.com/cloudwatch/home?region={}#logEventViewer:group={};stream={}

{}
"""
        message = message.format(log_group, region, log_group, log_stream, json.dumps(log_events, indent=2))
        logger.debug({'sns message': message})

        logger.info("publishing alert to topic {}".format(SNS_TOPIC_ARN))
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="[ERROR] {}".format(function_name),
            Message=message
        )


def create_log_subscription(event, context):

    try:
        log_group_name = event['detail']['requestParameters']['logGroupName']
        region = event['detail']['awsRegion']
        logger.info("Log group {} has been created".format(log_group_name))
    except KeyError as e:
        logger.error("event didn't contain a logGroupName param")
        return

    if not log_group_name.startswith("/aws/lambda/{}-".format(STACK_NAME)):
        logger.info("Ignoring log group not belonging to stack {}".format(STACK_NAME))
        return

    # create the necessary permission for the log group to execute this function
    logger.info("setting up permissions for {} to exec {}" \
                .format(log_group_name, context.function_name)
                )
    permission_statement_id = "{}-invoke-perms".format(log_group_name.split('/')[-1])
    try:
        resp = lmbda.add_permission(
            FunctionName=context.function_name,
            StatementId=permission_statement_id,
            Action="lambda:InvokeFunction",
            Principal="logs.{}.amazonaws.com".format(region)
        )
        logger.debug({'add permission': resp})
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            logger.info("permission already exists")
        else:
            raise

    # Create the subscription; target is *this* function
    logger.info("subscribing {} to events from {} matching pattern '{}'" \
                .format(context.function_name, log_group_name, SUBSCRIPTION_FILTER_PATTERN)
                )
    resp = cwlogs.put_subscription_filter(
        logGroupName=log_group_name,
        filterName="error events",
        filterPattern=SUBSCRIPTION_FILTER_PATTERN,
        destinationArn=context.invoked_function_arn
    )
    logger.debug({'put subscription filter': resp})

    # unrelated bonus: we get to set the retention policy
    logger.info("setting retention policy for {} to {} days" \
                .format(log_group_name, LOG_RETENTION_DAYS))
    resp = cwlogs.put_retention_policy(
        logGroupName=log_group_name,
        retentionInDays=LOG_RETENTION_DAYS
    )
    logger.debug({'put retention policy': resp})

