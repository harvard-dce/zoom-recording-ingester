from aws_cdk import Stack, RemovalPolicy, aws_dynamodb as dynamodb
from constructs import Construct
from . import names


class ZipRecordingEvents(Construct):
    def __init__(self, scope: Construct, id: str):
        """
        This table records the times of some webhook events.
        """
        super().__init__(scope, id)
        stack_name = Stack.of(self).stack_name

        self.table = dynamodb.Table(
            self,
            "table",
            table_name=f"{stack_name}-{names.RECORDING_EVENTS_TABLE}",
            partition_key=dynamodb.Attribute(
                name="zoom_uuid",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="expiration",
        )
