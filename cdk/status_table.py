from aws_cdk import Stack, RemovalPolicy, aws_dynamodb as dynamodb
from constructs import Construct
from . import names


class ZipStatus(Construct):
    def __init__(self, scope: Construct, id: str):
        """
        On demand requests status table
        """
        super().__init__(scope, id)
        stack_name = Stack.of(self).stack_name

        self.table = dynamodb.Table(
            self,
            "table",
            table_name=f"{stack_name}-{names.PIPELINE_STATUS_TABLE}",
            partition_key=dynamodb.Attribute(
                name="zip_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="expiration",
        )

        self.table.add_global_secondary_index(
            index_name="mid_index",
            partition_key=dynamodb.Attribute(
                name="meeting_id",
                type=dynamodb.AttributeType.NUMBER,
            ),
        )

        # Add secondary index for searching for latest updates
        # Since dynamoDB scans/filters only scan/filter on 1MB data,
        # we need an index ("update_date") to do an exact query to narrow
        # down the results to < 1MB before filtering
        # (1MB is several days worth of entries)
        self.table.add_global_secondary_index(
            index_name="time_index",
            partition_key=dynamodb.Attribute(
                name="update_date",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="update_time",
                type=dynamodb.AttributeType.NUMBER,
            ),
        )
