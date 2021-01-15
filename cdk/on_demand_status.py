from aws_cdk import core, aws_dynamodb as dynamodb
from . import  names

class ZipOnDemandStatus(core.Construct):

    def __init__(self, scope: core.Construct, id: str):
        """
        On demand requests status table
        """
        super().__init__(scope, id)
        stack_name = core.Stack.of(self).stack_name

        self.table = dynamodb.Table(
            self, "table",
            table_name=f"{stack_name}-{names.ON_DEMAND_STATUS_TABLE}",
            partition_key=dynamodb.Attribute(
                name="request_id",
                type=dynamodb.AttributeType.STRING
            ),
            read_capacity=1,
            write_capacity=1,
            removal_policy=core.RemovalPolicy.DESTROY,
            time_to_live_attribute="expiration"
        )
