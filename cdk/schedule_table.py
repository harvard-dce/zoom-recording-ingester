from aws_cdk import Stack, RemovalPolicy, aws_dynamodb as dynamodb
from constructs import Construct
from . import names


class ZipSchedule(Construct):
    def __init__(self, scope: Construct, id: str):
        """
        Class schedule
        """
        super().__init__(scope, id)
        stack_name = Stack.of(self).stack_name

        self.table = dynamodb.Table(
            self,
            "table",
            table_name=f"{stack_name}-{names.SCHEDULE_TABLE}",
            partition_key=dynamodb.Attribute(
                name="zoom_series_id", type=dynamodb.AttributeType.STRING
            ),
            read_capacity=1,
            write_capacity=3,
            removal_policy=RemovalPolicy.DESTROY,
        )
