from aws_cdk import core, aws_dynamodb as dynamodb

class ZipSchedule(core.Construct):

    def __init__(self, scope: core.Construct, id: str):
        """
        Class schedule
        """
        super().__init__(scope, id)
        stack_name = core.Stack.of(self).stack_name

        self.table = dynamodb.Table(
            self, "table",
            table_name=f"{stack_name}-schedule",
            partition_key=dynamodb.Attribute(
                name="zoom_series_id",
                type=dynamodb.AttributeType.STRING
            ),
            read_capacity=1,
            write_capacity=1,
            removal_policy=core.RemovalPolicy.DESTROY
        )
