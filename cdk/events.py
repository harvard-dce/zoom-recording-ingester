from aws_cdk import (
    Duration,
    aws_events as events,
    aws_events_targets as events_targets,
)
from constructs import Construct


class ZipEvent(Construct):
    def __init__(self, scope: Construct, id: str, function, event_rate):
        super().__init__(scope, id)
        self.rule = events.Rule(
            self,
            "rule",
            enabled=True,
            rule_name=f"{function.function_name}-rule",
            schedule=events.Schedule.rate(Duration.minutes(event_rate)),
            targets=[events_targets.LambdaFunction(function)],
        )
