from aws_cdk import (
    core,
    aws_events as events,
    aws_events_targets as events_targets
)

class ZipEvent(core.Construct):

    def __init__(self, scope: core.Construct, id: str, function, event_rate):
        super().__init__(scope, id)
        stack_name = core.Stack.of(self).stack_name
        self.rule = events.Rule(self, "rule",
            enabled=True,
            rule_name=f"{function.function_name}-rule",
            schedule=events.Schedule.rate(core.Duration.minutes(event_rate)),
            targets=[events_targets.LambdaFunction(function)]
        )
