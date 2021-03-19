from aws_cdk import (
    core,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_cloudwatch_actions as cloudwatch_actions,
)
from . import names


class ZipMonitoring(core.Construct):
    def __init__(self, scope: core.Construct, id: str, notification_email):
        super().__init__(scope, id)
        stack_name = core.Stack.of(self).stack_name

        self.topic = sns.Topic(
            self,
            "topic",
            topic_name=f"{stack_name}-{names.NOTIFICATIONS_TOPIC}",
        )
        self.topic.add_subscription(
            sns_subs.EmailSubscription(notification_email)
        )

        self.custom_metric_namespace = f"{stack_name}-{names.METRIC_NAMESPACE}"

    def add_alarm_action(self, alarm):
        alarm.add_alarm_action(cloudwatch_actions.SnsAction(self.topic))

    def add_insufficient_data_action(self, alarm):
        alarm.add_insufficient_data_action(
            cloudwatch_actions.SnsAction(self.topic)
        )
