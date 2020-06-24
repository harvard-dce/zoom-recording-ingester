from aws_cdk import (
    core,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_cloudwatch as cloudwatch
)

class ZipAlarms(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
            notification_email
    ):
        super().__init__(scope, id)
        stack_name = core.Stack.of(self).stack_name

        self.topic = sns.Topic(self, "topic", topic_name=f"{stack_name}-notifications")
        self.topic.add_subscription(sns_subs.EmailSubscription(notification_email))

    def create_alarm(self,
            metric_name,
            namespace,
            dimensions,
            period,
    ):

    def create_function_alarms(self, function):
        pass

    def create_api_alarm(self, api, metric_name):
        self.create_alarm()

        webhook_4xx_metric = cloudwatch.Metric(
            metric_name="ZoomWebhook4xxMetricAlarm",
            namespace="AWS/ApiGateway",
            dimensions={
                "ApiName": self.stack_name,
                "Stage": self.lambda_release_alias
            },
            period=core.Duration.minutes(1)
        )

        alarms.append(
            self._create_metric_alarm(
                webhook_4xx_metric.metric_name,
                metric=webhook_4xx_metric
            )
        )

        webhook_5xx_metric = cloudwatch.Metric(
            metric_name="ZoomWebhook5xxMetricAlarm",
            namespace="AWS/ApiGateway",
            dimensions={
                "ApiName": self.stack_name,
                "Stage": self.lambda_release_alias
            },
            period=core.Duration.minutes(1)
        )

        alarms.append(
            self._create_metric_alarm(
                webhook_5xx_metric.metric_name,
                metric=webhook_5xx_metric
            )
        )

        webhook_latency_metric = cloudwatch.Metric(
            metric_name="ZoomWebhookLatencyMetricAlarm",
            namespace="AWS/ApiGateway",
            dimensions={
                "ApiName": self.stack_name,
                "Stage": self.lambda_release_alias
            },
            period=core.Duration.minutes(1)
        )

        alarms.append(
            self._create_metric_alarm(
                webhook_latency_metric.metric_name,
                metric=webhook_latency_metric,
                statistic="avg",
                threshold=10000,
                evaluation_periods=3
            )
        )
