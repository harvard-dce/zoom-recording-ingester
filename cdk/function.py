from pathlib import Path
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda,
    aws_ec2 as ec2,
    aws_cloudwatch as cloudwatch,
    aws_logs as logs,
    aws_ecr_assets as ecr_assets,
)
from constructs import Construct
from . import names


class ZipFunction(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        name,
        environment,
        timeout=30,
        memory_size=128,
        vpc_id=None,
        security_group_id=None,
    ):
        super().__init__(scope, id)

        self.stack_name = Stack.of(self).stack_name
        environment = {
            key: str(val) for key, val in environment.items() if val
        }

        function_props = {
            "function_name": f"{self.stack_name}-{name}",
            "code": aws_lambda.DockerImageCode.from_image_asset(
                str(Path(__file__).parent.parent / "functions"),
                cmd=[f"{name}.handler"],
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            "timeout": Duration.seconds(timeout),
            "memory_size": memory_size,
            "environment": environment,
            "log_retention": logs.RetentionDays.SIX_MONTHS,
            "description": f"{name} created with cdk v2 for {self.stack_name}",
        }

        if vpc_id and security_group_id:
            opencast_vpc = ec2.Vpc.from_lookup(
                self, "OpencastVpc", vpc_id=vpc_id
            )
            opencast_security_group = ec2.SecurityGroup.from_security_group_id(
                self,
                "OpencastSecurityGroup",
                security_group_id=security_group_id,
            )
            function_props.update(
                {
                    "vpc": opencast_vpc,
                    "security_groups": [opencast_security_group],
                }
            )

        self.function = aws_lambda.DockerImageFunction(
            self, "function", **function_props
        )
        self.alias = aws_lambda.Alias(
            self,
            "alias",
            description="initial release",
            alias_name=names.LAMBDA_RELEASE_ALIAS,
            version=self.function.latest_version,
        )

    def add_monitoring(self, monitoring):
        errors_alarm = cloudwatch.Alarm(
            self,
            "ErrorsAlarm",
            metric=self.function.metric_errors(
                statistic="sum",
                period=Duration.minutes(1),
            ),
            alarm_name=f"{self.function.function_name}-errors",
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            threshold=1,
            evaluation_periods=1,
        )
        monitoring.add_alarm_action(errors_alarm)


class ZipOnDemandFunction(ZipFunction):
    pass


class ZipWebhookFunction(ZipFunction):
    def add_monitoring(self, monitoring):
        super().add_monitoring(monitoring)

        logs.MetricFilter(
            self,
            "RecordingCompletedLogMetric",
            log_group=self.function.log_group,
            metric_name="RecordingCompleted",
            metric_value="1",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.all(
                logs.JsonPattern(
                    '$.message.payload.status = "RECORDING_MEETING_COMPLETED"'
                )
            ),
        )

        logs.MetricFilter(
            self,
            "MeetingStartedLogMetric",
            log_group=self.function.log_group,
            metric_name="MeetingStarted",
            metric_value="1",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.all(
                logs.JsonPattern('$.message.payload.status= "STARTED"')
            ),
        )

        logs.MetricFilter(
            self,
            "MeetingEndedLogMetric",
            log_group=self.function.log_group,
            metric_name="MeetingEnded",
            metric_value="1",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.all(
                logs.JsonPattern('$.message.payload.status= "ENDED"')
            ),
        )


class ZipDownloaderFunction(ZipFunction):
    def add_monitoring(self, monitoring):
        super().add_monitoring(monitoring)

        invocations_alarm = cloudwatch.Alarm(
            self,
            "InvocationsAlarm",
            metric=self.function.metric_invocations(
                statistic="sum",
                period=Duration.minutes(1440),
            ),
            alarm_name=f"{self.function.function_name}-invocations",
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            threshold=1,
            evaluation_periods=1,
        )
        monitoring.add_alarm_action(invocations_alarm)

        logs.MetricFilter(
            self,
            "RecordingDurationLogMetric",
            log_group=self.function.log_group,
            metric_name="RecordingDuration",
            metric_value="$.message.duration",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.all(
                logs.JsonPattern("$.message.duration > 0")
            ),
        )

        logs.MetricFilter(
            self,
            "RecordingSkippedLogMetric",
            log_group=self.function.log_group,
            metric_name="SkippedForDuration",
            metric_value="1",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.literal("Skipping"),
        )


class ZipUploaderFunction(ZipFunction):
    def add_monitoring(self, monitoring):
        super().add_monitoring(monitoring)

        logs.MetricFilter(
            self,
            "MinutesInPipelineLogMetric",
            log_group=self.function.log_group,
            metric_name="MinutesInPipeline",
            metric_value="$.message.minutes_in_pipeline",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.all(
                logs.JsonPattern("$.message.minutes_in_pipeline > 0")
            ),
        )

        logs.MetricFilter(
            self,
            "WorkflowInitiatedLogMetric",
            log_group=self.function.log_group,
            metric_name="WorkflowInitiated",
            metric_value="1",
            metric_namespace=monitoring.custom_metric_namespace,
            filter_pattern=logs.FilterPattern.literal("Workflow"),
        )


class ZipLogNotificationsFunction(ZipFunction):
    pass


class ZipScheduleUpdateFunction(ZipFunction):
    pass


class ZipStatusQueryFunction(ZipFunction):
    pass


class ZipSlackQueryFunction(ZipFunction):
    pass
