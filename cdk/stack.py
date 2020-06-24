from aws_cdk import (
    core,
    aws_apigateway as apigw,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_dynamodb as dynamodb,
    aws_codebuild as codebuild,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_ec2 as ec2,
    aws_logs as logs,
    aws_iam as iam
)

from .bucket import ZipRecordingsBucket
from .queues import ZipQueues
from .schedule import ZipSchedule
from .function import ZipFunction
from .api import ZipApi
from .events import ZipEvent
from .codebuild import ZipCodebuildProject
from .alarms import ZipAlarms

class ZipStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str,
            lambda_code_bucket,
            notification_email,
            zoom_api_key,
            zoom_api_secret,
            local_time_zone,
            default_series_id,
            download_message_per_invocation,
            opencast_api_user,
            opencast_api_password,
            default_publisher,
            override_publisher,
            override_contributor,
            oc_workflow,
            oc_flavor,
            oc_track_upload_max,
            oc_base_url,
            zoom_admin_id,
            oc_vpc_id,
            oc_security_group_id,
            downloader_event_rate,
            uploader_event_rate,
            project_git_url,
            **kwargs
            ) -> None:
        super().__init__(scope, id, **kwargs)

        self.notification_email = notification_email

        # S3 bucket that stores packaged lambda functions
        lambda_code_bucket = s3.Bucket.from_bucket_name(
            self, "LambdaCodeBucket", lambda_code_bucket
        )

        recordings_bucket = ZipRecordingsBucket(self, "RecordingsBucket")
        queues = ZipQueues(self, "Queues")
        schedule = ZipSchedule(self, "Schedule")

        on_demand = ZipFunction(self, "OnDemandFunction",
            function_name=f"{self.stack_name}-on-demand-function",
            handler="zoom-on-demand.handler",
            lambda_code_bucket=lambda_code_bucket,
            environment={
                "ZOOM_API_KEY": zoom_api_key,
                "ZOOM_API_SECRET": zoom_api_secret
            }
        )

        webhook = ZipFunction(self, "WebhookFunction",
            function_name=f"{self.stack_name}-webhook-function",
            handler="zoom-webhook.handler",
            lambda_code_bucket=lambda_code_bucket,
            environment={
                "DOWNLOAD_QUEUE_NAME": queues.download_queue.queue_name,
                "LOCAL_TIME_ZONE": local_time_zone,
                "DEBUG": "0"
            }
        )

        # grant webhook lambda permission to send messages to downloads queue
        queues.download_queue.grant_send_messages(webhook)

        # downloader lambda checks for matches with the course schedule
        # and uploads matching recordings to S3
        downloader = ZipFunction(self, "DownloadFunction",
            function_name=f"{self.stack_name}-downloader-function",
            handler="zoom-downloader.handler",
            lambda_code_bucket = lambda_code_bucket,
            timeout=900,
            environment={
                "ZOOM_VIDEOS_BUCKET": recordings_bucket.bucket.bucket_name,
                "DOWNLOAD_QUEUE_NAME": queues.download_queue.queue_name,
                "DEADLETTER_QUEUE_NAME": queues.download_dlq.queue.queue_name,
                "UPLOAD_QUEUE_NAME": queues.upload_queue.queue_name,
                "CLASS_SCHEDULE_TABLE": schedule.table.table_name,
                "DEBUG": "0",
                "ZOOM_ADMIN_ID": zoom_admin_id,
                "ZOOM_API_KEY": zoom_api_key,
                "ZOOM_API_SECRET": zoom_api_secret,
                "LOCAL_TIME_ZONE": local_time_zone,
                "DEFAULT_SERIES_ID": default_series_id,
                "DOWNLOAD_MESSAGES_PER_INVOCATION": download_message_per_invocation,
            }
        )

        # grant downloader function permissions
        queues.download_queue.grant_consume_messages(downloader)
        queues.upload_queue.grant_consume_messages(downloader)
        schedule.table.grant_read_write_data(downloader)
        recordings_bucket.bucket.grant_write(downloader)

        op_counts = ZipFunction(self, 'OpCountsFunction',
            function_name=f"{self.stack_name}-opencast-op-counts",
            handler="opencast-op-counts.handler",
            lambda_code_bucket = lambda_code_bucket,
            environment={}
        )

        # uploader lambda uploads recordings to opencast
        uploader = ZipFunction(self, 'UploaderFunction',
            function_name=f"{self.stack_name}-uploader",
            handler="zoom-uploader.handler",
            lambda_code_bucket=lambda_code_bucket,
            timeout=300,
            vpc_id=oc_vpc_id,
            security_group_id=oc_security_group_id,
            environment = {
                "OPENCAST_API_USER": opencast_api_user,
                "OPENCAST_API_PASSWORD": opencast_api_password,
                "DEFAULT_PUBLISHER": default_publisher,
                "OVERRIDE_PUBLISHER": override_publisher,
                "OVERRIDE_CONTRIBUTOR": override_contributor,
                "OC_WORKFLOW": oc_workflow,
                "OC_FLAVOR": oc_flavor,
                "OC_TRACK_UPLOAD_MAX": oc_track_upload_max,
                "OPENCAST_BASE_URL": oc_base_url,
                "ZOOM_VIDEOS_BUCKET": recordings_bucket.bucket.bucket_name,
                "UPLOAD_QUEUE_NAME": queues.upload_queue.queue_name,
                "DEBUG": "0",
                "OC_OP_COUNT_FUNCTION": op_counts.function.function_name
            }
        )

        # grant uploader function permissions
        queues.upload_queue.grant_consume_messages(uploader)
        op_counts.function.grant_invoke(uploader)
        # this is required so that the presigned s3 urls generated by
        # the uploader function will be valid
        recordings_bucket.bucket.grant_read(uploader)

        log_notify = ZipFunction(self, 'LogNotificationFunction',
            function_name=f"{self.stack_name}-log-notify",
            handler="zoom-log-notifications.handler",
            lambda_code_bucket=lambda_code_bucket,
            environment={}
        )

        api = ZipApi(self, "RestApi",
            on_demand_function=on_demand,
            webhook_function=webhook
        )

        download_event = ZipEvent(self, "DownloadEvent",
            function=downloader,
            event_rate=downloader_event_rate
        )

        uploader_event = ZipEvent(self, "UploadEvent",
            function=uploader,
            event_rate=uploader_event_rate
        )

        codebuild_project = ZipCodebuildProject(self, "CodebuildProject",
            lambda_code_bucket=lambda_code_bucket,
            project_git_url=project_git_url,
            policy_resources=[
                on_demand.function.function_arn,
                webhook.function.function_arn,
                downloader.function.function_arn,
                uploader.function.function_arn,
                op_counts.function.function_arn,
                log_notify.function.function_arn
            ]
        )

        alarms = ZipAlarms(self, 'AlarmNotifications',
            notification_email=notification_email,
            api=api,

        )

        alarms.add_alarm(
            metric=cloudwatch.Metric
        )

        alarms = []

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

        # Webhook function Alarm
        alarms.append(
            self._create_metric_alarm(
                "ZoomWebhookErrorsMetricAlarm",
                webhook_function.metric_errors()
            )
        )

        # Downloader function alarms
        alarms.append(
            self._create_metric_alarm(
                "ZoomDownloaderErrorsMetricAlarm",
                downloader_function.metric_errors()
            )
        )

        alarms.append(
            self._create_metric_alarm(
                "ZoomDownloaderInvocationsMetricAlarm",
                downloader_function.metric_invocations(),
                period_minutes=1440,
                comparison="<"
            )
        )

        # Uploader function Alarm
        alarms.append(
            self._create_metric_alarm(
                "ZoomUploaderErrorsMetricAlarm",
                uploader_function.metric_errors()
            )
        )

        # Upload Queue Alarm
        upload_queue_alarm = self._create_metric_alarm(
            "ZoomUploadQueueDepthMetricAlarm",
            uploads_queue.metric("ApproximateNumberOfMessagesVisible"),
            comparison=">",
            period_minutes=5,
            threshold=40
        )
        upload_queue_alarm.add_insufficient_data_action(
            cloudwatch_actions.SnsAction(sns_topic)
        )
        alarms.append(upload_queue_alarm)

        for alarm in alarms:
            alarm.add_alarm_action(cloudwatch_actions.SnsAction(sns_topic))

        """
        Metric Filters
        """

        metric_filters = []

        metric_filters.append(self._metric_filter(
            "RecordingCompleted",
            webhook_function.log_group,
            logs.FilterPattern.all(logs.JsonPattern(
                "$.message.payload.status = \"RECORDING_MEETING_COMPLETED\""
            )),
            "1"
        ))

        metric_filters.append(self._metric_filter(
            "MeetingStarted",
            webhook_function.log_group,
            logs.FilterPattern.all(logs.JsonPattern(
                "$.message.payload.status= \"STARTED\""
            )),
            "1"
        ))

        metric_filters.append(self._metric_filter(
           "MeetingEnded",
           webhook_function.log_group,
           logs.FilterPattern.all(logs.JsonPattern(
               "$.message.payload.status= \"ENDED\""
           )),
           "1"
        ))

        metric_filters.append(self._metric_filter(
            "RecordingDuration",
            downloader_function.log_group,
            logs.FilterPattern.all(logs.JsonPattern("$.message.duration > 0")),
            "$.message.duration"
        ))

        metric_filters.append(self._metric_filter(
            "SkippedForDuration",
            downloader_function.log_group,
            logs.FilterPattern.literal("Skipping"),
            "1"
        ))

        metric_filters.append(self._metric_filter(
            "MinutesInPipeline",
            uploader_function.log_group,
            logs.FilterPattern.all(logs.JsonPattern(
                "$.message.minutes_in_pipeline > 0"
            )),
            "$.message.minutes_in_pipeline"
        ))

        metric_filters.append(self._metric_filter(
            "WorkflowInitiated",
            uploader_function.log_group,
            logs.FilterPattern.literal("Workflow"),
            "1"
        ))


    def _create_metric_alarm(
            self, alarm_id, metric,
            statistic="sum", comparison=">=",
            period_minutes=1, threshold=1, evaluation_periods=1
            ):

        if comparison == "<":
            comparison_operator = cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD
        elif comparison == "<=":
            comparison_operator = cloudwatch.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD
        elif comparison == ">":
            comparison_operator = cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD
        else:
            comparison_operator = cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD

        alarm = cloudwatch.Alarm(
            self, alarm_id,
            alarm_name=f"{self.stack_name}-{alarm_id}",
            metric=metric,
            statistic=statistic,
            comparison_operator=comparison_operator,
            evaluation_periods=evaluation_periods,
            threshold=threshold,
            period=core.Duration.minutes(period_minutes)
        )

        return alarm

    def _metric_filter(self, filter_name, log_group, pattern, metric_value):

        return logs.MetricFilter(
            self, f"{filter_name}MetricFilter",
            log_group=log_group,
            filter_pattern=pattern,
            metric_name=filter_name,
            metric_namespace=f"{self.stack_name}-ZoomIngesterLogMetrics",
            metric_value=metric_value
        )
