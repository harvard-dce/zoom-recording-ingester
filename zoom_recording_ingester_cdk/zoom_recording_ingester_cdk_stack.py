from aws_cdk import (
    core,
    aws_apigateway as apigw,
    aws_lambda as _lambda,
    aws_ssm as ssm,
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
    aws_logs as logs
)
import boto3
import jmespath
from dotenv import load_dotenv
from os import getenv
from os.path import join, dirname
from functions.common import zoom_api_request

load_dotenv(join(dirname(dirname(__file__)), '.env'))


class ZoomRecordingIngesterCdkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Lambda alias that points to the "live" version of the function
        self.lambda_release_alias = self._getenv("LAMBDA_RELEASE_ALIAS")

        self.notification_email = self._getenv("NOTIFICATION_EMAIL")

        # S3 bucket that stores packaged lambda functions
        self.lambda_code_bucket_name = self._getenv("LAMBDA_CODE_BUCKET")
        self.lambda_code_bucket = s3.Bucket.from_bucket_name(
            self, "LambdaCodeBucket", self.lambda_code_bucket_name
        )

        vpc_id, sg_id = self.vpc_components()
        self.oc_vpc = ec2.Vpc.from_lookup(self, "OcVpc", vpc_id=vpc_id)
        self.oc_sg = ec2.SecurityGroup.from_security_group_id(
            self, "OcSecurityGroup", security_group_id=sg_id
        )

        """
        S3 bucket to store zoom recording files
        """
        two_week_lifecycle_rule = s3.LifecycleRule(
            id="DeleteAfterTwoWeeks",
            prefix="",  # for development, remove later
            enabled=True,
            expiration=core.Duration.days(14),
            abort_incomplete_multipart_upload_after=core.Duration.days(1)
        )

        zoom_videos_bucket = s3.Bucket(
            self, "ZoomVideosBucket",
            bucket_name=f"{self.stack_name}-zoom-recording-files",
            lifecycle_rules=[two_week_lifecycle_rule],
            removal_policy=core.RemovalPolicy.DESTROY
        )

        """
        SQS queues
        """

        downloads_queue, downloads_dlq = self._create_queue("downloader")
        uploads_queue, uploads_dlq = self._create_queue("upload", fifo=True)

        """
        Class schedule
        """

        class_schedule_table = dynamodb.Table(
            self, "ClassScheduleDynamoTable",
            table_name=f"{self.stack_name}-schedule",
            partition_key=dynamodb.Attribute(
                name="zoom_series_id",
                type=dynamodb.AttributeType.STRING
            ),
            read_capacity=1,
            write_capacity=1,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        """
        Lambda functions
        """

        # on demand function handles on demand requests for ingests
        on_demand_function = self._create_lambda_function(
            "zoom-on-demand",
            {
                "ZOOM_API_KEY": self._getenv("ZOOM_API_KEY"),
                "ZOOM_API_SECRET": self._getenv("ZOOM_API_SECRET")
            }
        )

        # webhook lambda handles incoming webhook notifications
        webhook_environment = {
            "DOWNLOAD_QUEUE_NAME": downloads_queue.queue_name,
            "LOCAL_TIME_ZONE": self._getenv("LOCAL_TIME_ZONE"),
            "DEBUG": "0"
        }
        webhook_function = self._create_lambda_function(
            "zoom-webhook", webhook_environment)

        # grant webhook lambda permission to send messages to downloads queue
        downloads_queue.grant_send_messages(webhook_function)

        # downloader lambda checks for matches with the course schedule
        # and uploads matching recordings to S3
        downloader_environment = {
            "ZOOM_VIDEOS_BUCKET": zoom_videos_bucket.bucket_name,
            "DOWNLOAD_QUEUE_NAME": downloads_queue.queue_name,
            "DEADLETTER_QUEUE_NAME": downloads_dlq.queue_name,
            "UPLOAD_QUEUE_NAME": uploads_queue.queue_name,
            "CLASS_SCHEDULE_TABLE": class_schedule_table.table_name,
            "DEBUG": "0",
            "ZOOM_ADMIN_ID": self.zoom_admin_id(),
            "ZOOM_API_KEY": self._getenv("ZOOM_API_KEY"),
            "ZOOM_API_SECRET": self._getenv("ZOOM_API_SECRET"),
            "LOCAL_TIME_ZONE": self._getenv("LOCAL_TIME_ZONE"),
            "DEFAULT_SERIES_ID": self._getenv(
                "DEFAULT_SERIES_ID", required=False
            ),
            "DOWNLOAD_MESSAGES_PER_INVOCATION": self._getenv(
                "DOWNLOAD_MESSAGES_PER_INVOCATION"
            )
        }
        downloader_function = self._create_lambda_function(
            "zoom-downloader", downloader_environment, timeout=900)
        
        # grant downloader function permissions
        downloads_queue.grant_consume_messages(downloader_function)
        uploads_queue.grant_send_messages(downloader_function)
        class_schedule_table.grant_read_write_data(downloader_function)
        zoom_videos_bucket.grant_write(downloader_function)

        op_counts_function = self._create_lambda_function(
            "opencast-op-counts", {}
        )

        # uploader lambda uploads recordings to opencast
        uploader_ssm_params = [
            "OPENCAST_API_USER",
            "OPENCAST_API_PASSWORD",
            "DEFAULT_PUBLISHER",
            "OVERRIDE_PUBLISHER",
            "OVERRIDE_CONTRIBUTOR",
            "OC_WORKFLOW",
            "OC_FLAVOR",
            "OC_TRACK_UPLOAD_MAX"
        ]
        uploader_environment = {
            var: self._getenv(var) for var in uploader_ssm_params
        }
        uploader_environment.update({
            "OPENCAST_BASE_URL": self._oc_base_url,
            "ZOOM_VIDEOS_BUCKET": zoom_videos_bucket.bucket_name,
            "UPLOAD_QUEUE_NAME": uploads_queue.queue_name,
            "DEBUG": "0",
            "OC_OP_COUNT_FUNCTION": op_counts_function.function_name, 
            "ZOOM_RECORDING_TYPE_NUM": "L01"
        })
        uploader_function = self._create_lambda_function(
            "zoom-uploader", uploader_environment,
            timeout=300, vpc=self.oc_vpc, security_group=self.oc_sg
        )

        # grant uploader function permissions
        uploads_queue.grant_consume_messages(uploader_function)
        op_counts_function.grant_invoke(uploader_function)
        # this is required so that the presigned s3 urls generated by
        # the uploader function will be valid
        zoom_videos_bucket.grant_read(uploader_function)

        log_notification_function = self._create_lambda_function(
            "zoom-log-notifications", {}
        )

        """
        API definition
        """

        api = apigw.LambdaRestApi(
            self, "ZoomIngesterApi",
            handler=webhook_function,  # default handler
            rest_api_name=self.stack_name,
            proxy=False,
            deploy=True,
            deploy_options=apigw.StageOptions(
                data_trace_enabled=True,
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
                stage_name=self.lambda_release_alias
            )
        )

        api.add_api_key("ZoomIngesterApiKey")

        new_recording = api.root.add_resource("new_recording")
        new_recording_method = new_recording.add_method(
            "POST",
            request_parameters={
                "method.request.querystring.type": True,
                "method.request.querystring.content": True
            },
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": apigw.Model.EMPTY_MODEL
                    }
                )
            ]
        )

        ingest = api.root.add_resource("ingest")
        on_demand_integration = apigw.LambdaIntegration(on_demand_function)
        ingest_method = ingest.add_method(
            "POST",
            on_demand_integration,
            request_parameters={
                "method.request.querystring.uuid": True,
                "method.request.querystring.oc_series_id": True
            },
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": apigw.Model.EMPTY_MODEL
                    }
                )
            ]
        )

        on_demand_function.add_environment(
            "WEBHOOK_ENDPOINT_URL",
            (f"https://{api.rest_api_id}.execute-api.{self.region}"
             f".amazonaws.com/{self.lambda_release_alias}/new_recording")
        )

        """
        CloudWatch Events
        """

        download_trigger = self._create_event_rule(
            downloader_function, "downloader", rate_in_minutes=5
        )
        upload_trigger = self._create_event_rule(
            uploader_function, "uploader", rate_in_minutes=20
        )


        """
        CodeBuild project
        """

        codebuild_project = codebuild.Project(
            self, "CodeBuildProject",
            project_name=f"{self.stack_name}-codebuild",
            source=codebuild.Source.git_hub_enterprise(
                https_clone_url="https://github.com/harvard-dce/zoom-recording-ingester.git",
                clone_depth=1
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_2,
                compute_type=codebuild.ComputeType.SMALL
            ),
            artifacts=codebuild.Artifacts.s3(
                name=self.stack_name,
                bucket=self.lambda_code_bucket
            ),
            badge=True,
            timeout=core.Duration.minutes(5)
        )

        """
        SNS Topic
        """

        sns_topic = sns.Topic(
            self, "ZoomIngesterNotificationTopic",
            topic_name=f"{self.stack_name}-notification-topic"
        )

        sns_topic.add_subscription(
            sns_subscriptions.EmailSubscription(self.notification_email)
        )


        """
        CloudWatch Alarms
        """

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

    def zoom_admin_id(self):
        # get admin user id from admin email
        r = zoom_api_request(f"users/{self._getenv('ZOOM_ADMIN_EMAIL')}")
        return r.json()["id"]

    def vpc_components(self):

        oc_cluster_name = self._getenv("OC_CLUSTER_NAME")
        opsworks = boto3.client("opsworks")

        stacks = opsworks.describe_stacks()
        vpc_id = jmespath.search(
            f"Stacks[?Name=='{oc_cluster_name}'].VpcId",
            stacks
        )[0]

        ec2_boto = boto3.client("ec2")
        security_groups = ec2_boto.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "tag:aws:cloudformation:logical-id", 
                 "Values": ["OpsworksLayerSecurityGroupCommon"]}
            ]
        )
        sg_id = jmespath.search("SecurityGroups[0].GroupId", security_groups)

        return vpc_id, sg_id

    def _getenv(self, param_name, required=True):
        return getenv(param_name, required)

    @property
    def _oc_base_url(self):

        oc_cluster_name = self._getenv("OC_CLUSTER_NAME")

        ec2 = boto3.client('ec2')

        result = ec2.describe_instances(
            Filters=[
                {
                    "Name": "tag:opsworks:stack",
                    "Values": [oc_cluster_name]
                },
                {
                    "Name": "tag:opsworks:layer:admin",
                    "Values": ["Admin"]
                }
            ]
        )
        if "Reservations" not in result or len(result["Reservations"]) == 0:
            raise Exception(
                f"No dns name found for OC_CLUSTER_NAME {oc_cluster_name}"
            )
        dns_name = result["Reservations"][0]["Instances"][0]["PublicDnsName"]

        url = "http://" + dns_name.strip()

        return url

    def _create_event_rule(self, function, rule_name, rate_in_minutes):
        return events.Rule(
            self, f"Zoom{rule_name.capitalize()}EventRule",
            rule_name=f"{self.stack_name}-{rule_name}-rule",
            enabled=True,
            schedule=events.Schedule.rate(
                core.Duration.minutes(rate_in_minutes)
            ),
            targets=[events_targets.LambdaFunction(function)]
        )

    def _create_lambda_function(
        self, function_name, environment, timeout=30,
        vpc=None, security_group=None
    ):

        lambda_id_prefix = ''.join(
            [x.capitalize() for x in function_name.split('-')]
        )
        lambda_id = f"{lambda_id_prefix}Function"

        environment = {key: str(val) for key,val in environment.items() if val}

        function = _lambda.Function(
            self, lambda_id,
            function_name=f"{self.stack_name}-{function_name}-function",
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_bucket(
                self.lambda_code_bucket,
                f"{self.stack_name}/{function_name}.zip"
            ),
            handler=f"{function_name}.handler",
            timeout=core.Duration.seconds(timeout),
            environment=environment,
            vpc=vpc,
            security_group=security_group
        )

        latest = function.add_version("$LATEST")

        alias = _lambda.Alias(
            self, f"{lambda_id}Alias",
            version=latest,
            description="initial release",
            alias_name=self.lambda_release_alias
        )

        return function

    def _create_queue(self, queue_name, fifo=False):

        primary_queue_name = f"{self.stack_name}-{queue_name}"
        dlq_name = f"{primary_queue_name}-deadletter"
        if fifo:
            primary_queue_name += ".fifo"
            dlq_name += ".fifo"

        dlq = sqs.Queue(
            self,
            f"ZoomIngester{queue_name.capitalize()}DeadLetterQueue",
            queue_name=dlq_name,
            retention_period=core.Duration.days(14)
        )

        # These cannot be merged because some parameters are FIFO only
        # such as fifo and content_based_deduplcation. For example, creating
        # a queue with fifo=False will cause an error and cloudformation
        # rollback.
        if fifo:
            queue = sqs.Queue(
                self, f"ZoomIngester{queue_name.capitalize()}Queue",
                queue_name=primary_queue_name,
                fifo=fifo,
                retention_period=core.Duration.days(14),
                content_based_deduplication=True,
                visibility_timeout=core.Duration.seconds(300),
                dead_letter_queue=sqs.DeadLetterQueue(
                    max_receive_count=2,
                    queue=dlq
                )
            )
        else:
            queue = sqs.Queue(
                self, f"ZoomIngester{queue_name.capitalize()}Queue",
                queue_name=primary_queue_name,
                retention_period=core.Duration.days(14),
                visibility_timeout=core.Duration.seconds(300),
                dead_letter_queue=sqs.DeadLetterQueue(
                    max_receive_count=2,
                    queue=dlq
                )
            )

        return queue, dlq

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
