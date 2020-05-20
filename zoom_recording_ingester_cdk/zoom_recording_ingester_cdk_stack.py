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
    aws_cloudwatch_actions as cloudwatch_actions
)
import boto3
# from ssm_dotenv import getenv
from dotenv import load_dotenv
from os import getenv
from os.path import join, dirname

load_dotenv(join(dirname(dirname(__file__)), '.env'))

PROJECT_NAME = "zoom-ingester"


class ZoomRecordingIngesterCdkStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Name of this stack
        self.name = core.Stack.of(self).stack_name

        # Path in which variables are stored in ssm parameter store
        self.param_path = "/{}/{}/".format(PROJECT_NAME, self.name)

        # Lambda alias that points to the "live" version of the function
        self.lambda_release_alias = self.get_ssm_param("LAMBDA_RELEASE_ALIAS")

        self.notification_email = self.get_ssm_param("NOTIFICATION_EMAIL")

        # S3 bucket that stores packaged lambda functions
        self.lambda_code_bucket_name = self.get_ssm_param("LAMBDA_CODE_BUCKET")
        self.lambda_code_bucket = s3.Bucket.from_bucket_name(
            self, "LambdaCodeBucket", self.lambda_code_bucket_name
        )

        """
        S3 bucket to store zoom recording files
        """

        two_week_lifecycle_rule = s3.LifecycleRule(
            id="DeleteAfterTwoWeeks",
            prefix="", # for development, remove later
            enabled=True,
            expiration=core.Duration.days(14),
            abort_incomplete_multipart_upload_after=core.Duration.days(1)
        )

        zoom_videos_bucket = s3.Bucket(
            self, "ZoomVideosBucket",
            bucket_name="{}-zoom-recording-files".format(self.name),
            encryption=s3.BucketEncryption.KMS_MANAGED,
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
            table_name="{}-schedule".format(self.name),
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

        on_demand_function = self._create_lambda_function(
            "zoom-on-demand", {}
        )

        # webhook lambda handles incoming webhook notifications
        webhook_environment = {
            "DOWNLOAD_QUEUE_NAME": downloads_queue.queue_name,
            "LOCAL_TIME_ZONE": self.get_ssm_param("LOCAL_TIME_ZONE"),
            "DEBUG": "0"
        }
        webhook_function = self._create_lambda_function(
            "zoom-webhook", webhook_environment)

        # downloader lambda checks for matches with the course schedule
        # and uploads matching recordings to S3
        downloader_environment = {
            "ZOOM_VIDEOS_BUCKET": zoom_videos_bucket.bucket_name,
            "DOWNLOAD_QUEUE_NAME": downloads_queue.queue_name,
            "DEADLETTER_QUEUE_NAME": downloads_dlq.queue_name,
            "UPLOAD_QUEUE_NAME": uploads_queue.queue_name,
            "CLASS_SCHEDULE_TABLE": class_schedule_table.table_name,
            "DEBUG": "0",
            "ZOOM_ADMIN_EMAIL": self.get_ssm_param("ZOOM_ADMIN_EMAIL"),
            "ZOOM_API_KEY": self.get_ssm_param("ZOOM_API_KEY"),
            "ZOOM_API_SECRET": self.get_ssm_param("ZOOM_API_SECRET"),
            "LOCAL_TIME_ZONE": self.get_ssm_param("LOCAL_TIME_ZONE"),
            "DEFAULT_SERIES_ID": self.get_ssm_param(
                "DEFAULT_SERIES_ID", required=False
            )
        }
        downloader_function = self._create_lambda_function(
            "zoom-downloader", downloader_environment)

        # uploader lambda uploads recordings to opencast
        uploader_ssm_params = [
            "OPENCAST_BASE_USER",
            "DEFAULT_PUBLISHER",
            "OVERRIDE_PUBLISHER",
            "OVERRIDE_CONTRIBUTOR",
            "OC_WORKFLOW",
            "OC_FLAVOR",
            "UPLOAD_MESSAGES_PER_INVOCATION"
        ]
        uploader_environment = {
            var: self.get_ssm_param(var) for var in uploader_ssm_params
        }
        uploader_environment.update({
            "OPENCAST_BASE_URL": self._oc_base_url,
            "ZOOM_VIDEOS_BUCKET": zoom_videos_bucket.bucket_name,
            "UPLOAD_QUEUE_NAME": uploads_queue.queue_name,
            "DEBUG": "0"
        })
        uploader_function = self._create_lambda_function(
            "zoom-uploader", uploader_environment)

        op_counts_function = self._create_lambda_function(
            "opencast-op-counts", {}
        )

        log_notification_function = self._create_lambda_function(
            "zoom-log-notifications", {}
        )


        """
        API definition
        """

        api = apigw.LambdaRestApi(
            self, "ZoomIngesterApi",
            handler=webhook_function,
            rest_api_name=self.name,
            proxy=False,
            deploy=True,
            deploy_options=apigw.StageOptions(
                data_trace_enabled=True,
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
                stage_name=self.lambda_release_alias
            )
        )

        api_resource_new_recording = apigw.Resource(
            self, "ZoomIngesterResource",
            parent=api.root,
            path_part="new_recording"
        )

        api_method_new_recording = apigw.Method(
            self, "ZoomIngesterWebhook",
            http_method="POST",
            resource=api_resource_new_recording,
            options=apigw.MethodOptions(
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
        )

        api_resource_on_demand_ingest = apigw.Resource(
            self, "ZoomIngesterOnDemandResource",
            parent=api.root,
            path_part="ingest"
        )

        api_method_on_demand_ingest = apigw.Method(
            self, "ZoomIngesterOnDemandEndpoint",
            http_method="POST",
            resource=api_resource_on_demand_ingest,
            options=apigw.MethodOptions(
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
        )

        api_key = apigw.ApiKey(
            self, "ZoomIngesterApiKey",
            resources=[api]
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
            project_name="{}-codebuild".format(self.stack_name),
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
            topic_name="{}-notification-topic".format(self.stack_name)
        )

        sns_topic.add_subscription(
            sns_subscriptions.EmailSubscription(self.notification_email)
        )


        """
        CloudWatch Alarms
        """

        self._create_metric_alarm(webhook_function, "zoom-webhook", "Errors", sns_topic)
        self._create_metric_alarm(webhook_function, "zoom-webhook", "4xx", sns_topic)
        self._create_metric_alarm(webhook_function, "zoom-webhook", "5xx", sns_topic)
        self._create_metric_alarm(webhook_function, "zoom-webhook", "Latency", sns_topic)

        self._create_metric_alarm(downloader_function, "zoom-downloader", "Errors", sns_topic)
        self._create_metric_alarm(downloader_function, "zoom-downloader", "Invocations", sns_topic)

        self._create_metric_alarm(uploader_function, "zoom-uploader", "Errors", sns_topic)

        """
        Metric Filters
        """


        # TEMPORARY!
        # remove random hash from logical id
        # for development, remove later
        resources = [
            zoom_videos_bucket,
            downloads_queue,
            downloads_dlq,
            uploads_queue,
            uploads_dlq,
            class_schedule_table,
            webhook_function,
            downloader_function,
            uploader_function,
            op_counts_function,
            on_demand_function,
            log_notification_function,
            codebuild_project,
            api,
            api_key,
            api_resource_new_recording,
            api_method_new_recording,
            api_resource_on_demand_ingest,
            api_method_on_demand_ingest,
            download_trigger,
            upload_trigger,
            sns_topic
        ]
        for resource in resources:
            cfn_resource = resource.node.default_child
            cfn_id = resource.node.id
            cfn_resource.override_logical_id(cfn_id)

    def get_ssm_param(self, param_name, required=True):
        return getenv(param_name, required)

    @property
    def _oc_base_url(self):

        oc_cluster_name = self.get_ssm_param("OC_CLUSTER_NAME")

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
                "No dns name found for OC_CLUSTER_NAME {}"
                .format(oc_cluster_name)
            )
        dns_name = result["Reservations"][0]["Instances"][0]["PublicDnsName"]

        url = "http://" + dns_name.strip()

        return url

    def _create_event_rule(self, function, rule_name, rate_in_minutes):
        return events.Rule(
            self, "Zoom{}EventRule".format(rule_name.capitalize()),
            rule_name="{}-{}-rule".format(self.name, rule_name),
            enabled=True,
            schedule=events.Schedule.rate(
                core.Duration.minutes(rate_in_minutes)
            ),
            targets=[events_targets.LambdaFunction(function)]
        )

    def _create_lambda_function(self, function_name, environment):
        lambda_id = "{}Function".format(
            ''.join([x.capitalize() for x in function_name.split('-')])
        )

        environment = {key:str(val) for key,val in environment.items() if val}

        function = _lambda.Function(
            self, lambda_id,
            function_name="{}-{}-function"
                          .format(self.name, function_name),
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_bucket(
                self.lambda_code_bucket,
                "{}/{}.zip".format(self.name, function_name)
                ),
            handler="{}.handler".format(function_name),
            timeout=core.Duration.seconds(30),
            environment=environment
        )

        latest = function.add_version("$LATEST")

        alias = _lambda.Alias(
            self, "{}Alias".format(lambda_id),
            version=latest,
            description="initial release",
            alias_name=self.lambda_release_alias
        )

        # remove random hash from logical id
        # temporary, for development, remove later
        resources = [
            alias
        ]
        for resource in resources:
            cfn_resource = resource.node.default_child
            cfn_id = resource.node.id
            cfn_resource.override_logical_id(cfn_id)

        return function

    def _create_queue(self, queue_name, fifo=False):

        primary_queue_name = "{}-{}".format(self.name, queue_name)
        dlq_name = "{}-deadletter".format(primary_queue_name)
        if fifo:
            primary_queue_name += ".fifo"
            dlq_name += ".fifo"

        dlq = sqs.Queue(
            self,
            "ZoomIngester{}DeadLetterQueue".format(queue_name.capitalize()),
            queue_name=dlq_name,
            retention_period=core.Duration.days(14)
        )

        # These cannot be merged because some parameters are FIFO only
        # such as fifo and content_based_deduplcation. For example, creating
        # a queue with fifo=False will cause an error and cloudformation
        # rollback.
        if fifo:
            queue = sqs.Queue(
                self, "ZoomIngester{}Queue".format(queue_name.capitalize()),
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
                self, "ZoomIngester{}Queue".format(queue_name.capitalize()),
                queue_name=primary_queue_name,
                retention_period=core.Duration.days(14),
                visibility_timeout=core.Duration.seconds(300),
                dead_letter_queue=sqs.DeadLetterQueue(
                    max_receive_count=2,
                    queue=dlq
                )
            )

        return queue, dlq

    def _create_metric_alarm(self, function, function_name, metric_name, sns_topic):

        metric_alarm_id = "{}{}MetricAlarm".format(
                ''.join([x.capitalize() for x in function_name.split('-')]),
                metric_name
        )

        metric_alarm = cloudwatch.Alarm(
            self, metric_alarm_id,
            metric=function.metric(metric_name),
            evaluation_periods=1,
            threshold=1,
            period=core.Duration.seconds(60),
            statistic="sum",
            alarm_name=function_name
        )

        metric_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(sns_topic)
        )

        # Temporary, for development, remove later
        cfn_resource = metric_alarm.node.default_child
        cfn_resource.override_logical_id(metric_alarm_id)

