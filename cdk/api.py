from aws_cdk import (
    core,
    aws_apigateway as apigw,
    aws_cloudwatch as cloudwatch
)

class ZipApi(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
            webhook_function,
            on_demand_function,
            lambda_release_alias):
        super().__init__(scope, id)

        stack_name = core.Stack.of(self).stack_name

        self.rest_api_name = f"{stack_name}-api"
        self.api = apigw.LambdaRestApi(
            self, "api",
            handler=webhook_function.function,  # default handler
            rest_api_name=self.rest_api_name,
            proxy=False,
            deploy=True,
            deploy_options=apigw.StageOptions(
                data_trace_enabled=True,
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
                stage_name="live"
            )
        )

        self.api.add_api_key("ZoomIngesterApiKey")

        self.new_recording_resource = self.api.root.add_resource("new_recording")
        self.new_recording_method = self.new_recording_resource.add_method(
            "POST",
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": apigw.Model.EMPTY_MODEL
                    }
                )
            ]
        )

        self.ingest_resource = self.api.root.add_resource("ingest")
        on_demand_integration = apigw.LambdaIntegration(on_demand_function.function)
        self.ingest_method = self.ingest_resource.add_method(
            "POST",
            on_demand_integration,
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_models={
                        "application/json": apigw.Model.EMPTY_MODEL
                    }
                )
            ]
        )

        on_demand_function.function.add_environment(
            "WEBHOOK_ENDPOINT_URL",
            (f"https://{self.api.rest_api_id}.execute-api.{core.Stack.of(self).region}"
             f".amazonaws.com/{lambda_release_alias}/new_recording")
        )

    def add_monitoring(self, monitoring):

        for metric_name in ["4XXError", "5XXError"]:
            for resource in [self.new_recording_resource, self.ingest_resource]:
                alarm = cloudwatch.Alarm(self, f"{metric_name}Alarm",
                    metric=cloudwatch.Metric(
                        metric_name=metric_name,
                        namespace="AWS/ApiGateway",
                        dimensions={
                            "ApiName": self.rest_api_name,
                            "Stage": "live",
                            "Method": "POST",
                            "Resource": resource.path,
                        },
                        period=core.Duration.minutes(1)
                    ),
                    statistic="sum",
                    threshold=1,
                    evaluation_periods=1,
                    comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
                )
                monitoring.add_alarm_action(alarm)

        webhook_latency_alarm = cloudwatch.Alarm(self, "WebhookLatencyAlarm",
            metric=cloudwatch.Metric(
                metric_name="Latency",
                namespace="AWS/ApiGateway",
                dimensions={
                    "ApiName": self.rest_api_name,
                    "Stage": "live",
                    "Method": "POST",
                    "Resource": self.new_recording_resource.path,
                },
                period=core.Duration.minutes(1),
            ),
            statistic="avg",
            threshold=10000,
            evaluation_periods=3,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
        )
        monitoring.add_alarm_action(webhook_latency_alarm)

