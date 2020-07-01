from aws_cdk import (
    core,
    aws_apigateway as apigw,
    aws_cloudwatch as cloudwatch,
    aws_iam as iam,
    aws_logs as logs
)

from . import names

class ZipApi(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
            webhook_function,
            on_demand_function,
            ingest_allowed_ips):
        super().__init__(scope, id)

        stack_name = core.Stack.of(self).stack_name

        policy = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions= ["execute-api:Invoke"],
                    principals=[iam.AnyPrincipal()],
                    # note that the policy is a prop of the api which cannot reference itself
                    # see the Cloudformation documentation for api gateway policy attribute
                    resources=[core.Fn.join('', ['execute-api:/', '*'])]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.DENY,
                    actions=["execute-api:Invoke"],
                    principals=[iam.AnyPrincipal()],
                    resources=[core.Fn.join('', ['execute-api:/', '*/POST/ingest'])],
                    conditions={
                        "NotIpAddress": {
                            "aws:SourceIp": ingest_allowed_ips
                        }
                    }
                )
            ]
        )

        self.rest_api_name = f"{stack_name}-{names.REST_API}"

        log_group = logs.LogGroup(self, "apilogs",
            log_group_name=f"/aws/apigateway/{self.rest_api_name}/access_logs",
            retention=logs.RetentionDays.SIX_MONTHS
        )

        self.api = apigw.LambdaRestApi(
            self, "api",
            handler=webhook_function,  # default handler
            rest_api_name=self.rest_api_name,
            proxy=False,
            deploy=True,
            policy=policy,
            deploy_options=apigw.StageOptions(
                access_log_destination=apigw.LogGroupLogDestination(log_group),
                access_log_format=apigw.AccessLogFormat.clf(),
                data_trace_enabled=True,
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
                stage_name=names.API_STAGE
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

        self.ingest_resource = self.api.root.add_resource("ingest",
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=["POST", "OPTIONS"],
                allow_headers=apigw.Cors.DEFAULT_HEADERS \
                              + ["Accept-Language","X-Requested-With"]
            )
        )
        on_demand_integration = apigw.LambdaIntegration(on_demand_function)
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

        def endpoint_url(resource_name):
            return (f"https://{self.api.rest_api_id}.execute-api."
                    f"{core.Stack.of(self).region}.amazonaws.com/"
                    f"{names.API_STAGE}/{resource_name}")

        on_demand_function.add_environment(
            "WEBHOOK_ENDPOINT_URL",
            endpoint_url("new_recording")
        )

        core.CfnOutput(self, "WebhookEndpoint",
            export_name=f"{stack_name}-{names.WEBHOOK_ENDPOINT}-url",
            value=endpoint_url("new_recording")
        )

        core.CfnOutput(self, "OnDemandEndpoint",
            export_name=f"{stack_name}-{names.ON_DEMAND_ENDPOINT}-url",
            value=endpoint_url("ingest")
        )

        core.CfnOutput(self, "WebhookResourceId",
            export_name=f"{stack_name}-{names.WEBHOOK_ENDPOINT}-resource-id",
            value=self.new_recording_resource.resource_id
        )

        core.CfnOutput(self, "OnDemandResourceId",
            export_name=f"{stack_name}-{names.ON_DEMAND_ENDPOINT}-resource-id",
            value=self.ingest_resource.resource_id
        )

        core.CfnOutput(self, "RestApiId",
            export_name=f"{stack_name}-{names.REST_API}-id",
            value=self.api.rest_api_id
        )

    def add_monitoring(self, monitoring):

        resource_metrics = [
            (self.new_recording_resource, "4XXError"),
            (self.new_recording_resource, "5XXError"),
            (self.ingest_resource, "5XXError")
        ]
        for resource, metric_name in resource_metrics:
            construct_id = f"{metric_name}-{resource.path.replace('/', '_')}-alarm"
            alarm = cloudwatch.Alarm(self, construct_id,
                metric=cloudwatch.Metric(
                    metric_name=metric_name,
                    namespace="AWS/ApiGateway",
                    dimensions={
                        "ApiName": self.rest_api_name,
                        "Stage": names.API_STAGE,
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
                    "Stage": names.API_STAGE,
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

