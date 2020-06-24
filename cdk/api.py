from aws_cdk import core, aws_apigateway as apigw

class ZipApi(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
            webhook_function,
            on_demand_function):
        super().__init__(scope, id)

        stack_name = core.Stack.of(self).stack_name

        api = apigw.LambdaRestApi(
            self, "api",
            handler=webhook_function,  # default handler
            rest_api_name=f"{stack_name}-api",
            proxy=False,
            deploy=True,
            deploy_options=apigw.StageOptions(
                data_trace_enabled=True,
                metrics_enabled=True,
                logging_level=apigw.MethodLoggingLevel.INFO,
                stage_name="live"
            )
        )

        api.add_api_key("ZoomIngesterApiKey")

        new_recording = api.root.add_resource("new_recording")
        new_recording_method = new_recording.add_method(
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

        ingest = api.root.add_resource("ingest")
        on_demand_integration = apigw.LambdaIntegration(on_demand_function)
        ingest_method = ingest.add_method(
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

        on_demand_function.add_environment(
            "WEBHOOK_ENDPOINT_URL",
            (f"https://{api.rest_api_id}.execute-api.{self.region}"
             f".amazonaws.com/{self.lambda_release_alias}/new_recording")
        )

