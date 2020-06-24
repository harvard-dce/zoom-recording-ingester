from aws_cdk import core, aws_lambda, aws_ec2 as ec2

class ZipFunction(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
                 function_name,
                 handler,
                 lambda_code_bucket,
                 environment,
                 timeout=30,
                 vpc_id=None,
                 security_group_id=None):
        super().__init__(scope, id)

        stack_name = core.Stack.of(self).stack_name
        environment = {key: str(val) for key,val in environment.items() if val}

        function_props = {
            "function_name": function_name,
            "runtime": aws_lambda.Runtime.PYTHON_3_8,
            "code": aws_lambda.Code.from_bucket(
                bucket=lambda_code_bucket,
                key=f"{stack_name}/{function_name}.zip"
            ),
            "handler": handler,
            "timeout": core.Duration.seconds(timeout),
            "environment": environment,
        }

        if vpc_id is not None and security_group_id is not None:
            function_props["vpc"] = ec2.Vpc.from_lookup(self, "vpc", vpc_id=vpc_id),
            function_props["security_group"] = ec2.SecurityGroup.from_security_group_id(
                self, "securitygroup", security_group_id=security_group_id
            )

        self.function = aws_lambda.Function(self, "function", **function_props)
        self.alias = aws_lambda.Alias(
            self, "alias",
            version=self.function.add_version("$LATEST"),
            description="initial release",
            alias_name="live"
        )
