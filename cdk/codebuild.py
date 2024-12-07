from aws_cdk import Stack, Duration, aws_codebuild as codebuild, aws_iam as iam
from constructs import Construct
from . import names


class ZipCodebuildProject(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        project_git_url,
        lambda_code_bucket,
        policy_resources,
    ):
        """
        CodeBuild project
        """
        super().__init__(scope, id)

        stack_name = Stack.of(self).stack_name

        self.project = codebuild.Project(
            self,
            "project",
            project_name=f"{stack_name}-{names.CODEBUILD_PROJECT}",
            source=codebuild.Source.git_hub_enterprise(
                https_clone_url=project_git_url,
                clone_depth=1,
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0,
                compute_type=codebuild.ComputeType.LARGE,
            ),
            artifacts=codebuild.Artifacts.s3(
                name=stack_name,
                bucket=lambda_code_bucket,
            ),
            badge=True,
            timeout=Duration.minutes(30),
        )

        self.project.add_to_role_policy(
            iam.PolicyStatement(
                resources=policy_resources,
                actions=[
                    "lambda:GetFunction",
                    "lambda:UpdateFunctionCode",
                    "lambda:PublishVersion",
                    "lambda:UpdateAlias",
                ],
            )
        )
