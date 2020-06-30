from aws_cdk import core, aws_codebuild as codebuild, aws_iam as iam
from . import names

class ZipCodebuildProject(core.Construct):

    def __init__(self, scope: core.Construct, id: str,
            project_git_url,
            lambda_code_bucket,
            policy_resources
    ):
        """
        CodeBuild project
        """
        super().__init__(scope, id)

        stack_name = core.Stack.of(self).stack_name

        self.project = codebuild.Project(
            self, "project",
            project_name=f"{stack_name}-{names.CODEBUILD_PROJECT}",
            source=codebuild.Source.git_hub_enterprise(
                https_clone_url=project_git_url,
                clone_depth=1
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.AMAZON_LINUX_2_2,
                compute_type=codebuild.ComputeType.MEDIUM
            ),
            artifacts=codebuild.Artifacts.s3(
                name=stack_name,
                bucket=lambda_code_bucket
            ),
            badge=True,
            timeout=core.Duration.minutes(5)
        )

        self.project.add_to_role_policy(
            iam.PolicyStatement(
                resources=policy_resources,
                actions=[
                    "lambda:UpdateFunctionCode",
                    "lambda:PublishVersion",
                    "lambda:UpdateAlias"
                ]
            )
        )
