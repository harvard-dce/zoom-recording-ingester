from aws_cdk import core, aws_s3 as s3
from . import names


class ZipRecordingsBucket(core.Construct):
    def __init__(self, scope: core.Construct, id: str):
        """
        S3 bucket to store zoom recording files
        """
        super().__init__(scope, id)

        one_week_lifecycle_rule = s3.LifecycleRule(
            id="DeleteAfterOneWeek",
            enabled=True,
            expiration=core.Duration.days(7),
            abort_incomplete_multipart_upload_after=core.Duration.days(1),
        )

        stack_name = core.Stack.of(self).stack_name

        self.bucket = s3.Bucket(
            self,
            "bucket",
            bucket_name=f"{stack_name}-{names.RECORDINGS_BUCKET}",
            lifecycle_rules=[one_week_lifecycle_rule],
            removal_policy=core.RemovalPolicy.DESTROY,
        )
