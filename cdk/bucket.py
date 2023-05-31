from aws_cdk import Stack, Duration, RemovalPolicy, aws_s3 as s3
from constructs import Construct
from . import names


class ZipRecordingsBucket(Construct):
    def __init__(self, scope: Construct, id: str):
        """
        S3 bucket to store zoom recording files
        """
        super().__init__(scope, id)

        one_week_lifecycle_rule = s3.LifecycleRule(
            id="DeleteAfterOneWeek",
            enabled=True,
            expiration=Duration.days(7),
            abort_incomplete_multipart_upload_after=Duration.days(1),
        )

        stack_name = Stack.of(self).stack_name

        self.bucket = s3.Bucket(
            self,
            "bucket",
            bucket_name=f"{stack_name}-{names.RECORDINGS_BUCKET}",
            lifecycle_rules=[one_week_lifecycle_rule],
            removal_policy=RemovalPolicy.DESTROY,
        )
