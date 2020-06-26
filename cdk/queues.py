from aws_cdk import (
    core,
    aws_sqs as sqs,
    aws_cloudwatch as cloudwatch
)

class ZipQueues(core.Construct):

    def __init__(self, scope: core.Construct, id: str):
        super().__init__(scope, id)
        self.stack_name = core.Stack.of(self).stack_name

        self.download_dlq = sqs.DeadLetterQueue(
            max_receive_count=2,
            queue=sqs.Queue(
                self, "DownloadDeadLetterQueue",
                queue_name=f"{self.stack_name}-download-dlq",
                retention_period=core.Duration.days(14)
            )
        )
        self.download_queue = sqs.Queue(
            self, "DownloadQueue",
            queue_name=f"{self.stack_name}-download",
            retention_period=core.Duration.days(14),
            visibility_timeout=core.Duration.seconds(300),
            dead_letter_queue=self.download_dlq
        )

        self.upload_dlq = sqs.DeadLetterQueue(
            max_receive_count=2,
            queue=sqs.Queue(
                self, "UploadDeadLetterQueue",
                queue_name=f"{self.stack_name}-upload-dlq.fifo",
                retention_period=core.Duration.days(14)
            )
        )

        self.upload_queue = sqs.Queue(
            self, "UploadQueue",
            queue_name=f"{self.stack_name}-upload.fifo",
            retention_period=core.Duration.days(14),
            visibility_timeout=core.Duration.seconds(300),
            content_based_deduplication=False,
            fifo=True,
            dead_letter_queue=self.upload_dlq
        )

        url_exports = [
            ("download-queue", self.download_queue),
            ("download-dlq", self.download_dlq.queue),
            ("updload-queue", self.upload_queue),
            ("upload-dlq", self.upload_dlq.queue)
        ]
        for name, queue in url_exports:
            core.CfnOutput(self, f"{name}Export",
                export_name=f"{self.stack_name}-{name}-url",
                value=queue.queue_url
            )

    def add_monitoring(self, monitoring):

        upload_queue_alarm = cloudwatch.Alarm(self, "UploadQueueAlarm",
            metric=self.upload_queue.metric("ApproximateNumberOfMessagesVisible"),
            statistic="sum",
            threshold=40,
            period=core.Duration.minutes(5),
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD
        )
        monitoring.add_alarm_action(upload_queue_alarm)
        monitoring.add_insufficient_data_action(upload_queue_alarm)
