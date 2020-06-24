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
                queue_name=f"{self.stack_name}-download-dql",
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

        self.upload_dql = sqs.DeadLetterQueue(
            max_receive_count=2,
            queue=sqs.Queue(
                self, "UploadDeadLetterQueue",
                queue_name=f"{self.stack_name}-upload-dql.fifo",
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
            dead_letter_queue=self.upload_dql
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
