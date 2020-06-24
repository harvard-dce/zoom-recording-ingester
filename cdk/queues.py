from aws_cdk import core, aws_sqs as sqs

class ZipQueues(core.Construct):

    def __init__(self, scope: core.Construct, id: str):
        super().__init__(scope, id)
        stack_name = core.Stack.of(self).stack_name

        self.download_dlq = sqs.DeadLetterQueue(
            max_receive_count=2,
            queue=sqs.Queue(
                self, "DownloadDeadLetterQueue",
                queue_name=f"{stack_name}-download-dql",
                retention_period=core.Duration.days(14)
            )
        )
        self.download_queue = sqs.Queue(
            self, "DownloadQueue",
            queue_name=f"{stack_name}-download",
            retention_period=core.Duration.days(14),
            visibility_timeout=core.Duration.seconds(300),
            dead_letter_queue=self.download_dlq
        )

        self.upload_dql = sqs.DeadLetterQueue(
            max_receive_count=2,
            queue=sqs.Queue(
                self, "UploadDeadLetterQueue",
                queue_name=f"{stack_name}-upload-dql.fifo",
                retention_period=core.Duration.days(14)
            )
        )

        self.upload_queue = sqs.Queue(
            self, "UploadQueue",
            queue_name=f"{stack_name}-upload.fifo",
            retention_period=core.Duration.days(14),
            visibility_timeout=core.Duration.seconds(300),
            content_based_deduplication=False,
            fifo=True,
            dead_letter_queue=self.upload_dql
        )
