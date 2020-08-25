from aws_cdk import (
    core,
    aws_s3 as s3
)

from .bucket import ZipRecordingsBucket
from .queues import ZipQueues
from .schedule import ZipSchedule
from .function import (
    ZipDownloaderFunction,
    ZipOnDemandFunction,
    ZipUploaderFunction,
    ZipOpCountsFunction,
    ZipWebhookFunction,
    ZipLogNotificationsFunction,
    ZipScheduleUpdateFunction,
)
from .api import ZipApi
from .events import ZipEvent
from .codebuild import ZipCodebuildProject
from .monitoring import ZipMonitoring
from . import names


class ZipStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str,
            lambda_code_bucket,
            notification_email,
            zoom_api_key,
            zoom_api_secret,
            local_time_zone,
            default_series_id,
            download_message_per_invocation,
            opencast_api_user,
            opencast_api_password,
            default_publisher,
            override_publisher,
            override_contributor,
            oc_workflow,
            oc_flavor,
            oc_track_upload_max,
            oc_base_url,
            oc_db_url,
            ingest_allowed_ips,
            zoom_admin_id,
            oc_vpc_id,
            oc_security_group_id,
            downloader_event_rate,
            uploader_event_rate,
            project_git_url,
            gsheets_doc_id,
            gsheets_sheet_name,
            **kwargs
            ) -> None:
        
        super().__init__(scope, id, **kwargs)

        monitoring = ZipMonitoring(self, 'ZipMonitoring',
            notification_email=notification_email,
        )

        # S3 bucket that stores packaged lambda functions
        lambda_code_bucket = s3.Bucket.from_bucket_name(
            self, "LambdaCodeBucket", lambda_code_bucket
        )

        recordings_bucket = ZipRecordingsBucket(self, "RecordingsBucket")

        queues = ZipQueues(self, "Queues")

        schedule = ZipSchedule(self, "Schedule")

        schedule_update = ZipScheduleUpdateFunction(self, "ScheduleUpdateFunction",
            name=names.SCHEDULE_UPDATE_FUNCTION,
            lambda_code_bucket=lambda_code_bucket,
            environment={
                "CLASS_SCHEDULE_TABLE": schedule.table.table_name,
                "GSHEETS_DOC_ID": gsheets_doc_id,
                "GSHEETS_SHEET_NAME": gsheets_sheet_name,
            }
        )
        # grant schedule update function access to dynamo
        schedule.table.grant_read_write_data(schedule_update.function)

        on_demand = ZipOnDemandFunction(self, "OnDemandFunction",
            name=names.ON_DEMAND_FUNCTION,
            lambda_code_bucket=lambda_code_bucket,
            environment={
                "ZOOM_API_KEY": zoom_api_key,
                "ZOOM_API_SECRET": zoom_api_secret
            }
        )

        webhook = ZipWebhookFunction(self, "WebhookFunction",
            name=names.WEBHOOK_FUNCTION,
            lambda_code_bucket=lambda_code_bucket,
            environment={
                "DOWNLOAD_QUEUE_NAME": queues.download_queue.queue_name,
                "LOCAL_TIME_ZONE": local_time_zone,
                "DEBUG": "0"
            }
        )

        # grant webhook lambda permission to send messages to downloads queue
        queues.download_queue.grant_send_messages(webhook.function)

        # downloader lambda checks for matches with the course schedule
        # and uploads matching recordings to S3
        downloader = ZipDownloaderFunction(self, "DownloadFunction",
            name=names.DOWNLOAD_FUNCTION,
            lambda_code_bucket = lambda_code_bucket,
            timeout=900,
            memory_size=500,
            environment={
                "ZOOM_VIDEOS_BUCKET": recordings_bucket.bucket.bucket_name,
                "DOWNLOAD_QUEUE_NAME": queues.download_queue.queue_name,
                "DEADLETTER_QUEUE_NAME": queues.download_dlq.queue.queue_name,
                "UPLOAD_QUEUE_NAME": queues.upload_queue.queue_name,
                "CLASS_SCHEDULE_TABLE": schedule.table.table_name,
                "DEBUG": "0",
                "ZOOM_ADMIN_ID": zoom_admin_id,
                "ZOOM_API_KEY": zoom_api_key,
                "ZOOM_API_SECRET": zoom_api_secret,
                "LOCAL_TIME_ZONE": local_time_zone,
                "DEFAULT_SERIES_ID": default_series_id,
                "DOWNLOAD_MESSAGES_PER_INVOCATION": download_message_per_invocation,
            }
        )

        # grant downloader function permissions
        queues.download_queue.grant_consume_messages(downloader.function)
        queues.upload_queue.grant_send_messages(downloader.function)
        schedule.table.grant_read_write_data(downloader.function)
        recordings_bucket.bucket.grant_write(downloader.function)

        op_counts = ZipOpCountsFunction(self, 'OpCountsFunction',
            name=names.OP_COUNTS_FUNCTION,
            lambda_code_bucket = lambda_code_bucket,
            vpc_id=oc_vpc_id,
            security_group_id=oc_security_group_id,
            environment={
                "OPENCAST_DB_URL": oc_db_url
            }
        )

        # uploader lambda uploads recordings to opencast
        uploader = ZipUploaderFunction(self, 'UploaderFunction',
            name=names.UPLOAD_FUNCTION,
            lambda_code_bucket=lambda_code_bucket,
            timeout=300,
            vpc_id=oc_vpc_id,
            security_group_id=oc_security_group_id,
            environment = {
                "OPENCAST_API_USER": opencast_api_user,
                "OPENCAST_API_PASSWORD": opencast_api_password,
                "DEFAULT_PUBLISHER": default_publisher,
                "OVERRIDE_PUBLISHER": override_publisher,
                "OVERRIDE_CONTRIBUTOR": override_contributor,
                "OC_WORKFLOW": oc_workflow,
                "OC_FLAVOR": oc_flavor,
                "OC_TRACK_UPLOAD_MAX": oc_track_upload_max,
                "OPENCAST_BASE_URL": oc_base_url,
                "ZOOM_VIDEOS_BUCKET": recordings_bucket.bucket.bucket_name,
                "UPLOAD_QUEUE_NAME": queues.upload_queue.queue_name,
                "DEBUG": "0",
                "OC_OP_COUNT_FUNCTION": op_counts.function.function_name
            }
        )

        # grant uploader function permissions
        queues.upload_queue.grant_consume_messages(uploader.function)
        op_counts.function.grant_invoke(uploader.function)
        # this is required so that the presigned s3 urls generated by
        # the downloader function (ffprobe check)
        # and the uploader function (for Opencast)
        # will be valid
        recordings_bucket.bucket.grant_read(downloader.function)
        recordings_bucket.bucket.grant_read(uploader.function)

        log_notify = ZipLogNotificationsFunction(self, 'LogNotificationFunction',
            name=names.LOG_NOTIFICATION_FUNCTION,
            lambda_code_bucket=lambda_code_bucket,
            environment={}
        )

        api = ZipApi(self, "RestApi",
            on_demand_function=on_demand.function,
            webhook_function=webhook.function,
            ingest_allowed_ips=ingest_allowed_ips
        )

        download_event = ZipEvent(self, "DownloadEvent",
            function=downloader.function,
            event_rate=downloader_event_rate
        )

        uploader_event = ZipEvent(self, "UploadEvent",
            function=uploader.function,
            event_rate=uploader_event_rate
        )

        codebuild_project = ZipCodebuildProject(self, "CodebuildProject",
            lambda_code_bucket=lambda_code_bucket,
            project_git_url=project_git_url,
            policy_resources=[
                on_demand.function.function_arn,
                webhook.function.function_arn,
                downloader.function.function_arn,
                uploader.function.function_arn,
                op_counts.function.function_arn,
                log_notify.function.function_arn
            ]
        )

        on_demand.add_monitoring(monitoring)
        webhook.add_monitoring(monitoring)
        downloader.add_monitoring(monitoring)
        uploader.add_monitoring(monitoring)
        op_counts.add_monitoring(monitoring)
        log_notify.add_monitoring(monitoring)
        api.add_monitoring(monitoring)
        queues.add_monitoring(monitoring)

