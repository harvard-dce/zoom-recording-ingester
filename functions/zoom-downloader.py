import boto3
import json
import requests
from os import getenv as env
from pathlib import Path
from utils import (
    setup_logging,
    zoom_api_request,
    TIMESTAMP_FORMAT,
    retrieve_schedule,
    schedule_match,
    PipelineStatus,
    set_pipeline_status,
)
import subprocess
from pytz import timezone
from datetime import datetime
import logging
import concurrent.futures
from copy import deepcopy

logger = logging.getLogger()

ZOOM_VIDEOS_BUCKET = env("ZOOM_VIDEOS_BUCKET")
DOWNLOAD_QUEUE_NAME = env("DOWNLOAD_QUEUE_NAME")
UPLOAD_QUEUE_NAME = env("UPLOAD_QUEUE_NAME")
DEADLETTER_QUEUE_NAME = env("DEADLETTER_QUEUE_NAME")
MIN_CHUNK_SIZE = 5242880
MEETING_LOOKUP_RETRIES = 2
MEETING_LOOKUP_RETRY_DELAY = 60
ZOOM_ADMIN_ID = env("ZOOM_ADMIN_ID")
DEFAULT_SERIES_ID = env("DEFAULT_SERIES_ID")
CLASS_SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
DOWNLOAD_MESSAGES_PER_INVOCATION = env("DOWNLOAD_MESSAGES_PER_INVOCATION")
# Ignore recordings that are less than MIN_DURATION (in minutes)
MINIMUM_DURATION = int(env("MINIMUM_DURATION", 2))


class PermanentDownloadError(Exception):
    pass


class RetryableDownloadError(Exception):
    pass


class ZoomDownloadLinkError(Exception):
    pass


# abstraction for unit testing
def sqs_resource():
    return boto3.resource("sqs")


@setup_logging
def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DynamoDB table
    """

    ignore_schedule = event.get("ignore_schedule", False)
    override_series_id = event.get("override_series_id")
    sqs = sqs_resource()

    # try DOWNLOAD_MESSAGES_PER_INVOCATION number of times to retrieve
    # a recording that matches the class schedule
    dl, download_message = None, None
    download_queue = sqs.get_queue_by_name(QueueName=DOWNLOAD_QUEUE_NAME)
    for _ in range(int(DOWNLOAD_MESSAGES_PER_INVOCATION)):
        download_message = retrieve_message(download_queue)
        if not download_message:
            logger.info("No download queue messages available.")
            return

        dl_data = json.loads(download_message.body)
        dl = Download(sqs, dl_data)

        # this is checking for ~total~ duration of the recording as reported
        # by zoom in the webhook payload data. There is a separate check later
        # for the duration potentially different sets of files
        if dl.duration >= MINIMUM_DURATION or "on_demand_series_id" in dl_data:
            if dl.oc_series_found(ignore_schedule, override_series_id):
                set_pipeline_status(
                    dl_data["zip_id"],
                    PipelineStatus.OC_SERIES_FOUND,
                    oc_series_id=dl.opencast_series_id,
                )
                break
            else:
                failure_msg = {"no_oc_series_found": dl_data}
                set_pipeline_status(
                    dl_data["zip_id"],
                    PipelineStatus.IGNORED,
                    reason="No opencast series match",
                )
        else:
            failure_msg = {"recording_too_short": dl_data}
            set_pipeline_status(
                dl_data["zip_id"],
                PipelineStatus.IGNORED,
                reason=f"Recording <{MINIMUM_DURATION} minutes",
            )

        # discard and keep checking messages for schedule match
        logger.info(failure_msg)
        download_message.delete()
        dl, download_message = None, None

    if not dl:
        logger.info("No available recordings match the class schedule.")
        return

    global ADMIN_TOKEN
    ADMIN_TOKEN = get_admin_token()

    try:
        # upload matched recording to S3 and verify MP4 integrity
        dl.upload_to_s3()
    except PermanentDownloadError as e:
        # push message to deadletter queue, add error reason to message
        set_pipeline_status(
            dl.data["zip_id"],
            PipelineStatus.DOWNLOADER_FAILED,
            reason=f"permanent failure: {e}",
        )
        message = dl.send_to_deadletter_queue(e)
        set_pipeline_status(dl.data["zip_id"], PipelineStatus.SENT_TO_UPLOADER)
        download_message.delete()
        logger.error({"Error": e, "Sent to deadletter": message})
        raise
    except RetryableDownloadError as e:
        set_pipeline_status(
            dl.data["zip_id"],
            PipelineStatus.DOWNLOADER_FAILED,
            reason=f"retryable failure: {e}",
        )
        raise

    # send a message to the opencast uploader
    message = dl.send_to_uploader_queue()
    download_message.delete()
    logger.info({"sqs_message": message})


def retrieve_message(queue):
    messages = queue.receive_messages(
        MaxNumberOfMessages=1, VisibilityTimeout=700
    )
    if not messages:
        return None

    return messages[0]


def get_admin_token():
    # get admin level zak token from admin id
    r = zoom_api_request(f"users/{ZOOM_ADMIN_ID}/token?type=zak")
    return r.json()["token"]


class Download:
    def __init__(self, sqs, data):
        self.sqs = sqs
        self.download_message = deepcopy(data)  # do not modify
        self.data = data  # modify
        self.opencast_series_id = None
        self.title = "Lecture"  # default title

        logger.info({"download_message": self.download_message})

    @property
    def host_name(self):
        if not hasattr(self, "_host_name"):
            resp = zoom_api_request(f"users/{self.data['host_id']}").json()
            logger.info({"Host details": resp})
            self._host_name = f"{resp['first_name']} {resp['last_name']}"
        return self._host_name

    @property
    def uuid(self):
        return self.data["uuid"]

    @property
    def zoom_series_id(self):
        return self.data["zoom_series_id"]

    @property
    def duration(self):
        return int(self.data.get("duration", 0))

    @property
    def recording_files(self):
        if not hasattr(self, "_recording_files"):

            # all incoming files
            files = self.data["recording_files"]

            # number of distinct start times is the count of segments
            segment_start_times = sorted(
                set([file["recording_start"] for file in files])
            )

            # collect the recording files here as ZoomFile objs
            zoom_files = []

            # segment refers to the sets of recording files that
            # are generated when a host stops and restarts a meeting
            # NOTE: segment numbering starts at 0 (zero)
            for segment_num, start_time in enumerate(segment_start_times):
                # all the files from this segment
                segment_files = [
                    f for f in files if f["recording_start"] == start_time
                ]

                for file in segment_files:
                    file["meeting_uuid"] = self.uuid
                    file["zoom_series_id"] = self.zoom_series_id
                    file["created_local"] = self._created_local
                    zoom_files.append(ZoomFile(file, segment_num))

            self._recording_files = zoom_files

        return self._recording_files

    @property
    def _created_utc(self):
        """
        UTC time object for recording start.
        """
        utc = datetime.strptime(
            self.data["start_time"], TIMESTAMP_FORMAT
        ).replace(tzinfo=timezone("UTC"))
        return utc

    @property
    def _created_local(self):
        """
        Local time object for recording start.
        """
        tz = timezone(LOCAL_TIME_ZONE)
        return self._created_utc.astimezone(tz)

    @property
    def _series_id_from_schedule(self):
        """
        Check that the recording's start_time matches the schedule and
        extract the opencast series id.
        """
        schedule = retrieve_schedule(self.data["zoom_series_id"])
        if not schedule:
            return None

        event = schedule_match(schedule, self._created_local)
        if event:
            self.title = event["title"]
            return schedule["opencast_series_id"]

        logger.info("No opecast series match found")
        return None

    def oc_series_found(self, ignore_schedule=False, override_series_id=None):

        if override_series_id:
            self.opencast_series_id = override_series_id
            logger.info(
                f"Using override series id '{self.opencast_series_id}'"
            )
            return True

        if "on_demand_series_id" in self.data:
            self.opencast_series_id = self.data["on_demand_series_id"]
            logger.info(
                "Using on-demand provided series id "
                f"'{self.opencast_series_id}'"
            )
            return True

        if ignore_schedule:
            logger.info("Ignoring schedule")
        else:
            self.opencast_series_id = self._series_id_from_schedule
            if self.opencast_series_id:
                logger.info(
                    "Matched with opencast series "
                    f"'{self.opencast_series_id}'!"
                )
                return True

        if DEFAULT_SERIES_ID and DEFAULT_SERIES_ID != "None":
            logger.info(f"Using default series id {DEFAULT_SERIES_ID}")
            self.opencast_series_id = DEFAULT_SERIES_ID
            return True

        logger.info("No opencast series found.")
        return False

    @property
    def upload_message(self):
        if not hasattr(self, "self._upload_message"):
            s3_files = {}
            segment_durations = {}
            for file in self.downloaded_files:
                segment = {
                    "filename": file.s3_filename,
                    "segment_num": file.segment_num,
                    "recording_start": file.file_data["recording_start"],
                    "recording_end": file.file_data["recording_end"],
                    "ffprobe_bytes": file.file_data["ffprobe_bytes"],
                    "ffprobe_seconds": file.file_data["ffprobe_seconds"],
                }
                segment_durations[segment["recording_start"]] = segment[
                    "ffprobe_seconds"
                ]
                if file.recording_type in s3_files:
                    s3_files[file.recording_type]["segments"].append(segment)
                else:
                    s3_files[file.recording_type] = {"segments": [segment]}

            all_file_bytes = 0
            all_file_seconds = 0
            for view_data in s3_files.values():
                view_data["view_bytes"] = sum(
                    s["ffprobe_bytes"] for s in view_data["segments"]
                )
                view_data["view_seconds"] = sum(
                    s["ffprobe_seconds"] for s in view_data["segments"]
                )
                all_file_bytes += view_data["view_bytes"]
                all_file_seconds += view_data["view_seconds"]

            recording_seconds = sum(
                duration for duration in segment_durations.values()
            )

            self._upload_message = {
                "uuid": self.data["uuid"],
                "zoom_series_id": self.data["zoom_series_id"],
                "opencast_series_id": self.opencast_series_id,
                "oc_title": self.title,
                "host_name": self.host_name,
                "topic": self.data["topic"],
                "created": datetime.strftime(
                    self._created_utc, TIMESTAMP_FORMAT
                ),
                "created_local": datetime.strftime(
                    self._created_local, TIMESTAMP_FORMAT
                ),
                "webhook_received_time": self.data["received_time"],
                "zip_id": self.data["zip_id"],
                "s3_files": s3_files,
                "total_file_bytes": all_file_bytes,
                "total_file_seconds": all_file_seconds,
                "total_segment_seconds": recording_seconds,
            }

            for field in ["allow_multiple_ingests", "zoom_processing_minutes"]:
                if field in self.data:
                    self._upload_message[field] = self.data[field]

        return self._upload_message

    def upload_to_s3(self):

        logger.info(f"downloading {len(self.recording_files)} files")

        self.downloaded_files = []
        for file in self.recording_files:
            try:
                file.stream_file_to_s3()
                self.downloaded_files.append(file)
            except ZoomDownloadLinkError:
                logger.warning(
                    {"Error accessing possibly deleted file": file.file_data}
                )
                continue
        if not self.downloaded_files:
            raise PermanentDownloadError(
                "No files could be downloaded for this recording"
            )
        logger.info(
            f"successfully downloaded {len(self.downloaded_files)} files"
        )

    def send_to_deadletter_queue(self, error):
        deadletter_queue = self.sqs.get_queue_by_name(
            QueueName=DEADLETTER_QUEUE_NAME
        )
        message = SQSMessage(deadletter_queue, self.download_message)
        message.send(error=error)
        return self.download_message

    def send_to_uploader_queue(self):
        upload_queue = self.sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)
        message = SQSMessage(upload_queue, self.upload_message)
        message.send()
        return self.upload_message


class SQSMessage:
    def __init__(self, queue, message):
        self.queue = queue
        self.message = message

    def send(self, error=None):
        logger.info(
            {
                "sqs_destination_queue": self.queue.url,
                "sqs_message": self.message,
            }
        )

        if not error:
            message_attributes = {}
        else:
            message_attributes = {
                "FailedReason": {
                    "StringValue": str(error),
                    "DataType": "String",
                }
            }

        try:
            if "fifo" in self.queue.url:
                message_sent = self.queue.send_message(
                    MessageBody=json.dumps(self.message),
                    MessageGroupId=self.message["uuid"],
                    MessageAttributes=message_attributes,
                )
            else:
                message_sent = self.queue.send_message(
                    MessageBody=json.dumps(self.message),
                    MessageAttributes=message_attributes,
                )
        except Exception as e:
            msg = (
                f"Error when sending SQS message to queue {self.queue.url}:{e}"
            )
            logger.exception(msg)
            set_pipeline_status(
                self.message["zip_id"],
                PipelineStatus.DOWNLOADER_FAILED,
                reason=f"retryable failure: {msg}",
            )
            raise

        logger.debug(
            {
                "Queue": self.queue.url,
                "Message sent": message_sent,
                "FailedReason": error,
            }
        )


class ZoomFile:
    def __init__(self, file_data, segment_num):
        self.file_data = file_data
        self.segment_num = segment_num
        self.recording_type = self.__standardized_recording_type(
            file_data["recording_type"]
        )
        self.s3 = boto3.client("s3")

    def __standardized_recording_type(self, name):
        """
        Zoom does not guarantee these recording/view types will always be
        provided in exactly the same format. For example,
        `shared_screen_with_speaker_view` could also be
        `shared_screen_with_speaker_view(CC)`. Since the exact format is
        unpredictable, it is safer to standardize.
        """
        if "screen" in name.lower():
            if "speaker" in name.lower():
                return "shared_screen_with_speaker_view"
            if "gallery" in name.lower():
                return "shared_screen_with_gallery_view"
            return "shared_screen"
        elif "speaker" in name.lower():
            return "active_speaker"
        elif "gallery" in name.lower():
            return "gallery_view"
        return f"unrecognized_type_{name}"

    @property
    def zoom_filename(self):
        if not hasattr(self, "_zoom_filename"):
            # First request is for retrieving the filename
            url = f"{self.file_data['download_url']}?zak={ADMIN_TOKEN}"
            r = requests.get(url, allow_redirects=False)
            r.raise_for_status

            # If the file has been deleted or if the request is not authorized,
            # Zoom will return 200 and an HTML error page.
            if (
                "Content-Type" in r.headers
                and "text/html" in r.headers["Content-Type"]
            ):
                raise ZoomDownloadLinkError()

            # Filename that zoom uses should be found in the response headers
            if "Location" in r.headers:
                location = r.headers["Location"]
                zoom_name = location.split("?")[0].split("/")[-1]
            elif "Content-Disposition" in r.headers:
                zoom_name = r.headers["Content-Disposition"].split("=")[-1]
            else:
                raise PermanentDownloadError(
                    "Request for download stream from Zoom failed.\n"
                    "Zoom name not found in headers\n"
                    f"request: {url} response headers: {r.headers}"
                )
            self._zoom_filename = zoom_name.lower()
            logger.info(f"got filename {zoom_name}")

        return self._zoom_filename

    @property
    def s3_filename(self):
        if not hasattr(self, "_s3_filename"):
            ts = int(self.file_data["created_local"].timestamp())
            self._s3_filename = "{}/{}/{:03d}-{}.{}".format(
                self.file_data["zoom_series_id"],
                ts,
                self.segment_num,
                self.recording_type,
                self.file_extension,
            )
        return self._s3_filename

    def valid_mp4_file(self):
        # TODO: if file not found, add appropriate error
        url = self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": ZOOM_VIDEOS_BUCKET, "Key": self.s3_filename},
        )

        cmd = f"/var/task/ffprobe -of json -show_format {url}"
        r = subprocess.run(
            cmd.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout = json.loads(r.stdout)
        logger.info(
            {
                "ffprobe": {
                    "command": cmd,
                    "return_code": r.returncode,
                    "stdout": stdout,
                    "stderr": r.stderr,
                }
            }
        )

        if r.returncode == 1 or stdout["format"]["probe_score"] < 100:
            logger.warning(
                "Corrupt MP4, need to retry download "
                "from zoom to S3. {}\n{}".format(url, r.stderr)
            )
            return False

        self.file_data["ffprobe_seconds"] = float(stdout["format"]["duration"])
        self.file_data["ffprobe_bytes"] = int(stdout["format"]["size"])

        logger.debug("Successfully verified mp4 {}".format(url))
        return True

    @property
    def stream(self):
        if not hasattr(self, "_stream"):
            logger.info("requesting {}".format(self.file_data["download_url"]))
            url = "{}?zak={}".format(
                self.file_data["download_url"], ADMIN_TOKEN
            )
            r = requests.get(url, stream=True)
            r.raise_for_status

            self._stream = r

        return self._stream

    @property
    def file_extension(self):
        return Path(self.zoom_filename).suffix[1:]

    def upload_part(self, upload_id, part_number, chunk):
        part = self.s3.upload_part(
            Body=chunk,
            Bucket=ZOOM_VIDEOS_BUCKET,
            Key=self.s3_filename,
            PartNumber=part_number,
            UploadId=upload_id,
        )

        return part

    def stream_file_to_s3(self):

        metadata = {
            "uuid": self.file_data["meeting_uuid"],
            "file_id": self.file_data["recording_id"],
            "file_type": self.file_extension,
        }

        logger.info(
            {"uploading file to S3": self.s3_filename, "metadata": metadata}
        )
        parts = []
        mpu = self.s3.create_multipart_upload(
            Bucket=ZOOM_VIDEOS_BUCKET, Key=self.s3_filename, Metadata=metadata
        )

        try:
            chunks = enumerate(
                self.stream.iter_content(chunk_size=MIN_CHUNK_SIZE), 1
            )
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_map = {}
                for part_number, chunk in chunks:
                    f = executor.submit(
                        self.upload_part, mpu["UploadId"], part_number, chunk
                    )
                    future_map[f] = part_number

                for future in concurrent.futures.as_completed(future_map):
                    part_number = future_map[future]
                    part = future.result()
                    parts.append(
                        {"PartNumber": part_number, "ETag": part["ETag"]}
                    )

            # complete_multipart_upload requires parts in order by part number
            parts = sorted(parts, key=lambda i: i["PartNumber"])

            self.s3.complete_multipart_upload(
                Bucket=ZOOM_VIDEOS_BUCKET,
                Key=self.s3_filename,
                UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": parts},
            )
            print("Completed multipart upload of {}.".format(self.s3_filename))
        except Exception as e:
            logger.exception(
                "Something went wrong with upload of {}:{}".format(
                    self.s3_filename, e
                )
            )
            self.s3.abort_multipart_upload(
                Bucket=ZOOM_VIDEOS_BUCKET,
                Key=self.s3_filename,
                UploadId=mpu["UploadId"],
            )
            raise

        if self.file_extension == "mp4":
            if not self.valid_mp4_file():
                self.stream.close()
                raise RetryableDownloadError("MP4 failed to transfer.")

        self.stream.close()
