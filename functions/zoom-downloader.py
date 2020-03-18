import boto3
import json
import requests
from os import getenv as env
from common import setup_logging, zoom_api_request, TIMESTAMP_FORMAT
import subprocess
from pytz import timezone
from datetime import datetime
from collections import OrderedDict
import logging

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
# Recordings that happen within BUFFER_MINUTES a courses schedule
# start time will be captured
BUFFER_MINUTES = int(env("BUFFER_MINUTES", 30))
# Ignore recordings that are less than MIN_DURATION (in minutes)
MINIMUM_DURATION = int(env("MINIMUM_DURATION", 2))


class PermanentDownloadError(Exception):
    pass


# abstraction for unit testing
def sqs_resource():
    return boto3.resource("sqs")


@setup_logging
def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
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
        if dl.oc_series_found(ignore_schedule, override_series_id):
            # process matched recording, don't discared message until after
            break
        logger.info({"no_oc_series_found": dl_data})
        # discard and keep checking messages for schedule match
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
        message = dl.send_to_deadletter_queue(e)
        download_message.delete()
        logger.error({"Error": e, "Sent to deadletter": message})
        raise

    # send a message to the opencast uploader
    message = dl.send_to_uploader_queue()
    download_message.delete()
    logger.info({"sqs_message": message})


def retrieve_message(queue):
    messages = queue.receive_messages(
        MaxNumberOfMessages=1,
        VisibilityTimeout=700
    )
    if (len(messages) == 0):
        return None

    return messages[0]


def get_admin_token():
    # get admin level zak token from admin id
    r = zoom_api_request("users/{}/token?type=zak".format(ZOOM_ADMIN_ID))
    return r.json()["token"]


class Download:

    def __init__(self, sqs, data):
        self.sqs = sqs
        self.data = data
        self.opencast_series_id = None

        logger.info({"download_message": self.data})

    @property
    def host_name(self):
        if not hasattr(self, "_host_name"):
            resp = zoom_api_request(
                    "users/{}".format(self.data["host_id"])
                   ).json()
            logger.info({"Host details": resp})
            self._host_name = "{} {}".format(
                                resp["first_name"], resp["last_name"]
                                )
        return self._host_name

    @property
    def recording_files(self):
        if not hasattr(self, "_recording_files"):
            files = self.data["recording_files"]
            start_times = sorted(
                set([file["recording_start"] for file in files])
            )
            track_numbers = {
                start_times[i]: i for i in range(len(start_times))
            }

            tracks = [{}] * len(start_times)
            for file in files:
                track_number = track_numbers[file["recording_start"]]
                tracks[track_number][file["recording_type"]] = file

            logger.info({"tracks": tracks})

            self._recording_files = []
            for track_sequence in range(len(tracks)):
                for file_data in tracks[track_sequence].values():
                    file_data["meeting_uuid"] = self.data["uuid"]
                    file_data["zoom_series_id"] = self.data["zoom_series_id"]
                    file_data["created_local"] = self._created_local
                    self._recording_files.append(
                        ZoomFile(file_data, track_sequence)
                    )
        return self._recording_files

    @property
    def _created_utc(self):
        """
        UTC time object for recording start.
        """
        utc = datetime.strptime(
            self.data["start_time"], TIMESTAMP_FORMAT) \
            .replace(tzinfo=timezone("UTC"))
        return utc

    @property
    def _created_local(self):
        """
        Local time object for recording start.
        """
        tz = timezone(LOCAL_TIME_ZONE)
        return self._created_utc.astimezone(tz)

    @property
    def _class_schedule(self):
        """
        Retrieve the course schedule from DynamoDB.
        """
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(CLASS_SCHEDULE_TABLE)

        r = table.get_item(
            Key={"zoom_series_id": str(self.data["zoom_series_id"])}
        )

        if "Item" not in r:
            return None
        else:
            schedule = r["Item"]
            return schedule

    @property
    def _series_id_from_schedule(self):
        """
        Check that the recording's start_time matches the schedule and
        extract the opencast series id.
        """

        schedule = self._class_schedule

        if not schedule:
            return None

        zoom_time = self._created_local
        logger.info({"meeting creation time": zoom_time,
                     "course schedule": schedule})
        days = OrderedDict([
            ("M", "Mondays"),
            ("T", "Tuesdays"),
            ("W", "Wednesdays"),
            ("R", "Thursdays"),
            ("F", "Fridays"),
            ("Sa", "Saturday"),
            ("Sn", "Sunday")
        ])
        day_code = list(days.keys())[zoom_time.weekday()]
        if day_code not in schedule["Days"]:
            logger.debug("No opencast recording scheduled on {}."
                         .format(days[day_code]))
            return None

        for t in schedule["Time"]:
            scheduled_time = datetime.strptime(t, "%H:%M")
            timedelta = abs(zoom_time -
                            zoom_time.replace(hour=scheduled_time.hour,
                                              minute=scheduled_time.minute)
                            ).total_seconds()
            if timedelta < (BUFFER_MINUTES * 60):
                return schedule["opencast_series_id"]

        logger.debug("Meeting started more than {} minutes before or after "
                     "opencast scheduled start time."
                     .format(BUFFER_MINUTES))

        print("reached end of function")
        return None

    def oc_series_found(self, ignore_schedule=False, override_series_id=None):

        if "duration" in self.data and int(self.data["duration"]) < MINIMUM_DURATION:
            logger.info("Recording duration shorter than {} minutes"
                        .format(MINIMUM_DURATION))
            return False

        if override_series_id:
            self.opencast_series_id = override_series_id
            logger.info("Using override series id '{}'"
                        .format(self.opencast_series_id))
            return True

        if "on_demand_series_id" in self.data:
            self.opencast_series_id = self.data["on_demand_series_id"]
            logger.info("Using on-demand provided series id '{}'"
                        .format(self.opencast_series_id))
            return True

        if ignore_schedule:
            logger.info("Ignoring schedule")
        else:
            self.opencast_series_id = self._series_id_from_schedule
            if self.opencast_series_id is not None:
                logger.info("Matched with opencast series '{}'!"
                            .format(self.opencast_series_id))
                return True

        if DEFAULT_SERIES_ID and DEFAULT_SERIES_ID != "None":
            logger.info("Using default series id {}"
                        .format(DEFAULT_SERIES_ID))
            self.opencast_series_id = DEFAULT_SERIES_ID
            return True

        logger.info("No opencast series found.")
        return False

    @property
    def upload_message(self):
        if not hasattr(self, "self._upload_message"):
            s3_filenames = {}
            for file in self.recording_files:
                if file.recording_type in s3_filenames:
                    s3_filenames[file.recording_type].append(file.s3_filename)
                else:
                    s3_filenames[file.recording_type] = [file.s3_filename]

            self._upload_message = {
                "uuid": self.data["uuid"],
                "zoom_series_id": self.data["zoom_series_id"],
                "opencast_series_id": self.opencast_series_id,
                "host_name": self.host_name,
                "topic": self.data["topic"],
                "created": datetime.strftime(
                    self._created_utc, TIMESTAMP_FORMAT
                ),
                "created_local": datetime.strftime(
                    self._created_local, TIMESTAMP_FORMAT
                ),
                "webhook_received_time": self.data["received_time"],
                "correlation_id": self.data["correlation_id"],
                "s3_filenames": s3_filenames
            }
        return self._upload_message

    def upload_to_s3(self):

        logger.info("downloading {} files".format(len(self.recording_files)))

        for file in self.recording_files:
            file.stream_file_to_s3()

    def send_to_deadletter_queue(self, error):
        deadletter_queue = self.sqs.get_queue_by_name(
                                        QueueName=DEADLETTER_QUEUE_NAME)
        message = SQSMessage(deadletter_queue, self.data)
        message.send(error=error)
        return self.data

    def send_to_uploader_queue(self):
        upload_queue = self.sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)
        message = SQSMessage(upload_queue, self.upload_message)
        message.send()
        return self.upload_message


class SQSMessage():

    def __init__(self, queue, message):
        self.queue = queue
        self.message = message

    def send(self, error=None):
        logger.info({
            "sqs_destination_queue": self.queue.url,
            "sqs_message": self.message
        })

        if error is None:
            message_attributes = {}
        else:
            message_attributes = {
                "FailedReason": {
                    "StringValue": str(error),
                    "DataType": "String"
                }}

        try:

            if "fifo" in self.queue.url:
                message_sent = self.queue.send_message(
                    MessageBody=json.dumps(self.message),
                    MessageGroupId=self.message["uuid"],
                    MessageDeduplicationId=self.message["uuid"],
                    MessageAttributes=message_attributes
                )
            else:
                message_sent = self.queue.send_message(
                    MessageBody=json.dumps(self.message),
                    MessageAttributes=message_attributes
                )
        except Exception as e:
            logger.exception("Error when sending SQS message to queue {}:{}"
                             .format(self.queue.url, e))
            raise

        logger.debug({"Queue": self.queue.url,
                      "Message sent": message_sent,
                      "FailedReason": error})


class ZoomFile:

    def __init__(self, file_data, track_sequence):
        self.file_data = file_data
        self._track_sequence = track_sequence
        self.recording_type = self.__standardized_recording_type(
            file_data["recording_type"]
        )

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
        return "unrecognized_type_{}".format(name)

    @property
    def zoom_filename(self):
        if not hasattr(self, "_zoom_filename"):
            # First request is for retrieving the filename
            url = "{}?zak={}".format(
                self.file_data["download_url"], ADMIN_TOKEN
            )
            r = requests.get(url, allow_redirects=False)
            r.raise_for_status

            # If the request is not authorized, Zoom will return 200
            # and an HTML error page
            if "Content-Type" in r.headers and r.headers["Content-Type"] == "text/html":
                error_message = "Request for download stream not authorized.\n"
                if "Error" in str(r.content):
                    raise PermanentDownloadError(
                        "{} Zoom returned an HTML error page."
                        .format(error_message)
                    )
                else:
                    raise PermanentDownloadError(
                        "{} Zoom returned stream with content type text/html."
                        .format(error_message)
                    )

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
                    "request: {} response headers: {}".format(url, r.headers)
                )
            self._zoom_filename = zoom_name.lower()
            logger.info("got filename {}".format(zoom_name))

        return self._zoom_filename

    @property
    def s3_filename(self):
        if not hasattr(self, "_track_sequence"):
            return None
        if not hasattr(self, "_s3_filename"):
            ts = int(self.file_data["created_local"].timestamp())
            self._s3_filename = "{}/{}/{:03d}-{}.{}".format(
                                    self.file_data["zoom_series_id"],
                                    ts,
                                    self._track_sequence,
                                    self.recording_type,
                                    self.file_extension
                                )
        return self._s3_filename

    def valid_mp4_file(self):
        # TODO: if file not found, add appropriate error
        s3 = boto3.client("s3")
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": ZOOM_VIDEOS_BUCKET, "Key": self.s3_filename}
        )

        command = ["/var/task/ffprobe", "-of", "json", url]
        if subprocess.call(command) == 1:
            logger.warning("Corrupt MP4, need to retry download "
                           "from zoom to S3. {}".format(url))
            return False

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
        return self.zoom_filename.split(".")[-1]

    def stream_file_to_s3(self):
        s3 = boto3.client("s3")

        metadata = {
            "uuid": self.file_data["meeting_uuid"],
            "file_id": self.file_data["recording_id"],
            "file_type": self.file_extension
        }

        logger.info(
            {"uploading file to S3": self.s3_filename,
             "metadata": metadata}
        )
        part_info = {"Parts": []}
        mpu = s3.create_multipart_upload(
                    Bucket=ZOOM_VIDEOS_BUCKET,
                    Key=self.s3_filename,
                    Metadata=metadata)

        try:
            chunks = enumerate(
                        self.stream.iter_content(chunk_size=MIN_CHUNK_SIZE), 1
                     )
            for part_number, chunk in chunks:
                part = s3.upload_part(Body=chunk,
                                      Bucket=ZOOM_VIDEOS_BUCKET,
                                      Key=self.s3_filename,
                                      PartNumber=part_number,
                                      UploadId=mpu["UploadId"])

                part_info["Parts"].append({
                    "PartNumber": part_number,
                    "ETag": part["ETag"]
                })

            s3.complete_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET,
                                         Key=self.s3_filename,
                                         UploadId=mpu["UploadId"],
                                         MultipartUpload=part_info)
            print("Completed multipart upload of {}.".format(self.s3_filename))
        except Exception as e:
            logger.exception(
                "Something went wrong with upload of {}:{}"
                .format(self.s3_filename, e)
            )
            s3.abort_multipart_upload(Bucket=ZOOM_VIDEOS_BUCKET,
                                      Key=self.s3_filename,
                                      UploadId=mpu["UploadId"])

        if self.file_extension == "mp4":
            if not self.valid_mp4_file:
                self.stream.close()
                raise Exception("MP4 failed to transfer.")

        self.stream.close()
