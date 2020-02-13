import boto3
import json
import requests
from os import getenv as env
from hashlib import md5
from operator import itemgetter
from common import setup_logging, ZoomAPIRequests
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
ZOOM_ADMIN_EMAIL = env("ZOOM_ADMIN_EMAIL")
DEFAULT_SERIES_ID = env("DEFAULT_SERIES_ID")
CLASS_SCHEDULE_TABLE = env("CLASS_SCHEDULE_TABLE")
LOCAL_TIME_ZONE = env("LOCAL_TIME_ZONE")
DOWNLOAD_MESSAGES_PER_INVOCATION = env('DOWNLOAD_MESSAGES_PER_INVOCATION')
# Recordings that happen within BUFFER_MINUTES a courses schedule
# start time will be captured
BUFFER_MINUTES = 30
# Ignore recordings that are less than MIN_DURATION (in minutes)
MINIMUM_DURATION = 2

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

ZOOM_API = ZoomAPIRequests(env("ZOOM_API_KEY"), env("ZOOM_API_SECRET"))

recording_types = [
    "shared_screen_with_speaker_view",
    "shared_screen_with_gallery_view",
    "shared_screen",
    "active_speaker",
    "audio_only",
    "audio_transcript"
]


class PermanentDownloadError(Exception):
    pass


@setup_logging
def handler(event, context):
    """
    This function receives an event on each new entry in the download urls
    DyanmoDB table
    """

    ignore_schedule = event.get("ignore_schedule", False)
    override_series_id = event.get("override_series_id")
    sqs = boto3.resource("sqs")

    # try DOWNLOAD_MESSAGES_PER_INVOCATION number of times to retrieve
    # a recoreding that matches the class schedule
    download_message = None
    for _ in range(int(DOWNLOAD_MESSAGES_PER_INVOCATION)):
        download_message = Download(
            sqs, ignore_schedule, override_series_id
        )
        if not download_message.body:
            logger.info("No messages available in downloads queue.")
            return
        if download_message.opencast_series_id:
            break
        # discard and keep checking messges for schedule match
        download_message.delete()
        download_message = None

    if not download_message:
        logger.info("No available recordings match the class schedule.")
        return

    global ADMIN_TOKEN
    ADMIN_TOKEN = get_admin_token()

    try:
        # upload matched recording to S3 and verify MP4 integrity
        download_message.upload_to_s3()
    except PermanentDownloadError as e:
        # push message to deadletter queue, add error reason to message
        message = download_message.send_to_deadletter_queue()
        logger.error({"Error": e, "Sent to deadletter": message})
        raise

    # send a message to the opencast uploader
    message = download_message.send_to_uploader_queue()
    logger.info({"Sent to uploader": message})


def get_admin_token(self):
    # get admin user id from admin email
    r = ZOOM_API.get("users/{}".format(ZOOM_ADMIN_EMAIL))
    admin_id = r.json()["id"]

    # get admin level zak token from admin id
    r = ZOOM_API.get("users/{}/token?type=zak".format(admin_id))
    return r.json()["token"]


class Download:

    def __init__(self, sqs, ignore_schedule, override_series_id):
        self.ignore_schedule = ignore_schedule
        self.override_series_id = override_series_id
        self.sqs = sqs
        self._message = self.__retrieve_message()

        if self._message:
            self.body = json.loads(self._message.body)
            logger.info({"Retrieved download message": self.body})

            self._uuid = self.body["uuid"]
            self._zoom_series_id = self.body["zoom_series_id"]
            self._host = self.body["host_name"]
            self._topic = self.body["topic"]
            self._duration = self.body["duration"]
            self._received_time = self.body["received_time"]
            self._correlation_id = self.body["correlation_id"]
            self._recording_files = ZoomRecordingFiles(
                                        self._uuid,
                                        self.body["recording_files"])

    def __retrieve_message(self):
        download_queue = self.sqs.get_queue_by_name(
                                    QueueName=DOWNLOAD_QUEUE_NAME)
        messages = download_queue.receive_messages(
            MaxNumberOfMessages=1,
            VisibilityTimeout=700
        )
        if (len(messages) == 0):
            self.body = None
            return None

        return messages[0]

    @property
    def _created_utc(self):
        """
        UTC time object for recording start.
        """
        utc = datetime.strptime(
            self.body["start_time"], TIMESTAMP_FORMAT) \
            .replace(tzinfo=timezone("UTC"))
        return utc

    @property
    def _created_local(self):
        """
        Local time object for recording start.
        """
        return self._created_utc.astimezone(timezone(LOCAL_TIME_ZONE))

    @property
    def _class_schedule(self):
        """
        Retrieve the course schedule from DynamoDB.
        """
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(CLASS_SCHEDULE_TABLE)

        r = table.get_item(
            Key={"zoom_series_id": str(self._zoom_series_id)}
        )

        if "Item" not in r:
            return None
        else:
            schedule = r["Item"]
            logger.info(schedule)
            return schedule

    @property
    def _series_id_from_schedule(self):
        """
        Check that the recording's start_time matches the schedule and
        extract the opencast series id.
        """

        schedule = self._class_schedule
        print("got class schedule {}".format(schedule))

        if not schedule:
            return None

        zoom_time = self._created_local
        print(zoom_time.strftime(TIMESTAMP_FORMAT))
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

    @property
    def opencast_series_id(self):

        if not hasattr(self, "_oc_series_id"):

            self._oc_series_id = None

            if self.override_series_id:
                self._oc_series_id = self.override_series_id
                logger.info("Using override series id '{}'"
                            .format(self._oc_series_id))
                return self._oc_series_id

            if self.ignore_schedule:
                logger.info("Ignoring schedule")
            else:
                self._oc_series_id = self._series_id_from_schedule
                print("series_id_from_schedule returned {}"
                      .format(self._oc_series_id))
                if self._oc_series_id is not None:
                    logger.info("Matched with opencast series '{}'!"
                                .format(self._oc_series_id))
                    return self._oc_series_id

            if DEFAULT_SERIES_ID is not None and DEFAULT_SERIES_ID != "None":
                logger.info("Using default series id {}"
                            .format(DEFAULT_SERIES_ID))
                self._oc_series_id = DEFAULT_SERIES_ID

        return self._oc_series_id

    @property
    def upload_message(self):
        if not hasattr(self, "self._upload_message"):
            self._upload_message = {
                "uuid": self._uuid,
                "zoom_series_id": self._zoom_series_id,
                "opencast_series_id": self.opencast_series_id,
                "host_name": self._host,
                "topic": self._topic,
                "created": datetime.strftime(
                    self._created_utc, TIMESTAMP_FORMAT
                ),
                "created_local": datetime.strftime(
                    self._created_local, TIMESTAMP_FORMAT
                ),
                "webhook_received_time": self._received_time,
                "correlation_id": self._correlation_id
            }
        return self._upload_message

    def upload_to_s3(self):

        if self._duration and self._duration < MINIMUM_DURATION:
            logger.info("Recording duration shorter than {} minutes"
                        .format(MINIMUM_DURATION))
            return None

        if not self.opencast_series_id:
            logger.info("No opencast series match found")
            return None

        logger.info("downloading {} files".format(self._recording_files.count))

        self._recording_files.stream_files_to_s3()

        return self.upload_message

    def send_to_deadletter_queue(self, error):
        deadletter_queue = self.sqs.get_queue_by_name(
                                        QueueName=DEADLETTER_QUEUE_NAME)
        message = SQSMessage(deadletter_queue, self.body)
        message.send(error=error)
        self.delete()
        return self.body

    def send_to_uploader_queue(self):
        upload_queue = self.sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)
        message = SQSMessage(upload_queue, self.upload_message)
        message.send()
        self.delete()
        return self.upload_message

    def delete(self):
        """
        Delete the message from the queue.
        """
        self._message.delete()


class SQSMessage():

    def __init__(self, queue, message):
        self.queue = queue
        self.message = message

    def send(self, error=None):
        logger.info({
            "Sending SQS message to ": self.queue.url,
            "message": self.message
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


class ZoomRecordingFiles:

    def __init__(self, meeting_uuid, recording_list):
        self._files = []
        for file in self.__filter_and_sort(recording_list):
            self._files.append(ZoomFile(meeting_uuid, file))

    @property
    def count(self):
        return len(self._files)

    def __filter_and_sort(self, recording_list):
        """
        Sort files by recording start time and filter out multiple MP4 files
        that occur during the same time segment. Choose which MP4
        recording_type to keep based on priority_list.
        """
        if not recording_list:
            return None

        non_mp4_files = [file_data for file_data in recording_list
                         if file_data["file_type"].lower() != "mp4"]

        mp4_files = []

        priority_list = [
            "shared_screen_with_speaker_view",
            "shared_screen",
            "active_speaker"]
        start_times = set([file_data["recording_start"]
                           for file_data in recording_list])
        for start_time in start_times:
            recordings = {file_data["recording_type"]: file_data
                          for file_data in recording_list
                          if file_data["file_type"].lower() == "mp4"
                          and file_data["recording_start"] == start_time}

            added_mp4 = False
            for mp4_type in priority_list:
                if mp4_type in recordings:
                    logger.debug("Selected MP4 recording type '{}' "
                                 "for start time {}."
                                 .format(mp4_type, start_time))
                    added_mp4 = True
                    mp4_files.append(recordings[mp4_type])
                    break

            # make sure at least one mp4 is added, in case the zoom api changes
            if not added_mp4 and recordings:
                logger.warning("No MP4 found with the 'recording_type'"
                               "shared_screen_with_speaker_view, "
                               "shared_screen, or"
                               "active_speaker.")
                mp4_files.append(recordings.values()[0])

        sorted_files = sorted(mp4_files + non_mp4_files,
                              key=itemgetter("recording_start"))

        return sorted_files

    def __next_track_sequence(self, prev_file, file):

        if not prev_file:
            return False

        logger.debug({"start": file.start,
                      "end": file.end})

        if file.start == prev_file.start:
            logger.debug("start matches")
            if file.end == prev_file.end:
                logger.debug("end matches")
                return False

        if file.start >= prev_file.end:
            logger.debug("start >= end")
            return True

        raise PermanentDownloadError("Recording segments overlap.")

    def stream_files_to_s3(self):
        track_sequence = 1
        prev_file = None

        for file in self._files:
            if self.__next_track_sequence(prev_file, file):
                track_sequence += 1

            logger.debug("file {} is track sequence {}"
                         .format(file, track_sequence))
            prev_file = file

            file.stream_file_to_s3(track_sequence)


class ZoomFile:

    def __init__(self, meeting_uuid, file_data):
        # TODO each file should be assigned a track sequence
        self._meeting_uuid = meeting_uuid
        self._file_id = file_data["recording_id"]
        self._download_url = file_data["download_url"]
        self.zoom_file_type = file_data["file_type"].lower()
        self.start = file_data["recording_start"]
        self.end = file_data["recording_end"]
        self.recording_type = file_data["recording_type"]

    @property
    def zoom_filename(self):
        if not hasattr(self, "_zoom_filename"):
            # First request is for retrieving the filename
            url = "{}?zak={}".format(self._download_url, ADMIN_TOKEN)
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
            md5_meeting_id = md5(self._meeting_uuid.encode()).hexdigest()
            self._s3_filename = "{}/{:03d}-{}.{}".format(
                                    md5_meeting_id,
                                    self._track_sequence,
                                    self._file_id,
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
            logger.info("requesting {}".format(self._download_url))
            url = "{}?zak={}".format(self._download_url, ADMIN_TOKEN)
            r = requests.get(url, stream=True)
            r.raise_for_status

            self._stream = r

        return self._stream

    @property
    def file_extension(self):
        return self.zoom_filename.split(".")[-1]

    def stream_file_to_s3(self, track_sequence):
        s3 = boto3.client("s3")
        self._track_sequence = track_sequence

        metadata = {
            "uuid": self._meeting_uuid,
            "view": self.recording_type,
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
