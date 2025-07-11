import sys
import json
import boto3
import requests
from requests.auth import HTTPDigestAuth
from urllib.parse import urljoin
from os import getenv as env
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape
from datetime import datetime
from hashlib import md5
from uuid import UUID, uuid4
from utils import (
    setup_logging,
    TIMESTAMP_FORMAT,
    PipelineStatus,
    set_pipeline_status,
    get_recording_segments,
)


import logging

logger = logging.getLogger()

UPLOAD_QUEUE_NAME = env("UPLOAD_QUEUE_NAME")
OPENCAST_BASE_URL = env("OPENCAST_BASE_URL")
OPENCAST_API_USER = env("OPENCAST_API_USER")
OPENCAST_API_PASSWORD = env("OPENCAST_API_PASSWORD")
ZOOM_VIDEOS_BUCKET = env("ZOOM_VIDEOS_BUCKET")
DEFAULT_OC_WORKFLOW = env("DEFAULT_OC_WORKFLOW")
DEFAULT_PUBLISHER = env("DEFAULT_PUBLISHER")
OVERRIDE_PUBLISHER = env("OVERRIDE_PUBLISHER")
OVERRIDE_CONTRIBUTOR = env("OVERRIDE_CONTRIBUTOR")
OC_TRACK_UPLOAD_MAX = int(env("OC_TRACK_UPLOAD_MAX", 5))
# Ignore recordings that are less than MIN_DURATION (in minutes)
MINIMUM_DURATION = int(env("MINIMUM_DURATION", 2))

s3 = boto3.resource("s3")
aws_lambda = boto3.client("lambda")
sqs = boto3.resource("sqs")

UPLOAD_OP_TYPES = ["track", "uri-track"]
SESSION = None


class OpencastConnectionError(Exception):
    pass


class InvalidOpencastSeriesId(Exception):
    pass


def oc_api_request(method, endpoint, **kwargs):
    url = urljoin(OPENCAST_BASE_URL, endpoint)
    logger.info({"url": url, "kwargs": kwargs})

    try:
        resp = SESSION.request(method, url, **kwargs)
    except requests.RequestException:
        raise OpencastConnectionError
    resp.raise_for_status()
    return resp


@setup_logging
def handler(event, context):
    upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

    messages = upload_queue.receive_messages(
        MaxNumberOfMessages=1,
        VisibilityTimeout=900,
    )
    if not messages:
        logger.warning("No upload queue messages available.")
        return
    else:
        logger.info(f"{len(messages)} upload messages in queue")

    global SESSION
    SESSION = requests.Session()
    SESSION.auth = HTTPDigestAuth(OPENCAST_API_USER, OPENCAST_API_PASSWORD)
    SESSION.headers.update(
        {
            "X-REQUESTED-AUTH": "Digest",
            "X-Opencast-Matterhorn-Authentication": "true",
        }
    )

    upload_message = messages[0]
    logger.info(
        {
            "queue_message": {
                "body": upload_message.body,
            },
        }
    )

    upload_data = None
    final_status = None
    reason = None
    try:
        upload_data = json.loads(upload_message.body)
        logger.debug({"processing": upload_data})
        set_pipeline_status(
            upload_data["zip_id"],
            PipelineStatus.UPLOADER_RECEIVED,
        )

        wf_id = process_upload(upload_data)
        upload_message.delete()
        if wf_id:
            logger.info(f"Workflow id {wf_id} initiated.")
            # only ingest one per invocation
            final_status = PipelineStatus.SENT_TO_OPENCAST
        else:
            final_status = PipelineStatus.IGNORED
            logger.info("No workflow initiated.")

    except Exception as e:
        logger.exception(e)
        final_status = PipelineStatus.UPLOADER_FAILED
        if sys.exc_info()[0] == OpencastConnectionError:
            reason = "Unable to reach Opencast."
        elif sys.exc_info()[0] == InvalidOpencastSeriesId:
            reason = "Invalid Opencast series id."
        raise
    finally:
        if upload_data and "zip_id" in upload_data and final_status:
            set_pipeline_status(
                upload_data["zip_id"],
                final_status,
                reason=reason,
            )
        SESSION.close()


def minutes_in_pipeline(webhook_received_time):
    start_time = datetime.strptime(webhook_received_time, TIMESTAMP_FORMAT)
    ingest_time = datetime.utcnow()
    duration = ingest_time - start_time
    return duration.total_seconds() // 60


def process_upload(upload_data):
    upload = Upload(upload_data)
    wf_id = upload.upload()
    return wf_id


class Upload:
    def __init__(self, data):
        self.data = data
        self.recording_times = ""

    @property
    def created(self):
        return self.data["created"]

    @property
    def meeting_uuid(self):
        return self.data["uuid"]

    @property
    def mediapackage_id(self):
        if not hasattr(self, "_opencast_mpid"):
            # first uuid generated should be deterministic
            # regardless of the allow_multiple_ingests flag
            mpid = str(UUID(md5(self.meeting_uuid.encode()).hexdigest()))
            if self.already_ingested(mpid):
                if self.data["allow_multiple_ingests"]:
                    # random uuid
                    mpid = str(uuid4())
                    logger.info(f"Created random mediapackage id {mpid}")
                else:
                    logger.warning(
                        "Episode with deterministic mediapackage id"
                        f" {mpid} already ingested"
                    )
                    set_pipeline_status(
                        self.data["zip_id"],
                        PipelineStatus.IGNORED,
                        reason="Already in opencast",
                    )
                    mpid = None
            else:
                logger.info(
                    f"Created mediapackage id {mpid} "
                    f"from uuid {self.meeting_uuid}"
                )

            self._opencast_mpid = mpid
        return self._opencast_mpid

    @property
    def zoom_series_id(self):
        return self.data["zoom_series_id"]

    @property
    def created_local(self):
        return self.data["created_local"]

    @property
    def override_series_id(self):
        return self.data.get("override_series_id")

    @property
    def opencast_series_id(self):
        return self.data["opencast_series_id"]

    @property
    def type_num(self):
        if self.data["oc_title"] == "Lecture":
            return "L"
        elif self.data["oc_title"] == "Section":
            return "S"
        else:
            return "P"

    @property
    def publisher(self):
        series_data = {
            k: v[0]["value"]
            for k, v in json.loads(self.series_catalog)[
                "http://purl.org/dc/terms/"
            ].items()
        }

        if OVERRIDE_PUBLISHER and OVERRIDE_PUBLISHER != "None":
            return OVERRIDE_PUBLISHER
        elif "publisher" in series_data:
            return series_data["publisher"]
        elif DEFAULT_PUBLISHER:
            return DEFAULT_PUBLISHER

    @property
    def workflow_definition_id(self):
        if "oc_workflow" in self.data:
            return self.data["oc_workflow"]
        else:
            return DEFAULT_OC_WORKFLOW

    @property
    def s3_filenames(self):
        """
        Pulls out the s3 filenames grouped by view type.

        This is also the point where we need to filter out any
        segments that are less than our MINIMUM_DURATION value. This is
        tricky because the data is grouped by view instead of segment.

        Basically we want to correctly handle a situation like this:

            speaker view:
              - foo/bar/000-speaker_view.mp4 (1m)
              - foo/bar/001-speaker_view.mp4 (5m)
              - foo/bar/002-speaker_view.mp4 (2h)

            shared screen:
              - foo/bar/001-shared_screen.mp4 (5m)
              - foo/bar/002-shared_screen.mp4 (2h)

        Here we have 3 segments of length 1m, 5m and 2h. However, the first
        segment is only present in the "speaker" view. So we can't simply throw
        away the first file from each view; we have to make sure it's from the
        "000" segment.

        ZIP-74:
        We also keep recording start/end times for all segments.
        In the chat file, times are relative to the event start and OC will need
        start/end times of each segment to add chat messages to the right point into
        the video. These times will be passed as a workflow configuration:
        zoomRecordingTimes.

        :return: [description]
        :rtype: [type]
        """

        if not hasattr(self, "_s3_filenames"):
            self._s3_filenames = {}
            segment_0_discarded = False
            chat_has_segment_0 = False
            # Key = seg number, value = "recordingStart_recordingEnd"
            rec_times = {}

            # the s3_files dict is keyed on view type, e.g. gallery, speaker

            # In this loop we're going to check the first file (segment 0) of
            # each view. If it's < our MINIMUM_DURATION value
            for view, file_info in self.data["s3_files"].items():
                segment_files = file_info["segments"]
                self._s3_filenames[view] = [
                    file["filename"] for file in segment_files
                ]

                # ZIP-74 Remember recording start/end times for each segment.
                # We need to know the time each segment to be ingested starts and send
                # that to OC so that the chat messages are added at the right time in
                # the video.
                for seg in segment_files:
                    if seg["segment_num"] not in rec_times:
                        rec_times[seg["segment_num"]] = {
                            "start": seg["recording_start"],
                            "stop": seg["recording_end"],
                        }
                # ZIP-74: Chat file always have ffprobe_seconds 0 so don't
                # check their duration. But we need to know if there's a chat
                # segment 0 in case it needs to be discarded later.
                if view == "chat_file":
                    chat_has_segment_0 = segment_files[0]["segment_num"] == 0
                    continue

                # check the duration of the first file in this view
                too_short = segment_files[0]["ffprobe_seconds"] < (
                    MINIMUM_DURATION * 60
                )

                # check if this is part of the first segment
                is_first_segment = segment_files[0]["segment_num"] == 0

                if too_short and is_first_segment:
                    # Discard file in segment 0
                    self._s3_filenames[view] = self._s3_filenames[view][1:]
                    segment_0_discarded = True

            # ZIP-74 Remove segment 0 start/end times and chat seg 0 if there
            if segment_0_discarded:
                rec_times.pop(0, None)
                if chat_has_segment_0:
                    self._s3_filenames["chat_file"] = self._s3_filenames[
                        "chat_file"
                    ][1:]

            # ZIP-74 "start1_end1,start2_end2,start3_end3"
            # ZIP-91 Start/stop times need to include pause/resume events
            self.recording_times = get_recording_segments(
                zoom_uuid=self.data["uuid"],
                start_stop_segments=rec_times,
            )
            logger.info(
                f"Recording times for zoom uuid {self.data['uuid']}: "
                f"{self.recording_times}"
            )

        return self._s3_filenames

    @property
    def workflow_id(self):
        if not hasattr(self, "_workflow_id"):
            if not hasattr(self, "workflow_xml"):
                logger.warning("No workflow xml yet!")
                return None
            root = ET.fromstring(self.workflow_xml)
            self._workflow_id = root.attrib["id"]
        return self._workflow_id

    def upload(self):
        if not self.opencast_series_id:
            raise Exception("No opencast series id found!")
        if not self.mediapackage_id:
            return None
        self.get_series_catalog()
        self.ingest()

        wf_id = self.workflow_id
        logger.info(
            {
                "workflow_id": wf_id,
                "mediapackage_id": self.mediapackage_id,
                "minutes_in_pipeline": minutes_in_pipeline(
                    self.data["webhook_received_time"]
                ),
                "recording_data": self.data,
            }
        )
        return wf_id

    def already_ingested(self, mpid):
        # OC11, use the external api endpoint
        endpoint = f"/api/events/{mpid}"
        try:
            resp = oc_api_request("GET", endpoint)
            logger.debug(
                f"Lookup for mpid: {mpid}, enpoint: {endpoint}, {resp.json()}"
            )
            return resp.json()["identifier"] == mpid
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == "404":
                return False

    def get_series_catalog(self):
        logger.info(
            f"Getting series catalog for series: {self.opencast_series_id}"
        )

        endpoint = f"/series/{self.opencast_series_id}.json"

        try:
            resp = oc_api_request("GET", endpoint)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise InvalidOpencastSeriesId
            raise OpencastConnectionError

        logger.debug({"series_catalog": resp.text})

        self.series_catalog = resp.text

    def ingest(self):
        logger.info("Adding mediapackage and ingesting.")

        endpoint = f"/ingest/addMediaPackage/{self.workflow_definition_id}"

        params = [
            ("identifier", (None, self.mediapackage_id)),
            ("title", (None, self.data["oc_title"])),
            ("type", (None, self.type_num)),
            ("isPartOf", (None, self.opencast_series_id)),
            # fmt: off
            ("license", (None, "Creative Commons 3.0: Attribution-NonCommercial-NoDerivs",),),
            # fmt: on
            ("publisher", (None, escape(self.publisher))),
            ("created", (None, self.created)),
            ("language", (None, "en")),
            ("seriesDCCatalog", (None, self.series_catalog)),
            ("source", (None, escape(f"ZIP-{self.meeting_uuid}"))),
            ("spatial", (None, f"Zoom {self.zoom_series_id}")),
            ("zoomMeetingStart", (None, self.created)),
        ]

        if self.data.get("ingest_all_mp4"):
            fpg = ArchiveFileParamGenerator(self.s3_filenames)
        else:
            fpg = PublishFileParamGenerator(self.s3_filenames)
        try:
            file_params = fpg.generate()
        except Exception as e:
            logger.exception(f"Failed to generate file upload params: {e}")
            raise

        # ZIP-74: this is ingested as an Opencast workflow configuration.
        # Added here because it's calculated by self.s3_filenames.
        params.append(
            ("zoomRecordingTimes", (None, self.recording_times)),
        )
        params.extend(file_params)
        resp = oc_api_request("POST", endpoint, files=params)
        logger.debug({"addMediaPackage": resp.text})
        self.workflow_xml = resp.text


class FileParamGenerator(object):
    def __init__(self, s3_filenames):
        self.s3_filenames = s3_filenames
        self._params = []

    def _add_view(self, flavor, view, is_video=True):
        for s3_file in self.s3_filenames[view]:
            uri_param = "mediaUri" if is_video else "attachmentUri"
            logger.info(
                {
                    "adding": {
                        "s3_file": s3_file,
                        "view": view,
                        "flavor": flavor,
                    }
                }
            )
            self._params.extend(
                [
                    ("flavor", (None, escape(flavor))),
                    (
                        uri_param,
                        (None, self._generate_presigned_url(s3_file)),
                    ),
                ]
            )

    def _generate_presigned_url(self, s3_filename):
        logger.info(
            f"Generate presigned url bucket {ZOOM_VIDEOS_BUCKET} "
            f"key {s3_filename}"
        )
        url = s3.meta.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": ZOOM_VIDEOS_BUCKET, "Key": s3_filename},
        )
        logger.info(f"Got presigned url {url}")
        return url

    def generate(self) -> list:
        pass


class ArchiveFileParamGenerator(FileParamGenerator):
    FLAVORS = {
        "active_speaker": "speaker/chunked+source",
        "shared_screen": "shared-screen/chunked+source",
        "gallery_view": "gallery/chunked+source",
        "shared_screen_with_gallery_view": "shared-screen-gallery/chunked+source",
        "shared_screen_with_speaker_view": "shared-screen-speaker/chunked+source",
    }

    def __init__(self, s3_filenames):
        super().__init__(s3_filenames)

    def generate(self):
        for view, flavor in self.FLAVORS.items():
            if view in self.s3_filenames:
                super()._add_view(flavor, view)

        return self._params


class PublishFileParamGenerator(FileParamGenerator):
    FLAVORS = {
        # The workflow requires at least one of the files have this flavor
        # It will be treated as the "primary" stream and cannot be dropped by
        # the producer when choosing the views at the workflow edit/trim stage
        "presenter": "multipart/chunked+source",
        # if 2 or more streams there must always be a "presentation" flavor,
        # even if it's the "gallery_view"
        "presentation": "presentation/chunked+source",
        # this will only ever be used for the pure "gallery_view" and only if
        # it doesn't have to serve as the presentation flavor
        "other": "other/chunked+source",
        # this will contain the chat file
        "chat": "chat/chunked+source",
    }

    MEDIA_FLAVORS = ["presenter", "presentation", "other"]

    VIEW_PRIORITIES = {
        # if we have this...
        "active_speaker": [
            # then take these in this order...
            "shared_screen",
            "gallery_view",
            "shared_screen_with_gallery_view",
            "shared_screen_with_speaker_view",
        ],
        "shared_screen_with_speaker_view": [
            "shared_screen",
            "shared_screen_with_gallery_view",
            "gallery_view",
        ],
    }

    # if we don't have either of the above then there can only be three
    # possible views left and we just try them all in this order
    FALLBACK_PRIORITIES = [
        "shared_screen",
        "shared_screen_with_gallery_view",
        "gallery_view",
    ]

    def __init__(self, s3_filenames):
        super().__init__(s3_filenames)
        self._used_views = set()
        self._used_media_views = set()

    @property
    def flavors(self):
        return [x[1][1] for x in self._params if x[0] == "flavor"]

    def _has_view(self, view):
        if view in self.s3_filenames:
            # Don't look at number of segments for chat files
            if "chat_file" == view:
                return True
            # OPC-1071
            if len(self.s3_filenames[view]) > 0:
                return True

        return False

    def _has_presenter(self):
        return any("multipart" in f for f in self.flavors)

    def _has_presentation(self):
        return any("presentation" in f for f in self.flavors)

    def _add_presenter(self, view):
        flavor = self.FLAVORS["presenter"]
        self._add_view(flavor, view)

    def _add_secondary(self, view):
        if not self._has_presentation():
            flavor = self.FLAVORS["presentation"]
        else:
            flavor = self.FLAVORS["other"]
        self._add_view(flavor, view)

    def _add_view(self, flavor, view, is_video=True):
        """
        params must be added in pairs
        each "view" consists of a flavor (file type) param and
        a mediaUri/attachmentUri param the s3 url of the file
        :param flavor:
        :param view:
        :return:
        """
        logger.info(f"Selected {view} as {flavor}")
        if view in self._used_views:
            # we already got this view
            return

        if is_video and len(self._used_media_views) >= len(self.MEDIA_FLAVORS):
            # we already got all the flavors
            return

        super()._add_view(flavor, view, is_video)

        self._used_views.add(view)
        if is_video:
            self._used_media_views.add(view)

    def _generate_presigned_url(self, s3_filename):
        return super()._generate_presigned_url(s3_filename)

    def generate(self):
        for primary_view, secondary_views in self.VIEW_PRIORITIES.items():
            if not self._has_view(primary_view) or self._has_presenter():
                continue
            self._add_presenter(primary_view)

            for view in secondary_views:
                if not self._has_view(view):
                    continue
                self._add_secondary(view)

        for view in self.FALLBACK_PRIORITIES:
            if not self._has_view(view):
                continue
            if not self._has_presenter():
                self._add_presenter(view)
            else:
                self._add_secondary(view)

        if not self._has_presenter():
            raise RuntimeError("Unable to find a presenter view")

        if not self._has_presentation():
            logger.info("Unable to find a secondary view")

        # ZIP-74 Add chat view if there
        if "chat_file" in self.s3_filenames:
            self._add_view(self.FLAVORS["chat"], "chat_file", is_video=False)

        return self._params
