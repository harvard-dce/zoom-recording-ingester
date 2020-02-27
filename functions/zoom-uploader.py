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
from uuid import UUID
from common import TIMESTAMP_FORMAT


import logging
from common import setup_logging
logger = logging.getLogger()

UPLOAD_QUEUE_NAME = env("UPLOAD_QUEUE_NAME")
OPENCAST_BASE_URL = env("OPENCAST_BASE_URL")
OPENCAST_API_USER = env("OPENCAST_API_USER")
OPENCAST_API_PASSWORD = env("OPENCAST_API_PASSWORD")
ZOOM_VIDEOS_BUCKET = env("ZOOM_VIDEOS_BUCKET")
ZOOM_RECORDING_TYPE_NUM = "L01"
ZOOM_OPENCAST_WORKFLOW = env("OC_WORKFLOW")
ZOOM_OPENCAST_FLAVOR = env("OC_FLAVOR")
DEFAULT_PUBLISHER = env("DEFAULT_PUBLISHER")
OVERRIDE_PUBLISHER = env("OVERRIDE_PUBLISHER")
OVERRIDE_CONTRIBUTOR = env("OVERRIDE_CONTRIBUTOR")

s3 = boto3.resource("s3")

session = requests.Session()
session.auth = HTTPDigestAuth(OPENCAST_API_USER, OPENCAST_API_PASSWORD)
session.headers.update({
    "X-REQUESTED-AUTH": "Digest",
    "X-Opencast-Matterhorn-Authentication": "true",

})

MP4_VIEW_PRIORITY_LIST = [
    "active_speaker",
    "shared_screen_with_speaker_view",
    "shared_screen",
    "shared_screen_with_gallery_view",
    "gallery_view"
]


def oc_api_request(method, endpoint, **kwargs):
    url = urljoin(OPENCAST_BASE_URL, endpoint)
    logger.info({"url": url, "kwargs": kwargs})
    try:
        resp = session.request(method, url, **kwargs)
    except requests.RequestException:
        raise
    resp.raise_for_status()
    return resp


# boto resource/client setup must be wrapped for unit testing
def sqs_resource():
    return boto3.resource("sqs")


@setup_logging
def handler(event, context):

    sqs = sqs_resource()
    upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

    messages = upload_queue.receive_messages(
        MaxNumberOfMessages=1,
        VisibilityTimeout=300
    )
    if len(messages) == 0:
        logger.warning("No upload queue messages available")
        return

    upload_message = messages[0]
    logger.debug({
        "queue_message": {
            "attributes": upload_message.attributes,
            "body": upload_message.body
        }
    })

    try:
        upload_data = json.loads(upload_message.body)
        logger.info(upload_data)
        wf_id = process_upload(upload_data)
        upload_message.delete()
        if wf_id:
            logger.info("Workflow id {} initiated".format(wf_id))
            # only ingest one per invocation
        else:
            logger.info("No workflow initiated.")
    except Exception as e:
        logger.exception(e)
        raise


def process_upload(upload_data):
    upload = Upload(upload_data)
    upload.upload()
    logger.info({"minutes in pipeline": upload.minutes_in_pipeline})
    return upload.workflow_id


class Upload:

    def __init__(self, data):
        self.data = data

    @property
    def creator(self):
        return self.data["host_name"]

    @property
    def created(self):
        return self.data["created"]

    @property
    def meeting_uuid(self):
        return self.data["uuid"]

    @property
    def mediapackage_id(self):
        if not hasattr(self, "_opencast_mpid"):
            mpid = str(UUID(md5(self.meeting_uuid.encode()).hexdigest()))
            logger.debug("Created mediapackage id {} from uuid {}"
                         .format(mpid, self.meeting_uuid))
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
    def minutes_in_pipeline(self):
        webhook_received_time = datetime.strptime(
                                    self.data["webhook_received_time"],
                                    TIMESTAMP_FORMAT
                                )
        ingest_time = datetime.utcnow()
        duration = ingest_time - webhook_received_time
        return duration.total_seconds() // 60

    @property
    def type_num(self):
        return ZOOM_RECORDING_TYPE_NUM

    @property
    def publisher(self):
        series_data = {k: v[0]["value"] for k, v in
                       json.loads(self.series_catalog)["http://purl.org/dc/terms/"].items()}

        if OVERRIDE_PUBLISHER and OVERRIDE_PUBLISHER != "None":
            return OVERRIDE_PUBLISHER
        elif "publisher" in series_data:
            return series_data["publisher"]
        elif DEFAULT_PUBLISHER:
            return DEFAULT_PUBLISHER

    @property
    def workflow_definition_id(self):
        return ZOOM_OPENCAST_WORKFLOW

    @property
    def s3_filenames(self):
        return self.data["s3_filenames"]

    @property
    def workflow_id(self):
        if not hasattr(self, "workflow_xml"):
            logger.warning("No workflow xml yet!")
            return None
        if not hasattr(self, "_workflow_id"):
            root = ET.fromstring(self.workflow_xml)
            self._workflow_id = root.attrib["id"]
        return self._workflow_id

    def upload(self):
        if not self.opencast_series_id:
            raise Exception("No opencast series id found!")
        if self.already_ingested():
            logger.warning("Episode with mediapackage id {} already ingested"
                           .format(self.mediapackage_id))
            return None
        self.get_series_catalog()
        self.ingest()
        return self.workflow_id

    def already_ingested(self):
        endpoint = "/workflow/instances.json?mp={}".format(self.mediapackage_id)
        try:
            resp = oc_api_request("GET", endpoint)
            logger.debug("Lookup for mpid: {}, {}"
                         .format(self.mediapackage_id, resp.json()))
            return int(resp.json()["workflows"]["totalCount"]) > 0
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == "404":
                return False

    def get_series_catalog(self):

        logger.info("Getting series catalog for series: {}"
                    .format(self.opencast_series_id))

        endpoint = "/series/{}.json".format(self.opencast_series_id)
        resp = oc_api_request("GET", endpoint)

        logger.debug({"series_catalog": resp.text})

        self.series_catalog = resp.text

    def ingest(self):
        logger.info("Adding mediapackage and ingesting.")

        video_files = None
        for view_type in MP4_VIEW_PRIORITY_LIST:
            if view_type in self.s3_filenames:
                video_files = self.s3_filenames[view_type]
                logger.info("Found '{}' view in files".format(view_type))
                break

        if not video_files:
            raise Exception("No mp4 files available for upload.")

        endpoint = ("/ingest/addMediaPackage/{}"
                    .format(self.workflow_definition_id))

        params = [
            ("creator", (None, escape(self.creator))),
            ("identifier", (None, self.mediapackage_id)),
            ("title", (None, "Lecture")),
            ("type", (None, self.type_num)),
            ("isPartOf", (None, self.opencast_series_id)),
            ("license",
                (None,
                 "Creative Commons 3.0: Attribution-NonCommercial-NoDerivs")
             ),
            ("publisher", (None, escape(self.publisher))),
            ("created", (None, self.created)),
            ("language", (None, "en")),
            ("seriesDCCatalog", (None, self.series_catalog)),
            ("source", (None, "Zoom")),
            ("spatial", (None, "Zoom"))
        ]

        for s3_filename in video_files:
            url = self._generate_presigned_url(s3_filename)
            params.extend([
                ("flavor", (None, escape(ZOOM_OPENCAST_FLAVOR))),
                ("mediaUri", (None, url))
            ])

        resp = oc_api_request("POST", endpoint, files=params)

        logger.debug({"addMediaPackage": resp.text})

        self.workflow_xml = resp.text

    def _generate_presigned_url(self, s3_filename):
        logger.info("Generate presigned url bucket {} key {}"
                    .format(ZOOM_VIDEOS_BUCKET, s3_filename))
        url = s3.meta.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": ZOOM_VIDEOS_BUCKET, "Key": s3_filename}
        )
        logger.info("Got presigned url {}".format(url))
        return url
