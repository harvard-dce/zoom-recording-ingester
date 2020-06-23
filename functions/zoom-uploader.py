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
from common import TIMESTAMP_FORMAT, setup_logging


import logging
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
OC_OP_COUNT_FUNCTION = env("OC_OP_COUNT_FUNCTION")
OC_TRACK_UPLOAD_MAX = int(env("OC_TRACK_UPLOAD_MAX", 5))

s3 = boto3.resource("s3")
aws_lambda = boto3.client("lambda")
sqs = boto3.resource('sqs')

session = requests.Session()
session.auth = HTTPDigestAuth(OPENCAST_API_USER, OPENCAST_API_PASSWORD)
session.headers.update({
    "X-REQUESTED-AUTH": "Digest",
    # TODO: it's possible this header is not necessary for the endpoints being
    # used here. It seems like for Opencast endpoints where the header *is*
    # necessary the correct value is actually
    # "X-Opencast-Matterhorn-Authorization"
    "X-Opencast-Matterhorn-Authentication": "true",
})

UPLOAD_OP_TYPES = [
    'track',
    'uri-track'
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


@setup_logging
def handler(event, context):

    upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

    messages = upload_queue.receive_messages(
        MaxNumberOfMessages=1,
        VisibilityTimeout=300
    )
    if len(messages) == 0:
        logger.warning("No upload queue messages available.")
        return
    else:
        logger.info("{} upload messages in queue".format(len(messages)))

    # don't ingest of opencast is overloaded
    current_uploads = get_current_upload_count()
    if current_uploads is None:
        logger.error("Unable to determine number of existing upload ops")
        return
    elif current_uploads >= OC_TRACK_UPLOAD_MAX:
        logger.warning(
            "Too many current track uploads: {}".format(current_uploads)
        )
        return
    else:
        logger.info(
            "Opencast upload count looks good: {}".format(current_uploads)
        )

    upload_message = messages[0]
    logger.debug({
        "queue_message": {
            "body": upload_message.body
        }
    })

    try:
        upload_data = json.loads(upload_message.body)
        logger.debug({"processing": upload_data})

        wf_id = process_upload(upload_data)
        upload_message.delete()
        if wf_id:
            logger.info(f"Workflow id {wf_id} initiated.")
            # only ingest one per invocation
        else:
            logger.info("No workflow initiated.")

    except Exception as e:
        logger.exception(e)
        raise


def minutes_in_pipeline(webhook_received_time):
    start_time = datetime.strptime(webhook_received_time, TIMESTAMP_FORMAT)
    ingest_time = datetime.utcnow()
    duration = ingest_time - start_time
    return duration.total_seconds() // 60


def get_current_upload_count():
    try:
        resp = aws_lambda.invoke(FunctionName=OC_OP_COUNT_FUNCTION)
        op_counts = json.load(resp['Payload'])
        logger.info('op counts: {}'.format(op_counts))
        return sum(
            v for k, v in op_counts.items()
            if v is not None and k in UPLOAD_OP_TYPES
        )
    except Exception as e:
        logger.exception(e)
        return None


def process_upload(upload_data):
    upload = Upload(upload_data)
    wf_id = upload.upload()
    return wf_id


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

            # first uuid generated should be deterministic
            # regardless of the allow_multiple_ingests flag
            mpid = str(UUID(md5(self.meeting_uuid.encode()).hexdigest()))
            if self.already_ingested(mpid):
                if self.data["allow_multiple_ingests"]:
                    # random uuid
                    mpid = str(uuid4())
                    logger.info(
                        f"Created random mediapackage id {mpid}"
                    )
                else:
                    logger.warning(
                        "Episode with deterministic mediapackage id"
                        f" {mpid} already ingested"
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
        if not hasattr(self, "_s3_filenames"):
            self._s3_filenames = {}
            for view, file in self.data["s3_files"].items():
                self._s3_filenames[view] = [data["filename"] for data in file["segments"]]

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
        logger.info({
            "workflow_id": wf_id,
            "mediapackage_id": self.mediapackage_id,
            "minutes_in_pipeline": minutes_in_pipeline(
                    self.data["webhook_received_time"]),
            "recording_data": self.data
        })
        return wf_id

    def already_ingested(self, mpid):
        endpoint = "/workflow/instances.json?mp={}".format(mpid)
        try:
            resp = oc_api_request("GET", endpoint)
            logger.debug("Lookup for mpid: {}, {}"
                         .format(mpid, resp.json()))
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

        endpoint = ("/ingest/addMediaPackage/{}"
                    .format(self.workflow_definition_id))

        params = [
            ("creator", (None, escape(self.creator))),
            ("identifier", (None, self.mediapackage_id)),
            ("title", (None, "Lecture")),
            ("type", (None, self.type_num)),
            ("isPartOf", (None, self.opencast_series_id)),
            ("license", (None, "Creative Commons 3.0: Attribution-NonCommercial-NoDerivs")),
            ("publisher", (None, escape(self.publisher))),
            ("created", (None, self.created)),
            ("language", (None, "en")),
            ("seriesDCCatalog", (None, self.series_catalog)),
            ("source", (None, "Zoom Ingester Pipeline")),
            ("spatial", (None, "Zoom {}".format(self.zoom_series_id)))
        ]

        fpg = FileParamGenerator(self.s3_filenames)
        try:
            file_params = fpg.generate()
        except Exception as e:
            logger.exception("Failed to generate file upload params")
            raise

        params.extend(file_params)
        resp = oc_api_request("POST", endpoint, files=params)
        logger.debug({"addMediaPackage": resp.text})
        self.workflow_xml = resp.text


class FileParamGenerator(object):

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
    }

    VIEW_PRIORITIES = {
        # if we have this...
        "active_speaker": [
            # then take these in this order...
            "shared_screen",
            "gallery_view",
            "shared_screen_with_gallery_view",
            "shared_screen_with_speaker_view"
        ],
        "shared_screen_with_speaker_view": [
            "shared_screen",
            "shared_screen_with_gallery_view",
            "gallery_view",
        ]
    }

    # if we don't have either of the above then there can only be three
    # possible views left and we just try them all in this order
    FALLBACK_PRIORITIES = [
        "shared_screen",
        "shared_screen_with_gallery_view",
        "gallery_view",
    ]

    def __init__(self, s3_filenames):
        self.s3_filenames = s3_filenames
        self._used_views = []
        self._params = []
        # whatever the max length of any view's list of files is the number
        # of sets of files we're dealing with. When hosts stop/start a meeting
        # it results in multiple file sets being generated
        self._file_sets = max(
            (len(x) for x in s3_filenames.values()),
            default=0
        )

    @property
    def flavors(self):
        return [x[1][1] for x in self._params if x[0] == "flavor"]

    def _has_view(self, view):
        if view in self.s3_filenames:
            if self._file_sets == 1:
                return True
            # if there's more than one set of files and this particular view
            # isn't present in all of them, then it's not ingestable
            elif len(self.s3_filenames[view]) == self._file_sets:
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

    def _add_view(self, flavor, view):
        """
        params must be added in pairs
        each "view" consists of a flavor (file type) param and
        a mediaUri param the s3 url of the file
        :param flavor:
        :param view:
        :return:
        """

        if view in self._used_views:
            # we already got this view
            return

        if len(self._used_views) >= len(self.FLAVORS):
            # we already got all the flavors
            return

        for s3_file in self.s3_filenames[view]:
            logger.info({
                "adding": {
                    "s3_file": s3_file,
                    "view": view,
                    "flavor": flavor
                }
            })
            self._params.extend([
                ("flavor", (None, escape(flavor))),
                ("mediaUri", (None, self._generate_presigned_url(s3_file)))
            ])
            self._used_views.append(view)

    def _generate_presigned_url(self, s3_filename):
        logger.info("Generate presigned url bucket {} key {}"
                    .format(ZOOM_VIDEOS_BUCKET, s3_filename))
        url = s3.meta.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": ZOOM_VIDEOS_BUCKET, "Key": s3_filename}
        )
        logger.info("Got presigned url {}".format(url))
        return url

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

        return self._params

