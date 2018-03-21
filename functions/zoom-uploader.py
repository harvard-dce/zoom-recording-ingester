import json
import boto3
import requests
from requests.auth import HTTPDigestAuth
from urllib.parse import urljoin
from hashlib import md5
from os import getenv as env
import xml.etree.ElementTree as ET
from datetime import datetime
from pytz import timezone

import logging
from common import setup_logging
logger = logging.getLogger()

UPLOAD_QUEUE_NAME = env("UPLOAD_QUEUE_NAME")
OPENCAST_BASE_URL = env("OPENCAST_BASE_URL")
OPENCAST_API_USER = env("OPENCAST_API_USER")
OPENCAST_API_PASSWORD = env("OPENCAST_API_PASSWORD")
ZOOM_VIDEOS_BUCKET = env('ZOOM_VIDEOS_BUCKET')
ZOOM_RECORDING_TYPE_NUM = 'S1'
DEFAULT_SERIES_ID = env('DEFAULT_SERIES_ID')

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

session = requests.Session()
session.auth = HTTPDigestAuth(OPENCAST_API_USER, OPENCAST_API_PASSWORD)
session.headers.update({
    'X-REQUESTED-AUTH': 'Digest',
    'X-Opencast-Matterhorn-Authentication': 'true',

})


def oc_api_request(method, endpoint, **kwargs):
    url = urljoin(OPENCAST_BASE_URL, endpoint)
    logger.info({'url': url, 'kwargs': kwargs})
    try:
        resp = session.request(method, url, **kwargs)
    except requests.RequestException as e:
        raise
    resp.raise_for_status()
    return resp


@setup_logging
def handler(event, context):

    # allow upload count to be overridden
    num_uploads = event['num_uploads']

    upload_queue = sqs.get_queue_by_name(QueueName=UPLOAD_QUEUE_NAME)

    for i in range(num_uploads):
        try:
            messages = upload_queue.receive_messages(
                MaxNumberOfMessages=1,
                VisibilityTimeout=2500
            )
            upload_message = messages[0]
            logger.debug({'queue_message': upload_message})

        except IndexError:
            logger.warning("No upload queue messages available")
            return
        try:
            upload_data = json.loads(upload_message.body)
            logger.info(upload_data)
            wf_id = process_upload(upload_data)
            logger.info("Workflow id {} initiated".format(wf_id))
            if wf_id:
                upload_message.delete()
        except Exception as e:
            logger.exception(e)
            raise


def process_upload(upload_data):
    upload = Upload(upload_data)
    upload.upload()
    return upload.workflow_id


class Upload:

    def __init__(self, data):
        self.data = data

    @property
    def creator(self):
        return self.data['host_name']

    @property
    def created(self):
        return self.data['start_time']

    @property
    def created_local(self, tz='US/Eastern'):
        zone = timezone(tz)
        utc = datetime.strptime(self.created, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone('UTC'))
        local = datetime.strftime(utc.astimezone(zone), '%B %-d, %Y at %H:%M:%S %Z')
        return local

    @property
    def title(self):
        return self.data['topic']

    @property
    def description(self):
        return "Zoom Session Recording by {} for {} on {}".format(
            self.creator, self.title, self.created_local
        )

    @property
    def meeting_uuid(self):
        return self.data['uuid']

    @property
    def s3_prefix(self):
        return md5(self.meeting_uuid.encode()).hexdigest()

    @property
    def zoom_series_id(self):
        return self.data['meeting_number']

    @property
    def opencast_series_id(self):
        if not hasattr(self, '_oc_series_id'):
            params = {
                'originHost': 'ZOOM',
                'originCourseId': self.zoom_series_id
            }
            try:
                resp = oc_api_request('GET', '/otherpubs/series/getreference', params=params)
                resp.raise_for_status()
                self._oc_series_id = resp.text
            except requests.RequestException as e:
                logger.warning("Opencast series id lookup failed")
                if DEFAULT_SERIES_ID is not None:
                    logger.info("Using default series id {}".format(DEFAULT_SERIES_ID))
                    self._oc_series_id = DEFAULT_SERIES_ID
                else:
                    self._oc_series_id = None

        return self._oc_series_id

    @property
    def type_num(self):
        return ZOOM_RECORDING_TYPE_NUM

    @property
    def publisher(self):
        # TODO: this should be configurable
        return "zoom-ingester@dce.harvard.edu"

    @property
    def contributor(self):
        # TODO: configurable
        return "Zoom Ingester"

    @property
    def workflow_definition_id(self):
        return "DCE-zoom-test-wf"

    @property
    def s3_files(self):
        if not hasattr(self, '_s3_files'):
            bucket = s3.Bucket(ZOOM_VIDEOS_BUCKET)
            logger.info("Looking for files in {} with prefix {}"
                        .format(ZOOM_VIDEOS_BUCKET, self.s3_prefix))
            objs = [
                x.Object() for x in bucket.objects.filter(Prefix=self.s3_prefix)
            ]
            self._s3_files = [
                x for x in objs if 'directory' not in x.content_type
            ]
            logger.debug({'s3_files': self._s3_files})
        return self._s3_files

    @property
    def speaker_videos(self):
        return self._get_video_files('speaker')

    @property
    def gallery_videos(self):
        return self._get_video_files('gallery')

    @property
    def episode_xml(self):

        if not hasattr(self, '_episode_xml'):
            self._episode_xml = """
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<dublincore xmlns="http://www.opencastproject.org/xsd/1.0/dublincore/" xmlns:dcterms="http://purl.org/dc/terms/">
    <dcterms:creator>{}</dcterms:creator>
    <dcterms:type>{}</dcterms:type>
    <dcterms:isPartOf>{}</dcterms:isPartOf>
    <dcterms:license>Creative Commons 3.0: Attribution-NonCommercial-NoDerivs</dcterms:license>
    <dcterms:publisher>{}</dcterms:publisher>
    <dcterms:title>{}</dcterms:title>
    <dcterms:contributor>{}</dcterms:contributor>
    <dcterms:created>{}</dcterms:created>
    <dcterms:language xmlns="http://www.w3.org/1999/xhtml">en</dcterms:language>
    <dcterms:description xmlns="http://www.w3.org/1999/xhtml">{}</dcterms:description>
</dublincore>
""" \
            .format(
                self.creator,
                self.type_num,
                self.opencast_series_id,
                self.publisher,
                self.title,
                self.contributor,
                self.created,
                self.description
            ).strip()

        logger.debug({'episode_xml': self._episode_xml})
        return self._episode_xml

    @property
    def workflow_id(self):
        if not hasattr(self, 'workflow_xml'):
            logger.warning("No workflow xml yet!")
            return None
        if not hasattr(self, '_workflow_id'):
            root = ET.fromstring(self.workflow_xml)
            self._workflow_id = root.attrib['id']
        return self._workflow_id

    def upload(self):
        if not self.verify_series_mapping():
            logger.info("No series mapping found for zoom series {}".format(self.zoom_series_id))
            return
        self.create_mediapackage()
        self.add_tracks()
        self.add_episode()
        self.ingest()
        return self.workflow_id

    def verify_series_mapping(self):
        return self.opencast_series_id is not None

    def create_mediapackage(self):
        logger.info("Creating mediapackage")
        create_mp_resp = oc_api_request('GET', '/ingest/createMediaPackage')
        self.mediapackage_xml = create_mp_resp.text
        logger.debug({'mediapackage_xml': self.mediapackage_xml})

    def add_tracks(self):

        logger.info("Adding tracks")
        speaker_vids = sorted(
            self.speaker_videos,
            key=lambda vid: vid.metadata['track_sequence']
        )
        for video in speaker_vids:
            url = self._generate_presigned_url(video)
            logger.info("Adding track {}".format(url))
            logger.debug({"metadata": video.metadata})
            add_track_params = {
                'mediaPackage': self.mediapackage_xml,
                'flavor': 'multipart/partsource',
                'url': url
            }
            add_track_resp = oc_api_request('POST', '/ingest/addTrack',
                                            data=add_track_params)
            self.mediapackage_xml = add_track_resp.text

    def add_episode(self):

        logger.info("Adding episode")
        add_episode_params = {
            'mediaPackage': self.mediapackage_xml,
            'flavor': 'dublincore/episode',
            'dublinCore': self.episode_xml
        }

        add_episode_resp = oc_api_request('POST', '/ingest/addDCCatalog',
                                          data=add_episode_params)
        self.mediapackage_xml = add_episode_resp.text

    def ingest(self):
        logger.info("Ingesting")
        ingest_params = {
            'mediaPackage': self.mediapackage_xml,
            'workflowDefinitionId': self.workflow_definition_id
        }

        ingest_resp = oc_api_request('POST', '/ingest/ingest',
                                     data=ingest_params)
        self.workflow_xml = ingest_resp.text

    def _generate_presigned_url(self, video):
        url = s3.meta.client.generate_presigned_url(
            'get_object',
            Params={'Bucket': video.bucket_name, 'Key': video.key}
        )
        return url

    def _get_video_files(self, view):
        return [
            x for x in self.s3_files
            if x.metadata['file_type'].lower() == 'mp4'
               and x.metadata.get('view') == view
        ]
