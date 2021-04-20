import io
import os
import site
import json
import pytest
import inspect
from os.path import dirname, join
from importlib import import_module
from datetime import datetime
from uuid import UUID
from hashlib import md5

TIMESTAMP_FORMAT = os.getenv("TIMESTAMP_FORMAT")

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

uploader = import_module("zoom-uploader")


def test_too_many_uploads(handler, mocker):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    uploader.sqs.get_queue_by_name = mocker.Mock()
    receive_messages = mocker.Mock(return_value=["mock-sqs-message"])
    uploader.sqs.get_queue_by_name.return_value.receive_messages = (
        receive_messages
    )
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=10)
    )
    uploader.process_upload = mocker.Mock()

    # with max = 5 and fake count = 10 the handler should abort
    # before processing upload
    mocker.patch.object(uploader, "OC_TRACK_UPLOAD_MAX", 5)
    handler(uploader, {})
    assert uploader.process_upload.call_count == 0


def test_unknown_uploads(handler, mocker):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    uploader.sqs.get_queue_by_name = mocker.Mock()
    receive_messages = mocker.Mock(return_value=["mock-sqs-message"])
    uploader.sqs.get_queue_by_name.return_value.receive_messages = (
        receive_messages
    )
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=None)
    )
    uploader.process_upload = mocker.Mock()

    # with max = 5 and fake count = None the handler should abort
    # before processing upload
    mocker.patch.object(uploader, "OC_TRACK_UPLOAD_MAX", 5)
    handler(uploader, {})
    assert uploader.process_upload.call_count == 0


def test_upload_count_ok(handler, mocker):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    mock_message = mocker.Mock()
    mock_message.attribute = "attribute"
    mock_message.body = json.dumps(
        {
            "webhook_received_time": datetime.strftime(
                datetime.now(), TIMESTAMP_FORMAT
            ),
            "correlation_id": 123,
        }
    )
    receive_messages = mocker.Mock(return_value=[mock_message])
    uploader.sqs.get_queue_by_name.return_value.receive_messages = (
        receive_messages
    )
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=3)
    )
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        uploader, "set_pipeline_status", mock_set_pipeline_status
    )
    uploader.process_upload = mocker.Mock()

    # with max = 3 and fake count = 5 the handler should proceed
    # to processing the upload
    mocker.patch.object(uploader, "OC_TRACK_UPLOAD_MAX", 5)
    handler(uploader, {})
    assert uploader.process_upload.call_count == 1


def test_no_messages_available(handler, mocker, caplog):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=3)
    )
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = (
        []
    )
    handler(uploader, {})
    assert caplog.messages[-1] == "No upload queue messages available."


def test_ingestion_error(handler, mocker, upload_message):
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        uploader, "set_pipeline_status", mock_set_pipeline_status
    )
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=3)
    )
    message = upload_message()
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = [
        message
    ]
    uploader.process_upload = mocker.Mock(side_effect=Exception("boom!"))
    with pytest.raises(Exception) as exc_info:
        handler(uploader, {})
    assert exc_info.match("boom!")
    # make sure the message doesn't get deleted
    assert message.delete.call_count == 0


def test_bad_message_body(handler, mocker):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=3)
    )
    message = mocker.Mock(body="this is definitely not json")
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = [
        message
    ]
    with pytest.raises(Exception) as exc_info:
        handler(uploader, {})
    assert exc_info.typename == "JSONDecodeError"
    # make sure the message doesn't get deleted
    assert message.delete.call_count == 0


def test_workflow_initiated(handler, mocker, upload_message, caplog):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=3)
    )
    message = upload_message()
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = [
        message
    ]
    uploader.process_upload = mocker.Mock(return_value=12345)
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        uploader, "set_pipeline_status", mock_set_pipeline_status
    )
    handler(uploader, {})
    assert "12345 initiated" in caplog.messages[-1]


def test_workflow_not_initiated(handler, mocker, upload_message, caplog):
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        uploader, "set_pipeline_status", mock_set_pipeline_status
    )
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    mocker.patch.object(
        uploader, "get_current_upload_count", mocker.Mock(return_value=3)
    )
    message = upload_message()
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = [
        message
    ]
    uploader.process_upload = mocker.Mock(return_value=None)
    handler(uploader, {})
    assert "No workflow initiated." == caplog.messages[-1]


def test_first_ingest_mpid_from_uuid(mocker):
    mock_uuid = "mock_uuid"
    upload_data = {"uuid": mock_uuid, "allow_multiple_ingests": False}
    upload = uploader.Upload(upload_data)
    upload.already_ingested = mocker.Mock(return_value=False)
    expected_mpid = str(UUID(md5(mock_uuid.encode()).hexdigest()))
    assert upload.mediapackage_id == expected_mpid


def test_multiple_ingests_not_allowed(mocker):
    upload_data = {
        "correlation_id": 123,
        "uuid": "mock_uuid",
        "allow_multiple_ingests": False,
    }
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        uploader, "set_pipeline_status", mock_set_pipeline_status
    )
    upload = uploader.Upload(upload_data)
    upload.already_ingested = mocker.Mock(return_value=True)
    assert not upload.mediapackage_id


def test_multiple_ingests_allowed(mocker):
    upload_data = {
        "correlation_id": 123,
        "uuid": "mock_uuid",
        "allow_multiple_ingests": True,
    }
    mock_set_pipeline_status = mocker.Mock(return_value=None)
    mocker.patch.object(
        uploader, "set_pipeline_status", mock_set_pipeline_status
    )
    upload = uploader.Upload(upload_data)
    upload.already_ingested = mocker.Mock(return_value=True)
    # mock already_ingested to return true
    assert upload.mediapackage_id


def test_get_current_upload_count(mocker):
    uploader.aws_lambda = mocker.Mock()
    cases = [
        (10, {"track": 5, "uri-track": 5}),
        (10, {"track": 5, "uri-track": 5, "foo": 3, "bar": 9}),
        (35, {"track": 35, "bar": 9}),
    ]
    for count, count_data in cases:
        uploader.aws_lambda.invoke.return_value = {
            "Payload": io.StringIO(json.dumps(count_data))
        }
        assert uploader.get_current_upload_count() == count

    uploader.aws_lambda.invoke.return_value = {
        "Payload": io.StringIO("no json here either")
    }
    assert uploader.get_current_upload_count() is None


def test_s3_filename_filter_false_start():
    # entire meetings < MINIMUM_DURATION should have been filtered out
    # in the downloader code so I don't test that case here

    single_file = {
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 0,
                    }
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/000-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 0,
                    }
                ]
            },
        }
    }

    false_start = {
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 5,
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 1,
                    },
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/000-shared_screen.mp4",
                        "ffprobe_seconds": 5,
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 1,
                    },
                ]
            },
        }
    }

    false_end = {
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-speaker_view.mp4",
                        "ffprobe_seconds": 5,
                        "segment_num": 1,
                    },
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/000-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-shared_screen.mp4",
                        "ffprobe_seconds": 5,
                        "segment_num": 1,
                    },
                ]
            },
        }
    }

    segment_mismatch = {
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 5,
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-speaker_view.mp4",
                        "ffprobe_seconds": 10,
                        "segment_num": 1,
                    },
                    {
                        "filename": "a/b/002-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 2,
                    },
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/001-shared_screen.mp4",
                        "ffprobe_seconds": 10,
                        "segment_num": 1,
                    },
                    {
                        "filename": "a/b/002-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "segment_num": 2,
                    },
                ]
            },
        }
    }

    cases = [
        (single_file, 1, 1),
        (false_start, 1, 1),
        (false_end, 2, 2),
        (segment_mismatch, 2, 2),
    ]

    for data, expected_speaker, expected_screen_share in cases:
        upload = uploader.Upload(data)
        assert len(upload.s3_filenames["speaker_view"]) == expected_speaker
        assert (
            len(upload.s3_filenames["screen_share"]) == expected_screen_share
        )


def test_file_param_generator():
    # each `cases` item is a list containing two iterables
    # - first element in list gets turned into the s3_filenames data
    # - 2nd element is the expected params that get generated
    cases = [
        [
            ("active_speaker", "gallery_view"),
            [
                ("active_speaker", "multipart"),
                ("gallery_view", "presentation"),
            ],
            1,
        ],
        [
            ("active_speaker", "shared_screen"),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
            ],
            2,
        ],
        [
            ("active_speaker", "shared_screen_with_speaker_view"),
            [
                ("active_speaker", "multipart"),
                ("shared_screen_with_speaker_view", "presentation"),
            ],
            3,
        ],
        [
            ("active_speaker", "shared_screen_with_gallery_view"),
            [
                ("active_speaker", "multipart"),
                ("shared_screen_with_gallery_view", "presentation"),
            ],
            4,
        ],
        [
            ("gallery_view", "shared_screen"),
            [("shared_screen", "multipart"), ("gallery_view", "presentation")],
            5,
        ],
        [
            ("gallery_view", "shared_screen_with_speaker_view"),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("gallery_view", "presentation"),
            ],
            6,
        ],
        [
            ("gallery_view", "shared_screen_with_gallery_view"),
            [
                ("shared_screen_with_gallery_view", "multipart"),
                ("gallery_view", "presentation"),
            ],
            7,
        ],
        [
            ("shared_screen", "shared_screen_with_speaker_view"),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen", "presentation"),
            ],
            8,
        ],
        [
            ("shared_screen", "shared_screen_with_gallery_view"),
            [
                ("shared_screen", "multipart"),
                ("shared_screen_with_gallery_view", "presentation"),
            ],
            9,
        ],
        [
            (
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen_with_gallery_view", "presentation"),
            ],
            10,
        ],
        [
            ("active_speaker", "gallery_view", "shared_screen"),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("gallery_view", "other"),
            ],
            11,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen_with_speaker_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("gallery_view", "presentation"),
                ("shared_screen_with_speaker_view", "other"),
            ],
            12,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("gallery_view", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
            ],
            13,
        ],
        [
            (
                "active_speaker",
                "shared_screen",
                "shared_screen_with_speaker_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("shared_screen_with_speaker_view", "other"),
            ],
            14,
        ],
        [
            (
                "active_speaker",
                "shared_screen",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
            ],
            15,
        ],
        [
            (
                "active_speaker",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen_with_gallery_view", "presentation"),
                ("shared_screen_with_speaker_view", "other"),
            ],
            16,
        ],
        [
            (
                "gallery_view",
                "shared_screen",
                "shared_screen_with_speaker_view",
            ),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen", "presentation"),
                ("gallery_view", "other"),
            ],
            17,
        ],
        [
            (
                "gallery_view",
                "shared_screen",
                "shared_screen_with_gallery_view",
            ),
            [
                ("shared_screen", "multipart"),
                ("shared_screen_with_gallery_view", "presentation"),
                ("gallery_view", "other"),
            ],
            18,
        ],
        [
            (
                "gallery_view",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen_with_gallery_view", "presentation"),
                ("gallery_view", "other"),
            ],
            19,
        ],
        [
            (
                "shared_screen",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
            ],
            20,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen",
                "shared_screen_with_speaker_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("gallery_view", "other"),
            ],
            21,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("gallery_view", "other"),
            ],
            22,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("gallery_view", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
            ],
            23,
        ],
        [
            (
                "active_speaker",
                "shared_screen",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
            ],
            24,
        ],
        [
            (
                "gallery_view",
                "shared_screen",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
            ],
            25,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("gallery_view", "other"),
            ],
            26,
        ],
        [tuple(), RuntimeError, 27],
    ]

    for (incoming, expected, case_no) in cases:
        fpg = uploader.FileParamGenerator(
            s3_filenames={x: ["{}.MP4".format(x)] for x in incoming}
        )
        fpg._generate_presigned_url = lambda f: "signed-{}".format(f)
        if inspect.isclass(expected):
            with pytest.raises(expected):
                upload_params = fpg.generate()
        else:
            expected_params = []
            for view, flavor in expected:
                expected_params.extend(
                    [
                        ("flavor", (None, "{}/chunked+source".format(flavor))),
                        ("mediaUri", (None, "signed-{}.MP4".format(view))),
                    ]
                )
            upload_params = fpg.generate()
            assert upload_params == expected_params, "case {}".format(case_no)


def test_file_param_generator_multi_set():
    cases = [
        # this one has 2 x speaker but only 1 shared screen
        [
            {
                "active_speaker": ["speaker-000.mp4", "speaker-001.mp4"],
                "shared_screen": ["screen-001.mp4"],
            },
            # shared screen view should be ignored
            [
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-000.mp4")),
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-001.mp4")),
            ],
        ],
        # has 2x for both views
        [
            {
                "active_speaker": ["speaker-000.mp4", "speaker-001.mp4"],
                "shared_screen": ["screen-000.mp4", "screen-001.mp4"],
            },
            # params should include both files for both views
            [
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-000.mp4")),
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-001.mp4")),
                ("flavor", (None, "presentation/chunked+source")),
                ("mediaUri", (None, "signed-screen-000.mp4")),
                ("flavor", (None, "presentation/chunked+source")),
                ("mediaUri", (None, "signed-screen-001.mp4")),
            ],
        ],
    ]
    for incoming, expected in cases:
        fpg = uploader.FileParamGenerator(s3_filenames=incoming)
        fpg._generate_presigned_url = lambda f: "signed-{}".format(f)
        upload_params = fpg.generate()
        assert upload_params == expected
