import os
import site
import pytest
import inspect
from os.path import dirname, join
from uuid import UUID
from hashlib import md5
from utils import PipelineStatus

site.addsitedir(join(dirname(dirname(__file__)), "functions"))
import zoom_uploader as uploader

TIMESTAMP_FORMAT = os.getenv("TIMESTAMP_FORMAT")


def test_no_messages_available(handler, mocker, caplog):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = (
        []
    )
    handler(uploader, {})
    assert caplog.messages[-1] == "No upload queue messages available."


def test_ingestion_error(handler, mocker, upload_message):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
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
    message = mocker.Mock(body="this is definitely not json")
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = [
        message
    ]
    with pytest.raises(Exception) as exc_info:
        handler(uploader, {})
    assert exc_info.typename == "JSONDecodeError"
    # make sure the message doesn't get deleted
    assert message.delete.call_count == 0


def setup_upload(mocker, upload_message):
    mocker.patch.object(uploader, "sqs", mocker.Mock())
    message = upload_message({"zip_id": "mock_zip_id"})
    uploader.sqs.get_queue_by_name.return_value.receive_messages.return_value = [
        message
    ]


def test_workflow_initiated(handler, mocker, upload_message, caplog):
    setup_upload(mocker, upload_message)
    uploader.process_upload = mocker.Mock(return_value=12345)
    handler(uploader, {})
    assert "12345 initiated" in caplog.messages[-1]


def test_workflow_not_initiated(handler, mocker, upload_message, caplog):
    setup_upload(mocker, upload_message)
    uploader.process_upload = mocker.Mock(return_value=None)
    handler(uploader, {})
    assert "No workflow initiated." == caplog.messages[-1]


def test_invalid_oc_series_id(
    handler,
    mocker,
    upload_message,
    mock_uploader_set_pipeline_status,
):
    setup_upload(mocker, upload_message)

    uploader.process_upload = mocker.Mock(
        side_effect=uploader.InvalidOpencastSeriesId
    )

    with pytest.raises(uploader.InvalidOpencastSeriesId):
        handler(uploader, {})

    mock_uploader_set_pipeline_status.assert_called_with(
        "mock_zip_id",
        PipelineStatus.UPLOADER_FAILED,
        reason="Invalid Opencast series id.",
    )


def test_opencast_unreachable(
    handler,
    mocker,
    upload_message,
    mock_uploader_set_pipeline_status,
):
    setup_upload(mocker, upload_message)

    uploader.process_upload = mocker.Mock(
        side_effect=uploader.OpencastConnectionError
    )

    with pytest.raises(uploader.OpencastConnectionError):
        handler(uploader, {})

    mock_uploader_set_pipeline_status.assert_called_with(
        "mock_zip_id",
        PipelineStatus.UPLOADER_FAILED,
        reason="Unable to reach Opencast.",
    )


def test_first_ingest_mpid_from_uuid(mocker):
    mock_uuid = "mock_uuid"
    upload_data = {"uuid": mock_uuid, "allow_multiple_ingests": False}
    upload = uploader.Upload(upload_data)
    upload.already_ingested = mocker.Mock(return_value=False)
    expected_mpid = str(UUID(md5(mock_uuid.encode()).hexdigest()))
    assert upload.mediapackage_id == expected_mpid


def test_multiple_ingests_not_allowed(mocker):
    upload_data = {
        "zip_id": 123,
        "uuid": "mock_uuid",
        "allow_multiple_ingests": False,
    }
    upload = uploader.Upload(upload_data)
    upload.already_ingested = mocker.Mock(return_value=True)
    assert not upload.mediapackage_id


def test_multiple_ingests_allowed(mocker):
    upload_data = {
        "zip_id": 123,
        "uuid": "mock_uuid",
        "allow_multiple_ingests": True,
    }
    upload = uploader.Upload(upload_data)
    upload.already_ingested = mocker.Mock(return_value=True)
    # mock already_ingested to return true
    assert upload.mediapackage_id


def test_s3_filename_filter_false_start(mocker):
    # entire meetings < MINIMUM_DURATION should have been filtered out
    # in the downloader code so I don't test that case here

    single_file = {
        "created": "2021-08-02T15:44:53Z",
        "created_local": "2021-08-02T11:44:53Z",
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:53:14Z",
                        "segment_num": 0,
                    }
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/000-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:53:14Z",
                        "segment_num": 0,
                    }
                ]
            },
            "chat_file": {
                "segments": [
                    {
                        "filename": "a/b/000-chat_file.mp4",
                        "ffprobe_seconds": 0,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:53:14Z",
                        "segment_num": 0,
                    }
                ]
            },
        },
    }

    false_start = {
        "created": "2021-08-02T15:44:53Z",
        "created_local": "2021-08-02T11:44:53Z",
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 5,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:44:59Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T16:02:20Z",
                        "segment_num": 1,
                    },
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/000-shared_screen.mp4",
                        "ffprobe_seconds": 5,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:44:59Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T16:02:20Z",
                        "segment_num": 1,
                    },
                ]
            },
            "chat_file": {
                "segments": [
                    {
                        "filename": "a/b/000-chat_file.mp4",
                        "ffprobe_seconds": 0,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:44:59Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-chat_file.mp4",
                        "ffprobe_seconds": 0,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T16:02:20Z",
                        "segment_num": 1,
                    },
                ]
            },
        },
    }

    false_end = {
        "created": "2021-08-02T15:44:53Z",
        "created_local": "2021-08-02T11:44:53Z",
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:53:14Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-speaker_view.mp4",
                        "ffprobe_seconds": 5,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T15:54:05Z",
                        "segment_num": 1,
                    },
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/000-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T15:44:54Z",
                        "recording_end": "2021-08-02T15:53:14Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-shared_screen.mp4",
                        "ffprobe_seconds": 5,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T15:54:05Z",
                        "segment_num": 1,
                    },
                ]
            },
        },
    }

    segment_mismatch = {
        "created": "2021-08-02T15:41:00Z",
        "created_local": "2021-08-02T11:41:00Z",
        "s3_files": {
            "speaker_view": {
                "segments": [
                    {
                        "filename": "a/b/000-speaker_view.mp4",
                        "ffprobe_seconds": 5,
                        "recording_start": "2021-08-02T15:40:00Z",
                        "recording_end": "2021-08-02T15:40:05Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-speaker_view.mp4",
                        "ffprobe_seconds": 10,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T15:54:10Z",
                        "segment_num": 1,
                    },
                    {
                        "filename": "a/b/002-speaker_view.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T16:00:00Z",
                        "recording_end": "2021-08-02T16:08:20Z",
                        "segment_num": 2,
                    },
                ]
            },
            "screen_share": {
                "segments": [
                    {
                        "filename": "a/b/001-shared_screen.mp4",
                        "ffprobe_seconds": 10,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T15:54:10Z",
                        "segment_num": 1,
                    },
                    {
                        "filename": "a/b/002-shared_screen.mp4",
                        "ffprobe_seconds": 500,
                        "recording_start": "2021-08-02T16:00:00Z",
                        "recording_end": "2021-08-02T16:08:20Z",
                        "segment_num": 2,
                    },
                ]
            },
            "chat_file": {
                "segments": [
                    {
                        "filename": "a/b/000-chat_file.mp4",
                        "ffprobe_seconds": 0,
                        "recording_start": "2021-08-02T15:40:00Z",
                        "recording_end": "2021-08-02T15:48:20Z",
                        "segment_num": 0,
                    },
                    {
                        "filename": "a/b/001-chat_file.mp4",
                        "ffprobe_seconds": 0,
                        "recording_start": "2021-08-02T15:54:00Z",
                        "recording_end": "2021-08-02T15:54:10Z",
                        "segment_num": 1,
                    },
                    {
                        "filename": "a/b/002-chat_file.mp4",
                        "ffprobe_seconds": 0,
                        "recording_start": "2021-08-02T16:00:00Z",
                        "recording_end": "2021-08-02T16:08:20Z",
                        "segment_num": 2,
                    },
                ]
            },
        },
    }

    cases = [
        (
            single_file,
            1,
            1,
            1,
            "2021-08-02T15:44:54Z_2021-08-02T15:53:14Z",
            ["2021-08-02T15:44:54Z_2021-08-02T15:53:14Z"],
        ),
        (
            false_start,
            1,
            1,
            1,
            "2021-08-02T15:54:00Z_2021-08-02T16:02:20Z",
            ["2021-08-02T15:54:00Z_2021-08-02T16:02:20Z"],
        ),
        (
            false_end,
            2,
            2,
            0,
            "2021-08-02T15:44:54Z_2021-08-02T15:53:14Z,2021-08-02T15:54:00Z_2021-08-02T15:54:05Z",
            [
                "2021-08-02T15:44:54Z_2021-08-02T15:53:14Z",
                "2021-08-02T15:54:00Z_2021-08-02T15:54:05Z",
            ],
        ),
        (
            segment_mismatch,
            2,
            2,
            2,
            "2021-08-02T15:54:00Z_2021-08-02T15:54:10Z,2021-08-02T16:00:00Z_2021-08-02T16:08:20Z",
            [
                "2021-08-02T15:54:00Z_2021-08-02T15:54:10Z",
                "2021-08-02T16:00:00Z_2021-08-02T16:08:20Z",
            ],
        ),
    ]

    for (
        data,
        expected_speaker,
        expected_screen_share,
        expected_chat,
        expected_recording_times,
        mock_recording_times,
    ) in cases:
        data["zip_id"] = "on-demand-mock_zip_id"
        data["uuid"] = "mock_zip_id"
        upload = uploader.Upload(data)
        assert len(upload.s3_filenames["speaker_view"]) == expected_speaker
        assert (
            len(upload.s3_filenames["screen_share"]) == expected_screen_share
        )
        if expected_chat > 0:
            assert len(upload.s3_filenames["chat_file"]) == expected_chat
        else:
            assert "chat_file" not in upload.s3_filenames
        assert upload.recording_times == expected_recording_times


def test_publish_file_param_generator():
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
        [
            ("active_speaker", "shared_screen", "chat_file"),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("chat_file", "chat"),
            ],
            28,
        ],
        [
            (
                "active_speaker",
                "gallery_view",
                "shared_screen",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
                "chat_file",
            ),
            [
                ("active_speaker", "multipart"),
                ("shared_screen", "presentation"),
                ("gallery_view", "other"),
                ("chat_file", "chat"),
            ],
            29,
        ],
        [
            (
                "shared_screen",
                "shared_screen_with_speaker_view",
                "shared_screen_with_gallery_view",
                "chat_file",
            ),
            [
                ("shared_screen_with_speaker_view", "multipart"),
                ("shared_screen", "presentation"),
                ("shared_screen_with_gallery_view", "other"),
                ("chat_file", "chat"),
            ],
            30,
        ],
    ]

    for incoming, expected, case_no in cases:
        fpg = uploader.PublishFileParamGenerator(
            s3_filenames={
                x: [x + (".MP4" if x != "chat_file" else ".TXT")]
                for x in incoming
            }
        )
        fpg._generate_presigned_url = lambda f: "signed-{}".format(f)
        if inspect.isclass(expected):
            with pytest.raises(expected):
                upload_params = fpg.generate()
        else:
            expected_params = []
            for view, flavor in expected:
                uri_param = "mediaUri" if flavor != "chat" else "attachmentUri"
                ext = "MP4" if flavor != "chat" else "TXT"
                expected_params.extend(
                    [
                        ("flavor", (None, "{}/chunked+source".format(flavor))),
                        (uri_param, (None, f"signed-{view}.{ext}")),
                    ]
                )
            upload_params = fpg.generate()
            assert upload_params == expected_params, "case {}".format(case_no)


def test_publish_file_param_generator_multi_set():
    cases = [
        # this one has 2 x speaker but only 1 shared screen
        [
            {
                "active_speaker": ["speaker-000.mp4", "speaker-001.mp4"],
                "shared_screen": ["screen-001.mp4"],
                "chat_file": ["chat-000.txt", "chat-001.txt"],
            },
            # ZIP-98 shared screen view should still be ingested
            [
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-000.mp4")),
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-001.mp4")),
                ("flavor", (None, "presentation/chunked+source")),
                ("mediaUri", (None, "signed-screen-001.mp4")),
                ("flavor", (None, "chat/chunked+source")),
                ("attachmentUri", (None, "signed-chat-000.txt")),
                ("flavor", (None, "chat/chunked+source")),
                ("attachmentUri", (None, "signed-chat-001.txt")),
            ],
        ],
        # has 2x for both views, 1 for chat (but that should not affect the media ingested)
        [
            {
                "active_speaker": ["speaker-000.mp4", "speaker-001.mp4"],
                "shared_screen": ["screen-000.mp4", "screen-001.mp4"],
                "chat_file": ["chat-000.txt"],
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
                ("flavor", (None, "chat/chunked+source")),
                ("attachmentUri", (None, "signed-chat-000.txt")),
            ],
        ],
        # Make sure having 3 segments doesn't break the file param generator!
        [
            {
                "active_speaker": [
                    "speaker-000.mp4",
                    "speaker-001.mp4",
                    "speaker-002.mp4",
                ],
                "shared_screen_with_speaker_view": [
                    "screen-000.mp4",
                    "screen-001.mp4",
                    "screen-002.mp4",
                ],
                "chat_file": [
                    "chat-000.txt",
                    "chat-001.txt",
                    "chat-002.txt",
                ],
            },
            # params should include both files for both views
            [
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-000.mp4")),
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-001.mp4")),
                ("flavor", (None, "multipart/chunked+source")),
                ("mediaUri", (None, "signed-speaker-002.mp4")),
                ("flavor", (None, "presentation/chunked+source")),
                ("mediaUri", (None, "signed-screen-000.mp4")),
                ("flavor", (None, "presentation/chunked+source")),
                ("mediaUri", (None, "signed-screen-001.mp4")),
                ("flavor", (None, "presentation/chunked+source")),
                ("mediaUri", (None, "signed-screen-002.mp4")),
                ("flavor", (None, "chat/chunked+source")),
                ("attachmentUri", (None, "signed-chat-000.txt")),
                ("flavor", (None, "chat/chunked+source")),
                ("attachmentUri", (None, "signed-chat-001.txt")),
                ("flavor", (None, "chat/chunked+source")),
                ("attachmentUri", (None, "signed-chat-002.txt")),
            ],
        ],
    ]
    for incoming, expected in cases:
        fpg = uploader.PublishFileParamGenerator(s3_filenames=incoming)
        fpg._generate_presigned_url = lambda f: "signed-{}".format(f)
        upload_params = fpg.generate()
        assert upload_params == expected


def test_archive_file_param_generator():
    cases = [
        [
            {
                "active_speaker": [
                    "speaker-000.mp4",
                    "speaker-001.mp4",
                    "speaker-002.mp4",
                ],
                "shared_screen": ["screen-001.mp4"],
            },
            [
                ("flavor", (None, "speaker/chunked+source")),
                ("mediaUri", (None, "signed-speaker-000.mp4")),
                ("flavor", (None, "speaker/chunked+source")),
                ("mediaUri", (None, "signed-speaker-001.mp4")),
                ("flavor", (None, "speaker/chunked+source")),
                ("mediaUri", (None, "signed-speaker-002.mp4")),
                ("flavor", (None, "shared-screen/chunked+source")),
                ("mediaUri", (None, "signed-screen-001.mp4")),
            ],
        ],
        [
            {
                "active_speaker": ["speaker-000.mp4", "speaker-001.mp4"],
                "shared_screen": ["screen-000.mp4"],
                "gallery_view": ["gallery-000.mp4"],
                "shared_screen_with_gallery_view": [
                    "shared-screen-gallery-000.mp4"
                ],
                "shared_screen_with_speaker_view": [
                    "shared-screen-speaker-000.mp4"
                ],
            },
            [
                ("flavor", (None, "speaker/chunked+source")),
                ("mediaUri", (None, "signed-speaker-000.mp4")),
                ("flavor", (None, "speaker/chunked+source")),
                ("mediaUri", (None, "signed-speaker-001.mp4")),
                ("flavor", (None, "shared-screen/chunked+source")),
                ("mediaUri", (None, "signed-screen-000.mp4")),
                ("flavor", (None, "gallery/chunked+source")),
                ("mediaUri", (None, "signed-gallery-000.mp4")),
                ("flavor", (None, "shared-screen-gallery/chunked+source")),
                ("mediaUri", (None, "signed-shared-screen-gallery-000.mp4")),
                ("flavor", (None, "shared-screen-speaker/chunked+source")),
                ("mediaUri", (None, "signed-shared-screen-speaker-000.mp4")),
            ],
        ],
    ]

    for incoming, expected in cases:
        fpg = uploader.ArchiveFileParamGenerator(s3_filenames=incoming)
        fpg._generate_presigned_url = lambda f: f"signed-{f}"
        upload_params = fpg.generate()
        assert upload_params == expected
