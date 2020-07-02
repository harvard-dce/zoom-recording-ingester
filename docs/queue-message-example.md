## Download Queue Message

```
{
  // unique id of meeting session
  "uuid": "abcdefghijklmnop==",
  // meeting room id
  "zoom_series_id": 123456789,
  // meeting topic
  "topic": "CSCI E-50",
  // recording start time
  "start_time": "2020-03-12T21:26:18Z",
  // recording duration
  "duration": 30,
  // user id of the event host
  "host_id": "dfjkdfjdlksklfjkdlsf",
  "recording_files": [
      {
          // unique file id
          "recording_id": "af1ed20c-64bc-11ea-bc55-0242ac130003",
          // recording segment start and end time
          "recording_start": "2020-03-12T21:26:11Z",
          "recording_end": "2020-03-12T21:26:30Z",
          // link to download file
          "download_url": "https://zoom.us/rec/download/abcdefg",
          "file_type": "MP4",
          // view type (speaker, gallery, shared screen)
          "recording_type": "shared_screen_with_speaker_view"
      }
  ],
  // webhook function's aws request id
  "correlation_id": "cab436d8-64bc-11ea-bc55-0242ac130003",
  "received_time": "2020-03-12T22:40:00Z",
  // optional fields
  "on_demand_series_id": "2020019999",
  "allow_multiple_ingests": true,
  "zoom_processing_minutes": 40
}
```

## Upload Queue Message

```
{
  "minutes_in_pipeline": 5,
  "body": {
      // unique id of meeting session
      "uuid": "abcdefghijklmnop==",
      // meeting room id
      "zoom_series_id": 123456789,
      "opencast_series_id": "20190199999",
      "host_name": "Foo Bar",
      "topic": "Special Lecture",
      "created": "2020-03-12T20:43:36Z",
      "created_local": "2020-03-12T16:43:36Z",
      "webhook_received_time": "2020-03-13T00:34:12Z",
      // webhook function's aws request id
      "correlation_id": "cab436d8-64bc-11ea-bc55-0242ac130003",
      "s3_files": {
          "shared_screen_with_speaker_view": {
            "segments": [
                {
                    "filename": "123456789/1584045816/000-shared_screen_with_speaker_view.mp4",
                    "recording_start": "2020-06-09T13:43:30Z",
                    "recording_end": "2020-06-09T14:07:26Z",
                    "ffprobe_bytes": 12345678,
                    "ffprobe_seconds": 500.4
                }
            ],
            "view_bytes": 12345678,
            "view_seconds": 500.4
        },
        "total_file_bytes": 12345678,
        "total_file_seconds": 500.4,
        "total_segment_seconds": 500.4
      },
      // optional fields
      "allow_multiple_ingests": true,
      "zoom_processing_minutes": 40
  }
}
```
