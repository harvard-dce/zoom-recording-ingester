# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

## [3.1.0] - 2022-01-13

### Added
- New optional parameters for on demand ingest (`/ingest` endpoint): `ingest_all_mp4` (boolean),
and `oc_workflow` (string).
- Added `ArchiveFileParamGenerator` to uploader lambda. When `ingest_all_mp4` is
true then the `ArchiveFileParamGenerator` generates file parameters for all files
using the view to flavor mapping:
```
{
        "active_speaker": "speaker/chunked+source",
        "shared_screen": "shared-screen/chunked+source",
        "gallery_view": "gallery/chunked+source",
        "shared_screen_with_gallery_view": "shared-screen-gallery/chunked+source",
        "shared_screen_with_speaker_view": "shared-screen-speaker/chunked+source",
}
```
- `BUFFER_MINUTES` env var for downloader function
- `stack.changeset` task for more cautious deploys

### Changed
- Changed env var OC_WORKFLOW to DEFAULT_OC_WORKFLOW.
- Removed unused env var OC_FLAVOR.
- Fixed bug where reason from previous status sometimes appeared.

## [3.0.1] - 2021-07-15

### Changed
- Fix error message indicating in which channel / DM the tool can be use to not
mention any channel if `SLACK_ZIP_CHANNEL` is not specified.

## [3.0.0] - 2021-07-07

### Added
- Slack tool for querying recording status from recording start to ingest
- New endpoint /slack for receiving notifications from the Slack tool
- New endpoint /status for querying status
- New dynamoDB table to store status data
- Linting (flake8 & black)
- Github action: automated lint and unit test checks

### Changed
- Change "Source" in Opencast from "Zoom Ingester Pipeline" to "ZIP-[uuid]"
- Dependency version updates
- Switch from using zak token to download token

## [2.4.7] - 2021-02-25

### Changed
- Increase dynamo write capacity from 1 to 3

## [2.4.6] - 2021-02-12

### Changed
- Increase uploader max run time and message visibility to 900 seconds

## [2.4.5] - 2021-02-10

### Changed
- Upgrade cdk to 1.89.0

## [2.4.4] - 2021-02-10

### Changed
- Upgrade httplib2 from 0.18.1 to 0.19.0
- Simplify handling of broken download links

## [2.4.3] - 2021-01-29

### Changed
- Increase uploader timeout from 300 to 600 seconds

## [2.4.2] - 2021-01-27

### Changed
- Remove "creator" from ingest metadata

## [2.4.1] - 2021-01-07

### Changed
- Require either Apigee credentials or Zoom API credentials but not both

## [2.4.0] - 2021-01-07

### Added
- Apigee support

## [2.3.1] - 2020-12-03

### Changed
- Ignore minimum duration check if on demand ingest

## [2.3.0] - 2020-11-19

### Changed
- Reworked schedule parsing, added title to schedule, interpret typenum based on title

## [2.2.3] - 2020-11-05

### Changed
- Include resource tags from `STACK_TAGS` in stack creation

## [2.2.2] - 2020-10-28

### Changed
- Fixed eleted files race condition
- Fixed google API service account credentials save function

## [2.2.1] - 2020-10-22

### Added
- Descriptive logging for schedule updates

## [2.2.0] - 2020-10-22

### Added
- "schedule_update" endpoint that, when triggered, updates the ZIP schedule from a pre-configured sheet from a Google Sheets document

## [2.1.0] - 2020-09-03

### Changed
- Filter false starts based on ffprobe duration data
- Handle case when all views not present in first segment
- Handle multi-day values like "MW", etc
- Change Saturday and Sunday codes to S and U
- Updates to schedule parser

## [2.0.0] - 2020-07-17

### Added
- CDK
- "allow_multiple_ingests" demand ingest endpoint query param
- `opencast-op-counts` function to check current number of running workflows in Opencast from the uploader lambda

### Removed
- `tasks.py` scripts that were made redundant by CDK

# [1.13.3]
# [1.13.2]
# [1.13.1]
# [1.13.0]
# [1.12.2]
# [1.12.1]
# [1.12.0]
# [1.11.6]
# [1.11.5]
# [1.11.4]
# [1.11.3]
# [1.11.2]
# [1.11.1]
# [1.11.0]
# [1.10.1]
# [1.10.0]
# [1.09.1]
# [1.09.0]
# [1.08.7]
# [1.08.6]
# [1.08.5]
# [1.08.4]
# [1.08.3]
# [1.08.2]
# [1.08.1]
# [1.08.0]
# [1.06.0]
# [1.05.0]
# [1.04.1]
# [1.04.0]
# [1.03.1]
# [1.03.0]
# [1.02.0]
# [1.01.0]
# [1.00.2]
# [1.00.1]
# [1.00.0]
