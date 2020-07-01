[![Build Status](https://travis-ci.org/harvard-dce/zoom-recording-ingester.svg?branch=master)](https://travis-ci.org/harvard-dce/zoom-recording-ingester)

# Zoom Video Ingester

A set of AWS services for downloading and ingesting Zoom meeting videos into Opencast

## Overview

The Zoom Ingester (a.k.a., "Zoom Ingester Pipeline", a.k.a., "ZIP") is Harvard DCE's mechanism for moving Zoom
recordings out of the Zoom service and into our own video management and delivery system, Opencast. It allows DCE
to deliver recorded Zoom class meetings and lectures alongside our other, non-Zoom video content. 

When deployed, the pipeline will have an API endpoint that must be registered in your Zoom account as a receiver of
completed recording events. When Zoom has completed the processing of a recorded meeting video it will send a "webhook"
notification to the pipeline's endpoint. From there the recording metadata will be passed along through a series of queues 
and Lambda functions before finally being ingested into Opencast. Along the way, the actual recording files will be
fetched from Zoom and stored in S3. Alternatively, from the Opencast admin interface, a user can kick off an "on-demand"
ingestion by entering the identifier of a Zoom recording and the corresponding Opencast series into which it should be 
ingested. The On-Demand ingest function then fetches the recording metadata from the Zoom API and emulates a standard
webhook.

Info on Zoom's API and webhook functionality can be found at:
  * [developer portal](https://marketplace.zoom.us/docs/guides)
  * [webhooks](https://marketplace.zoom.us/docs/api-reference/webhook-reference)


## Pipeline flow diagram

![zoom ingester pipeline diagram](docs/Zoom%20Ingester%20Pipeline.png)

## Wiki

For detailed instructions on setup, please see the wiki.

[https://github.com/harvard-dce/zoom-recording-ingester/wiki]()
