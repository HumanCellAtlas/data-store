# DSS sync daemon

Event handlers in the dss-sync daemon use utility functions in
[dss.events.handlers.sync](../../dss/events/handlers/sync.py).

Events are first received from object storage:

* S3 invokes the dss-sync Lambda via SNS (although the event handler below sees an S3 event notification since Domovoi
  unpacks the SNS envelope). The event handler then calls `dss.events.handlers.sync.sync_blob`.

* GS invokes the `/internal/notify` route of the dss Lambda via Google Cloud Pub/Sub. The route handler then calls
  `dss.events.handlers.sync.sync_blob`.

If the event concerns an object that already exists in the destination bucket, the process stops.

If the event concerns an object less than 64MB in size (the size of one S3 part), it is copied immediately in the
process. Otherwise, work to copy parts of the object is dispatched by triggering lambdas over the SNS topic
`dss-copy-parts`. After copying its assigned parts, each lambda queries the destination object store for the state of
the object's parts. If the work is almost complete, the lambdas call another SNS topic to join the parts and finalize
the upload (`dss-s3-mpu-ready` for S3, `dss-gs-composite-upload-ready` for GS).

All the event handler lambdas above are actually the same lambda distribution (dss-sync), called via different entry
points by different S3 or SNS events, as seen in decorators below.

An example SNS message sent to the worker lambdas: (TODO: (akislyuk) formalize SNS message schema/protocol)

    {"source_platform": "s3",
     "source_bucket": "hca-test",
     "source_key": "hca-dss-sync-test/copy-part/991a4058-4671-43f4-86c7-77ef69157f56",
     "dest_platform": "gs",
     "dest_bucket": "hca-test",
     "dest_key": "hca-dss-sync-test/copy-part/991a4058-4671-43f4-86c7-77ef69157f56",
     "parts": [{"start": 0, "end": 1048575, "id": 1, "total_parts": 1}],
     "total_parts": 1,
     "mpu": None}
