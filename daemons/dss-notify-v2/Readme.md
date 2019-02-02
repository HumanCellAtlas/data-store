# DSS Notification Daemon version 2

Event handlers in the dss-notify-v2 daemon use utility functions in
[dss.events.handlers.notify_v2](../../dss/events/handlers/notify_v2.py).

Events are either received from object storage:

* S3 invokes the `dss-notify-v2` Lambda via an S3 event notification forwarded via SQS (`app.py:launch_from_s3_event()`).

* GS invokes the `dss-gs-event-relay` GCF via a PubSub event notification
  (`/daemons/dss-gs-event-relay-python/main.py:dss_gs_event_relay()`). The GCF then forwards the event to the
  `dss-notify-v2` Lambda via SQS (`app.py:launch_from_forwarded_event()`).

or via SQS redrive queue:

* Lambda-triggered via SQS `dss-notify-v2-{DSS_DEPLOYMENT_STAGE}`

If notification delivery fails, a notification record is made in the redrive queue. Delivery will be attempted once
after 15 minutes, and again every hour for 7 days. Delivery delay and duration is configured via SQS DelaySeconds
and VisibilityTimeout.
