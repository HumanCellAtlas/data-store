# DSS Notification Daemon version 2

The dss-notify-v2 daemon delivers bundle event notifications. Events are optionally filtered by supplying a JMESPath
query, which is applied to a bundle metadata document described below.

## Notifications

Notification delivery is triggered either by object storage events:

* S3 invokes the `dss-notify-v2` Lambda via an S3 event notification forwarded via SQS (`app.py:launch_from_s3_event()`).

* GS invokes the `dss-gs-event-relay` GCF via a PubSub event notification
  (`/daemons/dss-gs-event-relay-python/main.py:dss_gs_event_relay()`). The GCF then forwards the event to the
  `dss-notify-v2` Lambda via SQS (`app.py:launch_from_forwarded_event()`).

or via SQS queue:

* Lambda-triggered via SQS `dss-notify-v2-{DSS_DEPLOYMENT_STAGE}`

If notification delivery fails, a notification record is made in the queue. Delivery will be attempted once
after 15 minutes, and again every hour for 7 days. Delivery delay and duration is configured via SQS DelaySeconds
and VisibilityTimeout.

Event handlers in the dss-notify-v2 daemon use utility functions in
[dss.events.handlers.notify_v2](../../dss/events/handlers/notify_v2.py).

### Payload Format

```
{
  'transaction_id': {uuid},
  'subscription_id': {uuid},
  'type': "CREATE"|"TOMBSTONE"|"DELETE",
  'match': {
    'bundle_uuid': {uuid},
    'bundle_version': {version},
  }
  'attachments': {
    "attachment_name_1": {value},
    "attachment_name_1": {value},
    ...
    "_errors": [...]
  }
}
```
The `attachment` field is subscription dependent and may not be present.

## Subscriptions

Event subscriptions are managed via the `/subscriptions` API endpoint, described in detail by the
[DSS OpenAPI specification](../../dss-api.yml).

### Bundle Metadata Document

The bundle metadata document is constructed from json files present in the bundle manifest, and
includes the bundle manifest describe in the [DSS OpenAPI sepcifications](../../dss-api.yml). The structure
accomadates the possibility of multiple files sharing a name.

The bundle metadata document format for a normal bundle is

```
{
  'manifest': {bundle_manifest}
  'files': {
    {file_name_1}: [
      {json_contents},
      ...
    ],
    {file_name_1}: [
      {json_contents},
      ...
    ],
    ...
  }
}
```

and for a tombstone is
```
{
  'admin_deleted': "true",
  'email': {admin_email},
  'reason': "the reason",
  'version': "the version",
}
```

### JMESPath Filtering

Subcriptions containing a non-empty `jmespath_query` will recieve notifications when `jmespath_query` matches the
bundle metadata document.

#### Examples

To recieve notifications only for tombstones, use

```
jmespath_query = "admin_deleted==`true`"
```

To recieve notifications for everything EXCEPT tombstones, use

```
jmespath_query = "manifest"
```

For the bundle metadata document

```
{
  'manifest': [
    {
      "name": "cell_suspension.json",
      "uuid": "48b7bdf2-410a-4d56-969c-e42e91d1f1fe",
      "version": "2019-02-12T224603.042173Z",
      "content-type": "application/json",
      "size": 22,
      "indexed": True,
      "crc32c": "e978e85d",
      "s3-etag": "89f6f8bec37ec1fc4560f3f99d47721d",
      "sha1": "58f03f7c6c0887baa54da85db5c820cfbe25d367",
      "sha256": "9b4c0dde8683f924975d0867903dc7a967f46bee5c0a025c451b9ba73e43f120"
    }
  ],
  'files': {
    'cell_suspension': [
      {
        "biomaterial_core": {
          "biomaterial_id": "Q4_DEMO-cellsus_SAMN02797092",
          "ncbi_taxon_id": [
            9606
          ]
        },
      },
      {
        "biomaterial_core": {
          "biomaterial_id": "some_alphanumeric_sequence",
          "ncbi_taxon_id": [
            9607,
            9609
          ]
        },
      },
    ]
  }
}
```

These filters will recieve a notifications

```
jmespath_query = "manifest[?name==`cell_suspension.json`].sha"
jmespath_query = "files.cell_suspension[].biomaterial_core.biomaterial_id"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9607`)"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9609`)"
```

These filters will NOT recieve a notificartion

```
jmespath_query = "manifest[?name==`dissociation_protocol_0.json.json`]"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9608`)"
```
