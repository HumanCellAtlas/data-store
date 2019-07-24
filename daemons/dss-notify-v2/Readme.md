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

Bundle creation notifications have the format

```
{
  'dss-api': dss_api
  'bundle_url': {dss_api}"/v1/bundles/{uuid}?version={version}
  'transaction_id': {uuid},
  'subscription_id': {uuid},
  'event_type': "CREATE"|"TOMBSTONE"|"DELETE",
  'event_timestamp': "timestamp"
  'match': {
    'bundle_uuid': {uuid},
    'bundle_version': {version},
  }
  'jmespath_query': {jmespath_query},
  'attachments': {
    "attachment_name_1": {value},
    "attachment_name_1": {value},
    ...
    "_errors": [...]
  }
}
```

The `jmespath_query` and `attachment` fields are subscription dependent and may not be present. Attachment
usage and format description can be found in the /subscription endpoint of the [DSS OpenAPI specifications](../../dss-api.yml).

The `event_timestamp` field is a timestamp for when the event was created, the formatting is in the `DSS_VERSION` format.

The `dss_api` field is the URL for 
## Subscriptions

Event subscriptions are managed via the `/subscriptions` API endpoint, described in detail by the
[DSS OpenAPI specification](../../dss-api.yml).

The subscription backend is AWS DynamoDB, used as a key-value store. The hash key is the email
of the subscription owner, and the sort key is the subscription uuid. There is one table per replica.

Subscriptions are accessed via owner for API actions. When notifications are triggered during an object storage event,
subscriptions are fetched from the backend via `scan`.

### Bundle Metadata Document

The bundle metadata document is constructed from json files present in the bundle manifest, and
includes the bundle manifest describe in the [DSS OpenAPI specifications](../../dss-api.yml). The structure
accommodates the possibility of multiple files sharing a name.

The bundle metadata document format for a new bundle or version is is

```
{
  'event_type': "CREATE",
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

For a tombstone it is
```
{
  'event_type': "TOMBSTONE",
  'admin_deleted': "true",
  'email': {admin_email},
  'reason': "the reason",
  'version': "the version",
}
```

For a deleted bundle it is
```
{
  'event_type': "DELETE",
  'uuid': {uuid}
  'version': {version},
}
```

### JMESPath Filtering

Subscriptions containing a non-empty `jmespath_query` will receive notifications when `jmespath_query` matches the
bundle metadata document.

#### Examples

To receive notifications only for tombstones, use

```
jmespath_query = "event_type==`TOMBSTONE`"
```

and for tombstones and deletes,

```
jmespath_query = "event_type==`TOMBSTONE` || event_type=`DELETE`"
```

To receive notifications for everything EXCEPT tombstones, use

```
jmespath_query = "event_type==`CREATE`"
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

These filters will receive a notifications

```
jmespath_query = "manifest[?name==`cell_suspension.json`].sha"
jmespath_query = "files.cell_suspension[].biomaterial_core.biomaterial_id"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9607`)"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9609`)"
```

These filters will NOT receive a notification

```
jmespath_query = "manifest[?name==`dissociation_protocol_0.json.json`]"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9608`)"
```
