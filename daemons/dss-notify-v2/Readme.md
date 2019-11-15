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
after 15 minutes, and again every hour for 7 days. Delivery delay and duration is 
configured via SQS attribute DelaySeconds.

Event handlers in the dss-notify-v2 daemon use utility functions in
[dss.events.handlers.notify_v2](../../dss/events/handlers/notify_v2.py).

### Payload Format

Bundle creation notifications have the format

```
{
  "dss_api": $DSS_API_URL,
  "bundle_url": "$DSS_API_URL/v1/bundles/$UUID?version=$VERSION",
  "transaction_id": $UUID,
  "subscription_id": $UUID,
  "event_type": "CREATE"|"TOMBSTONE"|"DELETE",
  "event_timestamp": "timestamp"
  "match": {
    "bundle_uuid": $UUID,
    "bundle_version": $VERSION,
  }
  "jmespath_query": $JMESPATH_QUERY,
  "attachments": {
    "attachment_name_1": $ATTACHMENT_VALUE_1,
    "attachment_name_2": $ATTACHMENT_VALUE_2,
    ...
    "_errors": [...]
  }
}
```

The `jmespath_query` and `attachment` fields are subscription dependent and may not be present. Attachment
usage and format description can be found in the `/subscriptions` endpoint of the [DSS OpenAPI specifications](../../dss-api.yml).

The `event_timestamp` field is the event creation date in the `DSS_VERSION` format.

## Subscriptions

Event subscriptions are managed via the `/subscriptions` API endpoint, described in detail by the
[DSS OpenAPI specification](../../dss-api.yml).

The subscription backend is AWS DynamoDB, used as a key-value store. The hash key is the email of the subscription
owner, and the sort key is the subscription uuid. There is one table per replica.

Subscriptions are accessed via owner for API actions. When notifications are triggered during an object storage
event, subscriptions are fetched from the backend via `scan`.

If notification delivery fails, a notification record is made in the queue. Delivery will be attempted 15 minutes 
after failure, and then every 6 hours over the course of 7 days.

### Bundle Metadata Document

The bundle metadata document is constructed from JSON files present in the bundle manifest, and includes the bundle
manifest described in the [DSS OpenAPI specifications](../../dss-api.yml). The structure accommodates the
possibility of multiple files sharing a name.

The bundle metadata document format for a new bundle or version is

```
{
  "event_type": "CREATE",
  "bundle_info": {
    "uuid": "48b7bdf2-410a-4d56-969c-e42e91d1f1fe",
    "version": "2019-02-12T224603.042173Z"
  },
  "manifest": {
    "version": ...,
    "files": [
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
    ],
  },
  "files": {
    $file_name_A: [
      $json_contents,
      ...
    ],
    $file_name_B: [
      $json_contents,
      ...
    ],
    ...
  }
}
```

(Note: when the bundle metadata document is being assembled, file names are passed through a function
[`_dot_to_underscore_and_strip_numeric_suffix()`](https://github.com/HumanCellAtlas/data-store/blob/573e7ac028b119fcdb25dda488ffbb6d0e33ba0e/dss/events/__init__.py#L118-L129)
that transforms the filename by replacing dots `.` with underscores `_` and by stripping any numerical suffix
(e.g., the filename `library_preparation_protocol_0.json` becomes `library_preparation_protocol_json`).

For a tombstone it is

```
{
  "event_type": "TOMBSTONE",
  "bundle_info": {
    "uuid": $uuid,
    "version": $version,
  },
  "admin_deleted": "true",
  "email": $admin_email,
  "reason": "the reason",
  "version": "the version",
}
```

For a deleted bundle it is

```
{
  "event_type": "DELETE",
  "bundle_info": {
    "uuid": $uuid,
    "version": $version,
  },
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

As another example, for the following bundle metadata document

```
{
  "manifest": [
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
  "files": {
    "cell_suspension_json": [
      {
        "biomaterial_core": {
          "biomaterial_id": "Q4_DEMO-cellsus_SAMN02797092",
          "ncbi_taxon_id": [
            9606
          ]
        }
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
      ...
    ]
  }
}
```

The following filters would receive notifications:

```
jmespath_query = "manifest[?name==`cell_suspension.json`].sha"
jmespath_query = "files.cell_suspension[].biomaterial_core.biomaterial_id"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9607`)"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9609`)"
```

and the following filters would NOT receive notifications:

```
jmespath_query = "manifest[?name==`dissociation_protocol_0.json`]"
jmespath_query = "files.cell_suspension[].biomaterial_core.ncbi_taxon_id[] | contains(@, `9608`)"
```
