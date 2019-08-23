#### Activating Google Cloud APIs

```
gcloud service-management enable cloudfunctions.googleapis.com
gcloud service-management enable runtimeconfig.googleapis.com
```

#### Retrieving GCF logs

```
gcloud beta functions logs read dss-gs-event-relay
```

#### Example Google Storage event

```
{
  "timestamp": "2017-08-25T23:08:25.270Z",
  "eventType": "providers/cloud.storage/eventTypes/object.change",
  "resource": "projects/_/buckets/GS_BUCKET/objects/GS_BLOB_KEY#1503702505270802",
  "data": {
    "kind": "storage#object",
    "resourceState": "exists",
    "id": "GS_BUCKET/GS_BLOB_KEY/1503702505270802",
    "selfLink": "https://www.googleapis.com/storage/v1/b/GS_BUCKET/o/GS_BLOB_KEY",
    "name": "GS_BLOB_KEY",
    "bucket": "GS_BUCKET",
    "generation": "1503702505270802",
    "metageneration": "1",
    "contentType": "application/octet-stream",
    "timeCreated": "2017-08-25T23:08:25.245Z",
    "updated": "2017-08-25T23:08:25.245Z",
    "storageClass": "REGIONAL",
    "size": "1130",
    "md5Hash": "ZDllMDNlNjA0YzZiNjI4NWMxN2NlY2YxZDM4NWE3YzE=",
    "mediaLink": "https://www.googleapis.com/download/storage/v1/b/GS_BUCKET/o/GS_BLOB_KEY?generation=1503702505270802&alt=media",
    "crc32c": "yHkaXA=="
  }
}
```

#### Environment variables in the GCF container

```
{
  "WORKER_PORT": "8091",
  "GCLOUD_PROJECT": "PROJECT_NAME",
  "FUNCTION_NAME": "dss_gs_event_relay",
  "SUPERVISOR_HOSTNAME": "192.168.1.1",
  "PATH": "/usr/local/bin:/usr/bin:/bin",
  "PWD": "/user_code",
  "FUNCTION_TRIGGER_TYPE": "CLOUD_STORAGE_TRIGGER",
  "NODE_ENV": "production",
  "SHLVL": "1",
  "CODE_LOCATION": "/user_code",
  "FUNCTION_MEMORY_MB": "256",
  "GCP_PROJECT": "PROJECT_NAME",
  "PORT": "8080",
  "SUPERVISOR_INTERNAL_PORT": "8081",
  "ENTRY_POINT": "dss_gs_event_relay",
  "OLDPWD": "/var/tmp/worker",
  "_": "/usr/bin/env",
  "HOME": "/tmp"
}
```
