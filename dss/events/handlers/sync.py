import os
import sys
import datetime
import json
import time
import hashlib
from contextlib import closing

import boto3
import botocore.session
import urllib3
import google.cloud.storage
from google.cloud.client import ClientWithProject
from google.cloud._http import JSONConnection

gcs_presigned_url_lifetime_seconds = 3600
gcsts_sched_delay_minutes = 2

class GCStorageTransferClient(ClientWithProject):
    SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]

class GCStorageTransferConnection(JSONConnection):
    API_BASE_URL = "https://storagetransfer.googleapis.com"
    API_VERSION = "v1"
    API_URL_TEMPLATE = "{api_base_url}/{api_version}{path}"

# TODO akislyuk: access keys used here should be separate role credentials with need-based access
# TODO akislyuk: schedule a lambda to check the status of the job, get it permissions to execute:
#                storagetransfer.transferJobs().get(jobName=gcsts_job["name"]).execute()
# TODO akislyuk: parallelize S3->GCS transfers with range request lambdas
def sync_blob(source_platform, source_key, dest_platform, logger):
    logger.info("Begin transfer of {} from {} to {}".format(source_key, source_platform, dest_platform))
    gcs = google.cloud.storage.Client()
    gcs_bucket_name, s3_bucket_name = os.environ["DSS_GCS_TEST_BUCKET"], os.environ["DSS_S3_TEST_BUCKET"]
    if source_platform == "s3" and dest_platform == "gcs":
        gcsts_client = GCStorageTransferClient()
        gcsts_conn = GCStorageTransferConnection(client=gcsts_client)
        now = datetime.datetime.utcnow()
        schedule_at = now + datetime.timedelta(minutes=gcsts_sched_delay_minutes)
        schedule_struct = dict(year=schedule_at.year, month=schedule_at.month, day=schedule_at.day)
        gcsts_job_def = {
            "description": "hca-dss-{}-{}".format(int(now.timestamp()), hashlib.md5(source_key.encode()).hexdigest()),
            "status": "ENABLED",
            "projectId": gcs.project,
            "schedule": {
                "scheduleStartDate": schedule_struct,
                "scheduleEndDate": schedule_struct,
                "startTimeOfDay": dict(hours=schedule_at.hour, minutes=schedule_at.minute)
            },
            "transferSpec": {
                "awsS3DataSource": {
                    "bucketName": s3_bucket_name,
                    "awsAccessKey": {
                        "accessKeyId": botocore.session.get_session().get_credentials().access_key,
                        "secretAccessKey": botocore.session.get_session().get_credentials().secret_key
                    }
                },
                "gcsDataSink": {
                    "bucketName": gcs_bucket_name
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": False,
                    "deleteObjectsUniqueInSink": False,
                    "deleteObjectsFromSourceAfterTransfer": False,
                },
                "objectConditions": {
                    "includePrefixes": [source_key]
                }
            }
        }
        try:
            gcsts_job = gcsts_conn.api_request("POST", "/transferJobs", data=gcsts_job_def)
            logger.info(gcsts_job)
        except Exception as e:
            logger.error("FIXME: (akislyuk) GCSTS job submission failed: {}".format(e))
        # FIXME akislyuk: the service account doesn't have permission to look at the
        # status of the job, even though it has permission to create it.  I
        # couldn't figure out what permission scope to give the principal in the
        # IAM console, and the service definition at
        # https://storagetransfer.googleapis.com/$discovery/rest?version=v1
        # doesn't tell me either.
        # gcsts_job = gcsts_conn.api_request("GET", "/" + gcsts_job["name"])
    elif source_platform == "gcs" and dest_platform == "s3":
        s3 = boto3.resource("s3")
        gcs_blob = gcs.get_bucket(gcs_bucket_name).blob(source_key)
        gcs_blob_url = gcs_blob.generate_signed_url(expiration=int(time.time() + gcs_presigned_url_lifetime_seconds))
        http = urllib3.PoolManager(cert_reqs="CERT_REQUIRED")
        with closing(http.request("GET", gcs_blob_url, preload_content=False)) as fh:
            s3.Bucket(s3_bucket_name).Object(source_key).upload_fileobj(fh)
    else:
        raise NotImplementedError()
