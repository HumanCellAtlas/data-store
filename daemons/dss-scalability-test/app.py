import json
import logging
import os
import tempfile

import domovoi
from hca.dss import DSSClient

AWS_MIN_CHUNK_SIZE = 64 * 1024 * 1024

app = domovoi.Domovoi()

state_machine_def = {
    "Comment": "DSS scalability test state machine.",
    "StartAt": "UploadBundle",
    "TimeoutSeconds": 600,  # 10 minutes, in seconds.
    "States": {
        "UploadBundle": {
            "Type": "Task",
            "Resource": None,
            "Next": "DownloadBundle"
        },
        "DownloadBundle": {
            "Type": "Task",
            "Resource": None,
            "Next": "CheckoutBundle"
        },
        "CheckoutBundle": {
            "Type": "Task",
            "Resource": None,
            "Next": "Wait_Checkout",
            "ResultPath": "$.checkout"
        },
        "Wait_Checkout": {
            "Type": "Wait",
            "Seconds": 3,
            "Next": "CheckoutDownloadStatus"
        },
        "CheckoutDownloadStatus": {
            "Type": "Task",
            "Resource": None,
            "InputPath": "$.checkout",
            "ResultPath": "$.checkout.status",
            "End": True,
        },
    }
}

app.log.setLevel(logging.DEBUG)

test_bucket = os.environ["DSS_S3_CHECKOUT_BUCKET"]

os.environ["HOME"] = "/tmp"

os.environ["HCA_CONFIG_FILE"] = "/tmp/config.json"
with open(os.environ["HCA_CONFIG_FILE"], "w") as fh:
    fh.write(json.dumps({"DSSClient": {"swagger_url": "https://dss.dev.data.humancellatlas.org/v1/swagger.json"}}))

client = DSSClient()


@app.step_function_task(state_name="UploadBundle", state_machine_definition=state_machine_def)
def upload_bundle(event, context):
    app.log.info("Upload bundle")
    with tempfile.TemporaryDirectory() as src_dir:
        with tempfile.NamedTemporaryFile(dir=src_dir, suffix=".bin") as fh:
            fh.write(os.urandom(AWS_MIN_CHUNK_SIZE + 1))
            fh.flush()
            bundle_output = client.upload(src_dir=src_dir, replica="aws", staging_bucket=test_bucket)
            return {"bundle_id": bundle_output['bundle_uuid']}


@app.step_function_task(state_name="DownloadBundle", state_machine_definition=state_machine_def)
def download_bundle(event, context):
    app.log.info("Download bundle")
    bundle_id = event['bundle_id']
    with tempfile.TemporaryDirectory() as dest_dir:
        client.download(bundle_id, replica="aws", dest_name=dest_dir)
    return {"bundle_id": bundle_id}


@app.step_function_task(state_name="CheckoutBundle", state_machine_definition=state_machine_def)
def checkout_bundle(event, context):
    bundle_id = event['bundle_id']
    app.log.info(f"Checkout bundle: {bundle_id}")
    checkout_output = client.post_bundles_checkout(uuid=bundle_id, replica='aws', email='rkisin@chanzuckerberg.com')
    return {"job_id": checkout_output['checkout_job_id']}

@app.step_function_task(state_name="CheckoutDownloadStatus", state_machine_definition=state_machine_def)
def checkout_bundle(event, context):
    job_id = event['job_id']
    app.log.info(f"Checkout status job_id: {job_id}")
    #checkout_output = client.get_bundles_checkout(job_id)
    #return checkout_output['status']
