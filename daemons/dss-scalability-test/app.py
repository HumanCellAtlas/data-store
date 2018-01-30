import json
import logging
import os
import tempfile
import uuid

import boto3
import time

import datetime
import domovoi
import sys

from hca.dss import DSSClient

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import stepfunctions, Config

AWS_MIN_CHUNK_SIZE = 64 * 1024 * 1024
WAIT_CHECKOUT = 3

app = domovoi.Domovoi()

state_machine_def = {
    "Comment": "DSS scalability test state machine.",
    "StartAt": "UploadBundle",
    "TimeoutSeconds": 600,  # 10 minutes, in seconds.
    "States": {
        "UploadBundle": {
            "Type": "Task",
            "Resource": None,
            "InputPath": "$",
            "ResultPath": "$.bundle",
            "OutputPath": "$",
            "Next": "DownloadBundle",
            "Catch": [{
                "ErrorEquals": ["States.TaskFailed"],
                "Next": "fallback"
            }]
        },
        "DownloadBundle": {
            "Type": "Task",
            "Resource": None,
            "InputPath": "$",
            "OutputPath": "$",
            "ResultPath": "$.download",
            "Next": "CheckoutBundle",
            "Catch": [{
                "ErrorEquals": ["States.TaskFailed"],
                "Next": "fallback"
            }]
        },
        "CheckoutBundle": {
            "Type": "Task",
            "Resource": None,
            "InputPath": "$",
            "ResultPath": "$.checkout",
            "OutputPath": "$",
            "Next": "Wait_Checkout",
            "Catch": [{
                "ErrorEquals": ["States.TaskFailed"],
                "Next": "fallback"
            }]
        },
        "Wait_Checkout": {
            "Type": "Wait",
            "Seconds": WAIT_CHECKOUT,
            "Next": "CheckoutDownloadStatus"
        },
        "CheckoutDownloadStatus": {
            "Type": "Task",
            "Resource": None,
            "InputPath": "$",
            "ResultPath": "$.checkout.status",
            "OutputPath": "$",
            "Next": "CompleteTest",
            "Catch": [{
                "ErrorEquals": ["States.TaskFailed"],
                "Next": "fallback"
            }]
        },
        "CompleteTest": {
            "Type": "Task",
            "Resource": None,
            "End": True,
        },
        "fallback": {
            "Type": "Task",
            "Resource": None,
            "End": True,
        }
    }
}

app.log.setLevel(logging.DEBUG)

test_bucket = os.environ["DSS_S3_CHECKOUT_BUCKET"]

os.environ["HOME"] = "/tmp"

os.environ["HCA_CONFIG_FILE"] = "/tmp/config.json"
with open(os.environ["HCA_CONFIG_FILE"], "w") as fh:
    fh.write(json.dumps({"DSSClient": {"swagger_url": "https://dss.dev.data.humancellatlas.org/v1/swagger.json"}}))

client = DSSClient()

dynamodb = boto3.resource('dynamodb')


def current_time():
    return int(round(time.time() * 1000))


@app.step_function_task(state_name="UploadBundle", state_machine_definition=state_machine_def)
def upload_bundle(event, context):
    app.log.info("Upload bundle")
    with tempfile.TemporaryDirectory() as src_dir:
        with tempfile.NamedTemporaryFile(dir=src_dir, suffix=".bin") as fh:
            fh.write(os.urandom(AWS_MIN_CHUNK_SIZE + 1))
            fh.flush()
            start_time = current_time()
            bundle_output = client.upload(src_dir=src_dir, replica="aws", staging_bucket=test_bucket)
            return {"bundle_id": bundle_output['bundle_uuid'], "start_time": start_time}


@app.step_function_task(state_name="DownloadBundle", state_machine_definition=state_machine_def)
def download_bundle(event, context):
    app.log.info("Download bundle")
    bundle_id = event['bundle']['bundle_id']
    with tempfile.TemporaryDirectory() as dest_dir:
        client.download(bundle_id, replica="aws", dest_name=dest_dir)
    return {}


@app.step_function_task(state_name="CheckoutBundle", state_machine_definition=state_machine_def)
def checkout_bundle(event, context):
    bundle_id = event['bundle']['bundle_id']
    app.log.info(f"Checkout bundle: {bundle_id}")
    checkout_output = client.post_bundles_checkout(uuid=bundle_id, replica='aws', email='rkisin@chanzuckerberg.com')
    return {"job_id": checkout_output['checkout_job_id']}


@app.step_function_task(state_name="CheckoutDownloadStatus", state_machine_definition=state_machine_def)
def checkout_status(event, context):
    job_id = event['checkout']['job_id']
    app.log.info(f"Checkout status job_id: {job_id}")
    # checkout_output = client.get_bundles_checkout(job_id)
    # return checkout_output['status']


@app.step_function_task(state_name="CompleteTest", state_machine_definition=state_machine_def)
def complete_test(event, context):
    save_results(event, 'SUCCEEDED')


@app.step_function_task(state_name="fallback", state_machine_definition=state_machine_def)
def fallback(event, context):
    save_results(event, 'FAILED')


def save_results(event, result: str):
    table = dynamodb.Table('scalability_test')
    start_time = event['bundle']['start_time']
    table.put_item(
        Item={
            'run_id': event["test_run_id"],
            'execution_id': event["execution_id"],
            'duration': current_time() - start_time - WAIT_CHECKOUT * 1000,
            'status': result,
            'created_on': datetime.datetime.now().isoformat()
        }
    )


@app.sns_topic_subscriber("dss-scalability-test-run-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def launch_test_run(event, context):
    print('Log test run')
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    run_id = msg["run_id"]
    table = dynamodb.Table('scalability_test_run')
    table.put_item(
        Item={
            'run_id': run_id,
            'executions': 0,
            'succeeded_count': 0,
            'failed_count': 0,
            'average_duration': 0,
            'created_on': datetime.datetime.now().isoformat()
        }
    )

@app.sns_topic_subscriber("dss-scalability-test-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def launch_exec(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    run_id = msg["run_id"]
    execution_id = msg["execution_id"]
    test_input = {"execution_id": execution_id, "test_run_id": run_id}
    stepfunctions.step_functions_invoke("dss-scalability-test-{stage}", execution_id, test_input)
