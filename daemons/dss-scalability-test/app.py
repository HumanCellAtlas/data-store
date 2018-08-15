import datetime
import json
import logging
import os
import sys
import tempfile
import time
from decimal import Decimal

import boto3
import domovoi


pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import stepfunctions, Config, BucketConfig
from dss.api.files import ASYNC_COPY_THRESHOLD
from json_generator import generate_sample
from dss.logging import configure_lambda_logging

#: Wait in seconds begore performing another checkout readiness check
WAIT_CHECKOUT = 10

#: Number of parallel execution branches within the scale test step function
PARALLELIZATION_FACTOR = 10

app = domovoi.Domovoi(configure_logs=False)

logger = logging.getLogger(__name__)
configure_lambda_logging()

test_bucket = os.environ["DSS_S3_CHECKOUT_BUCKET"]

os.environ["HOME"] = "/tmp"
os.environ["HCA_CONFIG_FILE"] = "/tmp/config.json"

with open(os.environ["HCA_CONFIG_FILE"], "w") as fh:
    fh.write(json.dumps({"DSSClient": {"swagger_url": f"https://{os.environ['API_DOMAIN_NAME']}/v1/swagger.json"}}))

client = None
def get_client():
    global client
    if client is None:
        from hca.dss import DSSClient
        client = DSSClient()
    return client

dynamodb = boto3.resource('dynamodb')
Config.set_config(BucketConfig.NORMAL)


def current_time():
    return int(round(time.time() * 1000))

def upload_bundle(event, context):
    logger.info("Start uploading bundle")
    with tempfile.TemporaryDirectory() as src_dir:
        with tempfile.NamedTemporaryFile(dir=src_dir, suffix=".json", delete=False) as jfh:
            jfh.write(bytes(generate_sample(), 'UTF-8'))
            jfh.flush()
        with tempfile.NamedTemporaryFile(dir=src_dir, suffix=".bin") as fh:
            fh.write(os.urandom(ASYNC_COPY_THRESHOLD + 1))
            fh.flush()
            start_time = current_time()
            bundle_output = get_client().upload(src_dir=src_dir, replica="aws", staging_bucket=test_bucket)
            logger.debug("Bundle: %s", bundle_output['bundle_uuid'])
            return {"bundle_id": bundle_output['bundle_uuid'], "start_time": start_time}


def download_bundle(event, context):
    logger.debug("Download bundle")
    bundle_id = event['bundle']['bundle_id']
    with tempfile.TemporaryDirectory() as dest_dir:
        get_client().download(bundle_id, replica="aws", dest_name=dest_dir)
    return {}


def checkout_bundle(event, context):
    bundle_id = event['bundle']['bundle_id']
    logger.info("Checkout bundle: %s", bundle_id)
    checkout_output = get_client().post_bundles_checkout(uuid=bundle_id, replica='aws', email='foo@example.com')
    return {"job_id": checkout_output['checkout_job_id']}


def checkout_status(event, context):
    job_id = event['checkout']['job_id']
    logger.info("Checkout status job_id: %s", job_id)
    checkout_output = get_client().get_bundles_checkout(checkout_job_id=job_id)
    logger.debug("Checkout status %s", str(checkout_output))
    return {"status": checkout_output['status']}


def complete_test(event, context):
    save_results(event, 'SUCCEEDED')


def fallback(event, context):
    return {"failed": "failed"}


def save_results(event, result: str):
    table = dynamodb.Table('scalability_test')
    start_time = None

    expiration_ttl = int(time.time()) + 14 * 24 * 60 * 60  # 14 days

    fail_count = 0
    success_count = 0
    run_id = None
    execution_id = ''
    for branch_event in event['tests']:
        if branch_event.get('failed'):
            fail_count += 1
        else:
            success_count += 1

        if run_id is None and "test_run_id" in branch_event:
            run_id = branch_event["test_run_id"]
            execution_id = branch_event["execution_id"]
            start_time = branch_event['bundle']['start_time']

    table.put_item(
        Item={
            'run_id': run_id,
            'execution_id': execution_id,
            'duration': current_time() - start_time - WAIT_CHECKOUT * 1000,
            'success_count': success_count,
            'fail_count': fail_count,
            'created_on': datetime.datetime.now().isoformat(),
            'expiration_ttl': expiration_ttl
        }
    )


@app.sns_topic_subscriber("dss-scalability-test-run-" + os.environ["DSS_DEPLOYMENT_STAGE"])
def launch_test_run(event, context):
    msg = json.loads(event["Records"][0]["Sns"]["Message"])
    table = dynamodb.Table('scalability_test_result')
    table.put_item(
        Item={
            'run_id': msg["run_id"],
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
    nextBatch = roundTime()
    test_input = {
        "execution_id": execution_id,
        "test_run_id": run_id,
        "batch": nextBatch.isoformat() + 'Z'
    }
    logger.debug("Starting execution %s", execution_id)
    stepfunctions.step_functions_invoke("dss-scalability-test-{stage}", execution_id, test_input)


@app.dynamodb_stream_handler(table_name="scalability_test", batch_size=5)
def handle_dynamodb_stream(event, context):
    success_count = 0
    failure_count = 0
    records = 0
    duration_sum = 0
    run_id = None
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            run_id = record['dynamodb']['NewImage']['run_id']['S']
            success_count_rec = int(record['dynamodb']['NewImage']['success_count']['N'])
            fail_count_rec = int(record['dynamodb']['NewImage']['fail_count']['N'])

            duration = record['dynamodb']['NewImage']['duration']['N']
            duration_sum += Decimal(duration)
            records += 1
            success_count += success_count_rec
            failure_count += fail_count_rec
    logger.debug("Success_count_rec: %d", success_count)
    if records > 0:
        table = dynamodb.Table('scalability_test_result')
        run_entry_pk = {'run_id': run_id}
        run_entry = table.get_item(Key=run_entry_pk)
        if run_entry.get('Item'):
            old_avg_duration = run_entry['Item']['average_duration']
            old_count = run_entry['Item']['succeeded_count'] + run_entry['Item']['failed_count']

            table.update_item(
                Key=run_entry_pk,
                UpdateExpression='SET '
                                 'succeeded_count = succeeded_count + :success_count, '
                                 'failed_count = failed_count + :failure_count, '
                                 'executions = executions + :records, '
                                 'average_duration = :new_average',
                ExpressionAttributeValues={
                    ':success_count': success_count,
                    ':failure_count': failure_count,
                    ':records': records,
                    ':new_average': old_avg_duration + (duration_sum / records - old_avg_duration) /
                                                       (old_count + records)
                }
            )
        else:
            logger.debug('No run entries found')
    else:
        logger.debug('No INSERT records to process')

    return 'Successfully processed {} records.'.format(len(event['Records']))


def roundTime(to=datetime.timedelta(minutes=5)):
    roundTo = to.total_seconds()
    now = datetime.datetime.now()
    seconds = (now - now.min).seconds
    rounding = (seconds + roundTo / 2) // roundTo * roundTo
    return now + datetime.timedelta(0, rounding - seconds, -now.microsecond)


def exec_branch_def(tid):
    return {
        "StartAt": f"UploadBundle{tid}",
        "States": {

            f"UploadBundle{tid}": {
                "Type": "Task",
                "Resource": upload_bundle,
                "InputPath": "$",
                "ResultPath": "$.bundle",
                "OutputPath": "$",
                "Next": f"DownloadBundle{tid}",
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": f"fallback{tid}"
                }]
            },
            f"DownloadBundle{tid}": {
                "Type": "Task",
                "Resource": download_bundle,
                "InputPath": "$",
                "OutputPath": "$",
                "ResultPath": "$.download",
                "Next": f"CheckoutBundle{tid}",
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": f"fallback{tid}"
                }]
            },
            f"CheckoutBundle{tid}": {
                "Type": "Task",
                "Resource": checkout_bundle,
                "InputPath": "$",
                "ResultPath": "$.checkout",
                "OutputPath": "$",
                "Next": f"Wait_Checkout{tid}",
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": f"fallback{tid}"
                }]
            },
            f"Wait_Checkout{tid}": {
                "Type": "Wait",
                "Seconds": WAIT_CHECKOUT,
                "Next": f"CheckoutDownloadStatus{tid}"
            },
            f"CheckoutDownloadStatus{tid}": {
                "Type": "Task",
                "Resource": checkout_status,
                "InputPath": "$",
                "ResultPath": "$.checkout.status",
                "OutputPath": "$",
                "Next": f"CheckStatus{tid}",
                "Catch": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": f"fallback{tid}"
                }]
            },
            f"CheckStatus{tid}": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.checkout.status.status",
                        "StringEquals": "SUCCEEDED",
                        "Next": f"Done{tid}"
                    },
                    {
                        "Variable": "$.checkout.status.status",
                        "StringEquals": "RUNNING",
                        "Next": f"Wait_Checkout{tid}"
                    },
                ],
                "Default": f"fallback{tid}"
            },
            f"Done{tid}": {
                "Type": "Pass",
                "End": True
            },
            f"fallback{tid}": {
                "Type": "Task",
                "Resource": fallback,
                "InputPath": "$",
                "ResultPath": "$.failed",
                "OutputPath": "$",
                "End": True,
            }
        }
    }


def state_machine_def():
    return {
        "Comment": "DSS scalability test state machine.",
        "StartAt": "WaitUntil",
        "TimeoutSeconds": 3600,
        "States": {
            "WaitUntil": {
                "Type": "Wait",
                "TimestampPath": "$.batch",
                "Next": "Executors"
            },

            "Executors": {
                "Type": "Parallel",
                "Branches": [exec_branch_def(tid) for tid in range(PARALLELIZATION_FACTOR)],
                "InputPath": "$",
                "ResultPath": "$.tests",
                "OutputPath": "$",
                "Next": "CompleteTest",
            },

            "CompleteTest": {
                "Type": "Task",
                "Resource": complete_test,
                "End": True,
            }
        }
    }


app.register_state_machine(state_machine_def())
