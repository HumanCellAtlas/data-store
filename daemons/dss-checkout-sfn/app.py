import logging
import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.config import Replica

from dss.stepfunctions.checkout.checkout_states import state_machine_def
from dss.util.email import send_checkout_success_email, send_checkout_failure_email
from dss.storage.checkout import (parallel_copy, get_dst_bundle_prefix, get_manifest_files,
                                  validate_file_dst, pre_exec_validate, put_status_succeeded, put_status_failed,
                                  put_status_started)

logger = logging.getLogger(__name__)

app = domovoi.Domovoi()

dss.Config.set_config(dss.BucketConfig.NORMAL)
email_sender = dss.Config.get_notification_email()
default_checkout_bucket = dss.Config.get_s3_checkout_bucket()


@app.step_function_task(state_name="ScheduleCopy", state_machine_definition=state_machine_def)
def schedule_copy(event, context):
    bundle_fqid = event["bundle"]
    version = event["version"]
    dss_bucket = event["dss_bucket"]
    dst_bucket = get_dst_bucket(event)
    replica = Replica[event["replica"]]

    scheduled = 0
    for src_key, dst_key in get_manifest_files(bundle_fqid, version, replica):
        logger.info("Schedule copying a file %s to bucket %s", dst_key, dss_bucket)
        parallel_copy(dss_bucket, src_key, dst_bucket, dst_key, replica)
        scheduled += 1
    return {"files_scheduled": scheduled,
            "dst_location": get_dst_bundle_prefix(bundle_fqid, version),
            "wait_time_seconds": 30}


@app.step_function_task(state_name="GetJobStatus", state_machine_definition=state_machine_def)
def get_job_status(event, context):
    bundle_fqid = event["bundle"]
    version = event["version"]
    replica = Replica[event["replica"]]

    check_count = 0
    if "status" in event:
        check_count = event["status"].get("check_count", 0)

    complete_count = 0
    total_count = 0
    for src_key, dst_key in get_manifest_files(bundle_fqid, version, replica):
        total_count += 1
        if validate_file_dst(get_dst_bucket(event), dst_key, replica):
            complete_count += 1

    checkout_status = "SUCCESS" if complete_count == total_count else "IN_PROGRESS"
    check_count += 1
    logger.info("Check copy status for checkout jobId %s , check  count %d , status %s", event['execution_name'],
                check_count, checkout_status)
    return {"complete_count": complete_count, "total_count": total_count, "check_count": check_count,
            "checkout_status": checkout_status}


@app.step_function_task(state_name="PreExecutionCheck", state_machine_definition=state_machine_def)
def pre_execution_check(event, context):
    dst_bucket = get_dst_bucket(event)
    bundle = event["bundle"]
    version = event["version"]
    dss_bucket = event["dss_bucket"]
    replica = Replica[event["replica"]]

    logger.info("Pre-execution check job_id %s for bundle %s version %s replica %s", event['execution_name'],
                bundle, version, replica)

    checkout_status, cause = pre_exec_validate(dss_bucket, dst_bucket, replica, bundle, version)
    result = {"checkout_status": checkout_status.name.upper()}
    if cause:
        result["cause"] = cause
    return result


@app.step_function_task(state_name="Notify", state_machine_definition=state_machine_def)
def notify_complete(event, context):
    replica = Replica[event["replica"]]
    result = {}
    if "email" in event:
        result = send_checkout_success_email(email_sender, event["email"], get_dst_bucket(event),
                                             event["schedule"]["dst_location"], replica)
    # record results of execution into S3
    put_status_succeeded(event['execution_name'], replica, get_dst_bucket(event),
                         event["schedule"]["dst_location"])
    logger.info("Checkout completed successfully jobId %s", event['execution_name'])
    return {"result": result}


@app.step_function_task(state_name="NotifyFailure", state_machine_definition=state_machine_def)
def notify_complete_failure(event, context):
    cause = "Unknown issue"
    result = {}
    if "status" in event:
        cause = "failure to complete work within allowed time interval"
    elif "validation" in event:
        checkout_status = event["validation"].get("checkout_status", "Unknown error code")
        cause = "{} ({})".format(event["validation"].get("cause", "Unknown error"), checkout_status)
    if "email" in event:
        result = send_checkout_failure_email(email_sender, event["email"], cause)
    # record results of execution into S3
    put_status_failed(event['execution_name'], cause)
    logger.info("Checkout failed jobId %s", event['execution_name'])
    return {"result": result}


def get_dst_bucket(event):
    dst_bucket = event.get("bucket", default_checkout_bucket)
    return dst_bucket
