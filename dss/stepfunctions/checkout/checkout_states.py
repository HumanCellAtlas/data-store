import logging

import dss
from dss.config import Replica

from dss.util.email import send_checkout_success_email, send_checkout_failure_email
from dss.storage.checkout import (
    get_dst_bundle_prefix,
    get_manifest_files,
    parallel_copy,
    pre_exec_validate,
    put_status_failed,
    put_status_succeeded,
    validate_file_dst,
)

logger = logging.getLogger(__name__)


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


def notify_complete(event, context):
    replica = Replica[event["replica"]]
    result = {}
    if "email" in event:
        result = send_checkout_success_email(
            dss.Config.get_notification_email(),
            event["email"],
            get_dst_bucket(event),
            event["schedule"]["dst_location"],
            replica)
    # record results of execution into S3
    put_status_succeeded(event['execution_name'], replica, get_dst_bucket(event),
                         event["schedule"]["dst_location"])
    logger.info("Checkout completed successfully jobId %s", event['execution_name'])
    return {"result": result}


def notify_complete_failure(event, context):
    cause = "Unknown issue"
    result = {}
    if "status" in event:
        cause = "failure to complete work within allowed time interval"
    elif "validation" in event:
        checkout_status = event["validation"].get("checkout_status", "Unknown error code")
        cause = "{} ({})".format(event["validation"].get("cause", "Unknown error"), checkout_status)
    if "email" in event:
        result = send_checkout_failure_email(dss.Config.get_notification_email(), event["email"], cause)
    # record results of execution into S3
    put_status_failed(event['execution_name'], cause)
    logger.info("Checkout failed jobId %s", event['execution_name'])
    return {"result": result}


def get_dst_bucket(event):
    dst_bucket = event.get("bucket", dss.Config.get_s3_checkout_bucket())
    return dst_bucket


retry_config = [
    {
        "ErrorEquals": ["States.TaskFailed"],
        "IntervalSeconds": 5,
        "MaxAttempts": 5,
        "BackoffRate": 1.5
    },
    {
        "ErrorEquals": ["States.Timeout"],
        "IntervalSeconds": 30,
        "MaxAttempts": 3,
        "BackoffRate": 1.5
    },
    {
        "ErrorEquals": ["States.Permissions"],
        "MaxAttempts": 0
    },
    {
        "ErrorEquals": ["States.ALL"],
        "IntervalSeconds": 5,
        "MaxAttempts": 5,
        "BackoffRate": 2.0
    }
]

catch_config = [
    {
        "ErrorEquals": ["States.ALL"],
        "Next": "NotifyFailure"
    }
]

state_machine_def = {
    "Comment": "DSS checkout service state machine that submits a job to S3 copy client"
               " and monitors the Job until it completes.",
    "StartAt": "PreExecutionCheck",
    "TimeoutSeconds": 3600,             # 60 minutes, in seconds.
    "States": {
        "PreExecutionCheck": {
            "Type": "Task",
            "Resource": pre_execution_check,
            "ResultPath": "$.validation",
            "Next": "PreExecutionCheckPassed",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "PreExecutionCheckPassed": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.validation.checkout_status",
                    "StringEquals": "PASSED",
                    "Next": "ScheduleCopy"
                }
            ],
            "Default": "NotifyFailure"
        },
        "ScheduleCopy": {
            "Type": "Task",
            "Resource": schedule_copy,
            "ResultPath": "$.schedule",
            "Next": "Wait",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.schedule.wait_time_seconds",
            "Next": "GetJobStatus"
        },
        "GetJobStatus": {
            "Type": "Task",
            "Resource": get_job_status,
            "ResultPath": "$.status",
            "Next": "JobDone",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "JobDone": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.status.checkout_status",
                    "StringEquals": "FAILURE",
                    "Next": "NotifyFailure"
                },
                {
                    "And": [
                        {
                            "Variable": "$.status.checkout_status",
                            "StringEquals": "IN_PROGRESS",
                        },
                        {
                            "Variable": "$.status.check_count",
                            "NumericGreaterThan": 10,
                        }
                    ],
                    "Next": "NotifyFailure"
                },
                {
                    "Variable": "$.status.checkout_status",
                    "StringEquals": "IN_PROGRESS",
                    "Next": "Wait"
                },
                {
                    "Variable": "$.status.checkout_status",
                    "StringEquals": "SUCCESS",
                    "Next": "Notify"
                }
            ],
            "Default": "Wait"
        },
        "NotifyFailure": {
            "Type": "Task",
            "Resource": notify_complete_failure,
            "ResultPath": "$.emailFailure",
            "Next": "JobFailed"
        },
        "JobFailed": {
            "Type": "Fail",
            "Cause": "DSS Job Failed",
            "Error": "DSS Job returned FAILED"
        },
        "Notify": {
            "Type": "Task",
            "Resource": notify_complete,
            "ResultPath": "$.email",
            "End": True
        }
    }
}
