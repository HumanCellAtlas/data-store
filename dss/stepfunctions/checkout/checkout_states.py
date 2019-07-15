import logging

import dss
from dss.config import Replica
from dss.storage.checkout.error import PreExecCheckoutError

from dss.util.email import send_checkout_success_email, send_checkout_failure_email
from dss.storage.checkout import parallel_copy, pre_exec_validate, validate_file_dst
from dss.storage.checkout.bundle import (
    get_dst_bundle_prefix,
    get_manifest_files,
    mark_bundle_checkout_failed,
    mark_bundle_checkout_successful,
)
from .constants import EventConstants

logger = logging.getLogger(__name__)


class _InternalEventConstants:
    SCHEDULE = "schedule"
    """Key for dictionary that stores the output of schedule_copy."""

    SCHEDULED_FILES = "files_scheduled"
    SCHEDULED_DST_LOCATION = "dst_location"
    SCHEDULED_WAIT_TIME_SECONDS = "wait_time_seconds"

    VALIDATION = "validation"
    """Key for dictionary that stores the output of pre_execution_check."""
    VALIDATION_CHECKOUT_STATUS = "checkout_status"
    VALIDATION_CAUSE = "cause"
    VALIDATION_CHECKOUT_STATUS_PASSED = "PASSED"
    VALIDATION_CHECKOUT_STATUS_FAILED = "FAILED"

    RESULT = "result"


def schedule_copy(event, context):
    bundle_uuid = event[EventConstants.BUNDLE_UUID]
    bundle_version = event[EventConstants.BUNDLE_VERSION]
    dss_bucket = event[EventConstants.DSS_BUCKET]
    dst_bucket = get_dst_bucket(event)
    replica = Replica[event[EventConstants.REPLICA]]

    scheduled = 0
    for src_key, dst_key in get_manifest_files(replica, dss_bucket, bundle_uuid, bundle_version):
        logger.info("Schedule copying a file %s to bucket %s", dst_key, dss_bucket)
        parallel_copy(replica, dss_bucket, src_key, dst_bucket, dst_key)
        scheduled += 1
    return {_InternalEventConstants.SCHEDULED_FILES: scheduled,
            _InternalEventConstants.SCHEDULED_DST_LOCATION: get_dst_bundle_prefix(bundle_uuid, bundle_version),
            _InternalEventConstants.SCHEDULED_WAIT_TIME_SECONDS: 30}


def get_job_status(event, context):
    bundle_uuid = event[EventConstants.BUNDLE_UUID]
    bundle_version = event[EventConstants.BUNDLE_VERSION]
    dss_bucket = event[EventConstants.DSS_BUCKET]
    replica = Replica[event[EventConstants.REPLICA]]

    check_count = 0
    if EventConstants.STATUS in event:
        check_count = event[EventConstants.STATUS].get(EventConstants.STATUS_CHECK_COUNT, 0)

    complete_count = 0
    total_count = 0
    for src_key, dst_key in get_manifest_files(replica, dss_bucket, bundle_uuid, bundle_version):
        total_count += 1
        if validate_file_dst(replica, get_dst_bucket(event), dst_key):
            complete_count += 1

    checkout_status = "SUCCESS" if complete_count == total_count else "IN_PROGRESS"
    check_count += 1
    logger.info(
        "Check copy status for checkout jobId %s , check  count %d , status %s",
        event[EventConstants.EXECUTION_ID], check_count, checkout_status)
    return {
        EventConstants.STATUS_COMPLETE_COUNT: complete_count,
        EventConstants.STATUS_TOTAL_COUNT: total_count,
        EventConstants.STATUS_CHECK_COUNT: check_count,
        EventConstants.STATUS_OVERALL_STATUS: checkout_status
    }


def pre_execution_check(event, context):
    dst_bucket = get_dst_bucket(event)
    bundle_uuid = event[EventConstants.BUNDLE_UUID]
    bundle_version = event[EventConstants.BUNDLE_VERSION]
    dss_bucket = event[EventConstants.DSS_BUCKET]
    replica = Replica[event[EventConstants.REPLICA]]

    logger.info(
        "Pre-execution check job_id %s for bundle %s version %s replica %s",
        event[EventConstants.EXECUTION_ID], bundle_uuid, bundle_version, replica)

    try:
        pre_exec_validate(replica, dss_bucket, dst_bucket, bundle_uuid, bundle_version)
        result = {
            _InternalEventConstants.VALIDATION_CHECKOUT_STATUS:
                _InternalEventConstants.VALIDATION_CHECKOUT_STATUS_PASSED,
        }
    except PreExecCheckoutError as ex:
        result = {
            _InternalEventConstants.VALIDATION_CHECKOUT_STATUS:
                _InternalEventConstants.VALIDATION_CHECKOUT_STATUS_FAILED,
            _InternalEventConstants.VALIDATION_CAUSE:
                str(ex),
        }
    return result


def notify_complete(event, context):
    replica = Replica[event[EventConstants.REPLICA]]
    result = {}
    if EventConstants.EMAIL in event:
        result = send_checkout_success_email(
            dss.Config.get_notification_email(),
            event[EventConstants.EMAIL],
            get_dst_bucket(event),
            event[_InternalEventConstants.SCHEDULE][_InternalEventConstants.SCHEDULED_DST_LOCATION],
            replica)
    # record results of execution into S3
    mark_bundle_checkout_successful(
        event[EventConstants.EXECUTION_ID],
        replica,
        event[EventConstants.STATUS_BUCKET],
        get_dst_bucket(event),
        event[_InternalEventConstants.SCHEDULE][_InternalEventConstants.SCHEDULED_DST_LOCATION],
    )
    logger.info("Checkout completed successfully jobId %s", event[EventConstants.EXECUTION_ID])
    return {_InternalEventConstants.RESULT: result}


def notify_complete_failure(event, context):
    cause = "Unknown issue"
    result = {}
    if EventConstants.STATUS in event:
        cause = "failure to complete work within allowed time interval"
    elif _InternalEventConstants.VALIDATION in event:
        checkout_status = event[_InternalEventConstants.VALIDATION].get(
            _InternalEventConstants.VALIDATION_CHECKOUT_STATUS, "Unknown error code")
        cause = "{} ({})".format(
            event[_InternalEventConstants.VALIDATION].get(_InternalEventConstants.VALIDATION_CAUSE, "Unknown error"),
            checkout_status)
    if EventConstants.EMAIL in event:
        result = send_checkout_failure_email(dss.Config.get_notification_email(), event[EventConstants.EMAIL], cause)
    # record results of execution into S3
    mark_bundle_checkout_failed(
        event[EventConstants.EXECUTION_ID],
        Replica[event[EventConstants.REPLICA]],
        event[EventConstants.STATUS_BUCKET],
        cause,
    )
    logger.info("Checkout failed jobId %s", event[EventConstants.EXECUTION_ID])
    return {_InternalEventConstants.RESULT: result}


def get_dst_bucket(event):
    dst_bucket = event.get(EventConstants.DST_BUCKET, dss.Config.get_s3_checkout_bucket())
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
            "ResultPath": f"$.{_InternalEventConstants.VALIDATION}",
            "Next": "PreExecutionCheckPassed",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "PreExecutionCheckPassed": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable":
                        f"$.{_InternalEventConstants.VALIDATION}.{_InternalEventConstants.VALIDATION_CHECKOUT_STATUS}",
                    "StringEquals": f"{_InternalEventConstants.VALIDATION_CHECKOUT_STATUS_PASSED}",
                    "Next": "ScheduleCopy"
                }
            ],
            "Default": "NotifyFailure"
        },
        "ScheduleCopy": {
            "Type": "Task",
            "Resource": schedule_copy,
            "ResultPath": f"$.{_InternalEventConstants.SCHEDULE}",
            "Next": "Wait",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": f"$.{_InternalEventConstants.SCHEDULE}.wait_time_seconds",
            "Next": "GetJobStatus"
        },
        "GetJobStatus": {
            "Type": "Task",
            "Resource": get_job_status,
            "ResultPath": f"$.{EventConstants.STATUS}",
            "Next": "JobDone",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "JobDone": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": f"$.{EventConstants.STATUS}.{EventConstants.STATUS_OVERALL_STATUS}",
                    "StringEquals": "FAILURE",
                    "Next": "NotifyFailure"
                },
                {
                    "And": [
                        {
                            "Variable": f"$.{EventConstants.STATUS}.{EventConstants.STATUS_OVERALL_STATUS}",
                            "StringEquals": "IN_PROGRESS",
                        },
                        {
                            "Variable": f"$.{EventConstants.STATUS}.{EventConstants.STATUS_CHECK_COUNT}",
                            "NumericGreaterThan": 10,
                        }
                    ],
                    "Next": "NotifyFailure"
                },
                {
                    "Variable": f"$.{EventConstants.STATUS}.{EventConstants.STATUS_OVERALL_STATUS}",
                    "StringEquals": "IN_PROGRESS",
                    "Next": "Wait"
                },
                {
                    "Variable": f"$.{EventConstants.STATUS}.{EventConstants.STATUS_OVERALL_STATUS}",
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
