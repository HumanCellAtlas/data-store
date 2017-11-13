import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss.util.state_machine.checkout_states import definition
from dss.util.email import send_checkout_success_email, send_checkout_failure_email
from dss.util.checkout import parallel_copy, get_dst_bundle_prefix, get_manifest_files, \
    validate_file_dst, validate

app = domovoi.Domovoi()
dss.Config.set_config(dss.BucketConfig.NORMAL)

replica = "aws"

log = dss.get_logger()
dss_bucket = dss.Config.get_s3_bucket()
email_sender = dss.Config.get_notification_email()
default_checkout_bucket = dss.Config.get_s3_hca_checkout_bucket()

@app.step_function_task(state_name="ScheduleCopy", state_machine_definition=definition)
def worker(event, context):
    bundle_id = event["bundle"]
    version = event["version"]
    src_bucket = dss_bucket
    dst_bucket = get_bucket(event)

    scheduled = 0
    for src_key, dst_key in get_manifest_files(bundle_id, version, replica):
        log.debug("Copying a file " + dst_key)
        parallel_copy(src_bucket, src_key, dst_bucket, dst_key)
        scheduled += 1
    return {"files_scheduled": scheduled, "dst_location": get_dst_bundle_prefix(bundle_id, version), "wait_time": 5}


@app.step_function_task(state_name="GetJobStatus", state_machine_definition=definition)
def get_job_status(event, context):
    bundle_id = event["bundle"]
    version = event["version"]

    check_count = 0
    if "status" in event:
        check_count = event["status"].get("check_count", 0)

    complete_count = 0
    total_count = 0
    for src_key, dst_key in get_manifest_files(bundle_id, version, replica):
        total_count += 1
        if (validate_file_dst(get_bucket(event), dst_key, replica)):
            complete_count += 1

    code = "SUCCESS" if complete_count == total_count else "IN_PROGRESS"
    check_count += 1
    return {"complete_count": complete_count, "total_count": total_count, "check_count": check_count, "code": code}


@app.step_function_task(state_name="SanityCheck", state_machine_definition=definition)
def sanity_check(event, context):
    dst_bucket = get_bucket(event)
    bundle = event["bundle"]
    version = event["version"]
    code, cause = validate(dst_bucket, replica, bundle, version)
    result = {"code": code.name.upper()}
    if cause:
        result["cause"] = cause
    return result


@app.step_function_task(state_name="Notify", state_machine_definition=definition)
def notify_complete(event, context):
    result = send_checkout_success_email(email_sender, event["email"], get_bucket(event),
                                         event["schedule"]["dst_location"])
    return {"result": result}


@app.step_function_task(state_name="NotifyFailure", state_machine_definition=definition)
def notify_complete_failure(event, context):
    cause = "Unknown issue"
    if "status" in event:
        cause = "failure to complete work within allowed time interval"
    elif "validation" in event:
        code = event["validation"].get("code", "Unknown error code")
        cause = "{} ({})".format(event["validation"].get("cause", "Unknown error"), code)
    result = send_checkout_failure_email(email_sender, event["email"], cause)
    return {"result": result}


def get_bucket(event):
    return event.get("bucket", default_checkout_bucket)
