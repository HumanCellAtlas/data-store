import sys

import domovoi
import os


pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa

sys.path.insert(0, pkg_root)

import dss
from dss.api.bundles.helpers import CopyMode
from dss.util.state_machine.checkout_states import definition
from dss.util.email import send_checkout_success_email, send_checkout_failure_email

from dss.util.checkout import parallel_copy, get_dst_bundle_prefix, get_manifest_files, \
    validate_file_dst, validate_dst_bucket

app = domovoi.Domovoi()
dss.Config.set_config(dss.BucketConfig.NORMAL)

test_bucket = "org-humancellatlas-dss-dev"
HCA_HOSTED_CHECKOUT_BUCKET =  "org-humancellatlas-dss-dev"

replica = "aws"

log = dss.get_logger()

@app.step_function_task(state_name="ScheduleCopy", state_machine_definition=definition)
def worker(event, context):
    bundle_id = event["bundle_id"]
    version = event["version"]
    src_bucket = test_bucket
    dst_bucket = get_bucket(event)
#    handle, hca_handle, src_bucket = Config.get_cloud_specific_handles(replica)

    scheduled = 0
    skipped = 0

    for src_key, dst_key in get_manifest_files(bundle_id, version, replica):
        #copy_mode = get_copy_mode(handle, file, src_bucket, src_object_name, dst_bucket, dst_object_name, replica)
        copy_mode = CopyMode.COPY_ASYNC
        if copy_mode == CopyMode.NO_COPY:
            log.debug("File already exists - no copy is necessary " + dst_key)
            skipped += 1
            continue
        else:
            log.debug("Copying a file " + dst_key)
            parallel_copy(src_bucket, src_key, dst_bucket, dst_key)
            scheduled += 1
    return {"files_scheduled": scheduled, "files_skipped": skipped, "dst_location": get_dst_bundle_prefix(bundle_id, version),"wait_time": 5}

@app.step_function_task(state_name="GetJobStatus", state_machine_definition=definition)
def get_job_status(event, context):
    bundle_id = event["bundle_id"]
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
    code, cause = validate_dst_bucket(dst_bucket, replica)
#    return {"src_bucket_region": get_bucket_region(test_bucket), "dst_bucket_region": get_bucket_region(dst_bucket), "code": code.name.upper()}
    result = {"code": code.name.upper()}
    if cause:
        result["cause"] = cause
    return result

@app.step_function_task(state_name="Notify", state_machine_definition=definition)
def notify_complete(event, context):
    sender = "Roman Kisin <rkisin@chanzuckerberg.com>"
    result = send_checkout_success_email(sender, event["requester_email"], get_bucket(event), event["schedule"]["dst_location"])
    return {"result": result}


@app.step_function_task(state_name="NotifyFailure", state_machine_definition=definition)
def notify_complete_failure(event, context):
    sender = "Roman Kisin <rkisin@chanzuckerberg.com>"

    cause = "Unknown issue"
    if "status" in event:
        cause = "failure to complete work within allowed time interval"
    elif "validation" in event:
        code = event["validation"].get("code", "Unknown error code")
        cause = "{} ({})".format(event["validation"].get("cause", "Unknown error"), code)
    result = send_checkout_failure_email(sender, event["requester_email"], cause)
    return {"result": result}



def get_bucket(event):
    return event.get("bucket", HCA_HOSTED_CHECKOUT_BUCKET)

