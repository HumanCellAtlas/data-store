import sys

import domovoi
import os

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa

sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config
from dss.api.bundles.helpers import get_copy_mode, CopyMode
from dss.util.bundles import get_bundle

from dss.util.checkout_helpers import parallel_copy, get_dst_bundle_prefix, get_src_object_name
from dss.util.state_machine import sfn1

app = domovoi.Domovoi()
dss.Config.set_config(dss.BucketConfig.NORMAL)

log = dss.get_logger()

@app.step_function_task(state_name="ScheduleCopy", state_machine_definition=sfn1)
def worker(event, context):
    bundle_id = event["bundle_id"]
    version = event["version"]
    replica = "aws"
    src_bucket = "org-humancellatlas-dss-dev"
    dst_bucket = src_bucket
    handle, hca_handle, src_bucket = Config.get_cloud_specific_handles(replica)

    bundleManifest = get_bundle(bundle_id, replica, version).get('bundle')
    version = bundleManifest['version']
    dst_bundle_prefix = get_dst_bundle_prefix(bundleManifest, version)
    files = bundleManifest.get('files')

    scheduled = 0
    skipped = 0

    for file in files:
        dst_object_name = "{}/{}".format(dst_bundle_prefix, file.get('name'))
        src_object_name = get_src_object_name(file)
        #copy_mode = get_copy_mode(handle, file, src_bucket, src_object_name, dst_bucket, dst_object_name, replica)
        copy_mode = CopyMode.COPY_ASYNC
        if copy_mode == CopyMode.NO_COPY:
            log.debug("File already exists - no copy is necessary " + dst_object_name)
            skipped += 1
            continue
        else:
            log.debug("Copying a file " + dst_object_name)
            parallel_copy(src_bucket, src_object_name, dst_bucket, dst_object_name)
            scheduled += 1
    return {"files_scheduled": scheduled, "files_skipped": skipped, "wait_time": 20}


#
# def do_work(event, context):
#     bundle_uuid = event["bundleId"]
#     files = event["files"]
#     copy_file(bundle_uuid, files, event["branch"])
#     return event


#
# for branch in range(pool_size):
#     app.step_function_task(state_name="Copy{}".format(branch), state_machine_definition=sfn)(do_work)
