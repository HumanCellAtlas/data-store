from concurrent.futures import ThreadPoolExecutor

import chainedawslambda.aws
import requests
from chainedawslambda import aws
from chainedawslambda.s3copyclient import S3ParallelCopySupervisorTask
from flask import jsonify

from dss.api.bundles.helpers import get_destination_bucket, get_dst_bundle_prefix, get_src_object_name, get_copy_mode, \
    CopyMode, parallel_copy, copyInline
from dss.api.bundles.validation import validate_dst
from dss.util.aws import get_s3_chunk_size
from dss.util.bundles import get_bundle
from ... import DSSException, dss_handler, get_logger
from ...config import Config
from ...events.chunkedtask import s3copyclient

HCA_OWNED_STORE_BUCKET = "org-humancellatlas-dss-dev"

log = get_logger()

@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):
    handle, hca_handle, src_bucket = Config.get_cloud_specific_handles(replica)
    dst_bucket = get_destination_bucket(json_request_body)
    if dst_bucket is None:
        dst_bucket = src_bucket
    else:
        validate_dst(handle, json_request_body)

    bundleManifest = get_bundle(uuid, replica, version).get('bundle')
    version = bundleManifest['version']
    dst_bundle_prefix = get_dst_bundle_prefix(bundleManifest)
    files = bundleManifest.get('files')

    executor = ThreadPoolExecutor(max_workers=5)
    for file in files:
        dst_object_name = "{}/{}".format(dst_bundle_prefix, file.get('name'))
        src_object_name = get_src_object_name(file)
        copy_mode = get_copy_mode(handle, file, src_bucket, src_object_name, dst_bucket, dst_object_name, replica)

        if copy_mode == CopyMode.NO_COPY:
            log.debug("File already exists - no copy is necessary " + dst_object_name)
            continue
        elif copy_mode == CopyMode.COPY_ASYNC:
            log.debug("Copying a large file using multi-part copy " + dst_object_name)
            parallel_copy(src_bucket, src_object_name, dst_bucket, dst_object_name)
        elif copy_mode == CopyMode.COPY_INLINE:
            log.debug("Copying a small file using inline copy " + dst_object_name)
            executor.submit(copyInline(handle, src_bucket, src_object_name, dst_bucket, dst_object_name))

    return jsonify(dict(version=version, url='a')), requests.codes.ok
