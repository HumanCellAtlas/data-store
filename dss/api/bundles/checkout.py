import requests
from cloud_blobstore import BlobNotFoundError
from flask import jsonify

from dss import dss_handler, Replica
from dss.error import DSSException
from dss.storage.checkout import BundleNotFoundError
from dss.storage.checkout.bundle import get_bundle_checkout_status, start_bundle_checkout


@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str=None):

    assert replica is not None
    _replica: Replica = Replica[replica]
    dst_bucket = json_request_body.get('destination', _replica.checkout_bucket)

    try:
        execution_id = start_bundle_checkout(
            _replica,
            uuid,
            version,
            dst_bucket=dst_bucket,
            email_address=json_request_body.get('email', None),
        )
    except BundleNotFoundError:
        raise DSSException(404, "not_found", "Cannot find bundle!")

    return jsonify(dict(checkout_job_id=execution_id)), requests.codes.ok


@dss_handler
def get(replica: str, checkout_job_id: str):
    assert replica is not None
    _replica = Replica[replica]
    try:
        response = get_bundle_checkout_status(checkout_job_id, _replica, _replica.checkout_bucket)
    except BlobNotFoundError:
        raise DSSException(requests.codes.not_found, "not_found", "Cannot find checkout!")
    return response, requests.codes.ok
