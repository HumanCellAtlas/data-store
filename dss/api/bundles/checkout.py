import requests
from flask import jsonify

from dss import dss_handler, Replica
from dss.error import DSSException
from dss.storage.checkout import BundleNotFoundError, CheckoutStatus, start_bundle_checkout


@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str=None):

    assert replica is not None

    try:
        execution_id = start_bundle_checkout(
            uuid,
            version,
            Replica[replica],
            dst_bucket=json_request_body.get('destination', None),
            email_address=json_request_body.get('email', None),
        )
    except BundleNotFoundError:
        raise DSSException(404, "not_found", "Cannot find bundle!")

    return jsonify(dict(checkout_job_id=execution_id)), requests.codes.ok


@dss_handler
def get(replica: str, checkout_job_id: str):
    assert replica is not None
    _replica = Replica[replica]
    response = CheckoutStatus.get_bundle_checkout_status(_replica.checkout_bucket, checkout_job_id, _replica)
    return response, requests.codes.ok
