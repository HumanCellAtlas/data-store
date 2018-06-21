import requests
from flask import jsonify

from dss import dss_handler, Replica
from dss.storage.checkout import CheckoutStatus, start_bundle_checkout


@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str=None):

    assert replica is not None

    execution_id = start_bundle_checkout(
        uuid,
        version,
        Replica[replica],
        dst_bucket=json_request_body.get('destination', None),
        email_address=json_request_body.get('email', None),
    )

    return jsonify(dict(checkout_job_id=execution_id)), requests.codes.ok


@dss_handler
def get(checkout_job_id: str):
    response = CheckoutStatus.get_bundle_checkout_status(checkout_job_id)
    return response, requests.codes.ok
