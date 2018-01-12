import requests
import boto3
from flask import jsonify

from ..bundles import get_bundle
from ... import Config, dss_handler, stepfunctions, Replica
from ...util.checkout import get_execution_id

STATE_MACHINE_NAME_TEMPLATE = "dss-checkout-sfn-{stage}"

@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):
    email = json_request_body['email']
    dss_bucket = Config.get_s3_bucket()

    assert replica is not None

    bundle = get_bundle(uuid, Replica[replica], version)
    sfn_input = {"dss_bucket": dss_bucket, "bundle": uuid, "version": bundle["bundle"]["version"],
                 "email": email, "replica": replica}
    if "destination" in json_request_body:
        sfn_input["bucket"] = json_request_body["destination"]

    execution_id = get_execution_id()
    stepfunctions.step_functions_invoke(STATE_MACHINE_NAME_TEMPLATE, execution_id, sfn_input)
    return jsonify(dict(checkout_job_id=execution_id)), requests.codes.ok


@dss_handler
def get(checkout_job_id: str):
    response = stepfunctions.step_functions_describe_execution(STATE_MACHINE_NAME_TEMPLATE, checkout_job_id)
    return jsonify(dict(status=response.get('status'))), requests.codes.ok
