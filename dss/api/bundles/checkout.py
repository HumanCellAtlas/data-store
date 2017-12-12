import requests
import boto3
from flask import jsonify

from ..bundles import get_bundle
from ... import Config, dss_handler, stepfunctions, Replica
from ...util.checkout import get_execution_id


@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):
    email = json_request_body['email']
    dss_bucket = Config.get_s3_bucket()

    assert replica is not None

    get_bundle(uuid, Replica[replica], version)
    sfn_input = {"dss_bucket": dss_bucket, "bundle": uuid, "version": version, "email": email, "replica": replica}
    if "destination" in json_request_body:
        sfn_input["bucket"] = json_request_body["destination"]

    response = stepfunctions.step_functions_invoke("dss-checkout-sfn-{stage}", get_execution_id(), sfn_input)

    return jsonify(dict(checkout_job_id=response["executionArn"])), requests.codes.ok


@dss_handler
def get(checkout_job_id: str):
    sfn = boto3.client('stepfunctions')
    response = sfn.describe_execution(
        executionArn=checkout_job_id
    )
    return jsonify(dict(status=response.get('status'))), requests.codes.ok
