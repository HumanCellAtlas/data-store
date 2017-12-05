import os
import json

import requests
import boto3
from flask import jsonify

from ..bundles import get_bundle
from ... import Config, dss_handler
from ...util.checkout import get_execution_id

@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):
    sfn = boto3.client('stepfunctions')
    sts_client = boto3.client("sts")

    email = json_request_body['email']
    dss_bucket = Config.get_s3_bucket()

    assert replica is not None

    get_bundle(uuid, replica, version)
    sfn_input = {"dss_bucket": dss_bucket, "bundle": uuid, "version": version, "email": email, "replica": replica}
    if "destination" in json_request_body:
        sfn_input["bucket"] = json_request_body["destination"]
    execution_input = json.dumps(sfn_input)

    region = boto3.Session().region_name
    accountid = sts_client.get_caller_identity()['Account']
    stage = os.environ["DSS_DEPLOYMENT_STAGE"]
    state_machine_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-checkout-sfn-{stage}"
    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=get_execution_id(),
        input=execution_input
    )
    return jsonify(dict(checkout_job_id=response["executionArn"])), requests.codes.ok

@dss_handler
def get(checkout_job_id: str):
    sfn = boto3.client('stepfunctions')
    response = sfn.describe_execution(
        executionArn=checkout_job_id
    )
    return jsonify(dict(status=response.get('status'))), requests.codes.ok
