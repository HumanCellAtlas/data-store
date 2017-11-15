import json

import boto3
import os
import requests
from flask import jsonify
from cloud_blobstore import BlobNotFoundError
from dss import Config

from dss.util.bundles import get_bundle
from dss.util.checkout import get_execution_id
from ... import dss_handler, get_logger


@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):

    sfn = boto3.client('stepfunctions')
    sts_client = boto3.client("sts")

    email = json_request_body['email']
    dss_bucket = Config.get_s3_bucket()

    if replica is None:
        replica = "aws"

    try:
        get_bundle(uuid, replica, version)

        sfn_input = {"dss_bucket": dss_bucket, "bundle": uuid, "version": version, "email": email, "replica": replica}
        if "destination" in json_request_body:
            sfn_input["bucket"] = json_request_body["destination"]
        input = json.dumps(sfn_input)

        region = boto3.Session().region_name
        accountid = sts_client.get_caller_identity()['Account']
        stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        state_machine_arn = f"arn:aws:states:{region}:{accountid}:stateMachine:dss-checkout-sfn-{stage}"
        response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=get_execution_id(),
            input=input
        )
        return jsonify(dict(checkout_job_id=response["executionArn"])), requests.codes.ok
    except Exception as e:
        error_message = str(e)
        return jsonify(dict(code='not_found', title=error_message)), requests.codes.bad

@dss_handler
def get(checkout_job_id: str):
    sfn = boto3.client('stepfunctions')
    status = None
    error_message = None
    try:
        response = sfn.describe_execution(
            executionArn=checkout_job_id
        )
        status = response.get('status')
    except Exception as e:
        error_message = str(e)

    if status is None:
        return jsonify(dict(code='not_found', title=error_message)), requests.codes.bad_request

    return jsonify(dict(status=status)), requests.codes.ok
