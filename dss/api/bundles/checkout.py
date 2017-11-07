import json

import boto3
import os
import requests
from flask import jsonify

from dss.util.checkout import get_execution_id
from ... import dss_handler, get_logger

@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):

    sfn = boto3.client('stepfunctions')
    sts_client = boto3.client("sts")

    email = json_request_body['email']

    sfn_input = {"bundle": uuid, "version": version, "email": email}
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
    print(">>>>>")
    return jsonify(dict(version=version, url='TBD', execution_id=response["executionArn"])), requests.codes.ok
