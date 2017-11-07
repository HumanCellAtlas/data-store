import json

import boto3
import requests
from flask import jsonify

from ... import dss_handler, get_logger

@dss_handler
def post(uuid: str, json_request_body: dict, replica: str, version: str = None):

    sfn = boto3.client('stepfunctions')
    lambda_input = {"input": "some input"}
    input = json.dumps(lambda_input).encode('utf8')
    response = sfn.start_execution(
        stateMachineArn='my_state_machine_ARN',
        name="a unique name",
        input=input
    )
    return jsonify(dict(version=version, url='a')), requests.codes.ok
