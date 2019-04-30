from dcplib.aws import clients as aws_clients
from flask import make_response, request, jsonify

from fusillade import User, directory

iam = aws_clients.iam


def evaluate_policy():
    json_body = request.json
    principal = json_body["principal"]
    action = json_body["action"]
    resource = json_body["resource"]
    user = User(directory, principal)
    result = iam.simulate_custom_policy(
        PolicyInputList=user.lookup_policies(),
        ActionNames=[action],
        ResourceArns=[resource],
        ContextEntries=[
            {
                'ContextKeyName': 'fus:user_email',
                'ContextKeyValues': [principal],
                'ContextKeyType': 'string'
            }
        ]
    )['EvaluationResults'][0]['EvalDecision']
    result = True if result == 'allowed' else False
    return make_response(jsonify(principal=principal, action=action, resource=resource, result=result), 200)
