from flask import request, make_response, jsonify

from fusillade import Role, directory
from fusillade.utils.authorize import assert_authorized


def put_new_role(user: str):
    assert_authorized(user, ['fus:PutRole'], ['arn:hca:fus:*:*:role'])
    json_body = request.json
    Role.create(directory, json_body['name'], statement=json_body.get('policy'))
    return make_response(f"New role {json_body['name']} created.", 201)


def get_roles():
    pass


def get_role(user: str, role_id: str):
    assert_authorized(user, ['fus:GetRole'], [f'arn:hca:fus:*:*:role/{role_id}'])
    role = Role(directory, role_id)
    resp = dict(
        name=role.name,
        policy=role.statement
    )
    return make_response(jsonify(resp), 200)


def put_role_policy(user: str, role_id: str):
    assert_authorized(user, ['fus:PutRole'], [f'arn:hca:fus:*:*:role/{role_id}'])
    role = Role(directory, role_id)
    role.statement = request.json['policy']
    return make_response('Role policy updated.', 200)
