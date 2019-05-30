from flask import request, make_response, jsonify

from fusillade import Role, directory
from fusillade.utils.authorize import authorize
from fusillade.api.paging import get_next_token, get_page


@authorize(['fus:PutRole'], ['arn:hca:fus:*:*:role'])
def put_new_role(token_info: dict):
    json_body = request.json
    Role.create(directory, json_body['role_id'], statement=json_body.get('policy'))
    return make_response(f"New role {json_body['role_id']} created.", 201)


@authorize(['fus:GetRole'], ['arn:hca:fus:*:*:role'])
def get_roles(token_info: dict):
    next_token, per_page = get_next_token(request.args)
    return get_page(Role.list_all, next_token, per_page, directory)


@authorize(['fus:GetRole'], ['arn:hca:fus:*:*:role/{role_id}/'], ['role_id'])
def get_role(token_info: dict, role_id: str):
    role = Role(directory, role_id)
    resp = dict(
        role_id=role.name,
        policy=role.statement
    )
    return make_response(jsonify(resp), 200)


@authorize(['fus:PutRole'], ['arn:hca:fus:*:*:role/{role_id}/'], ['role_id'])
def put_role_policy(token_info: dict, role_id: str):
    role = Role(directory, role_id)
    role.statement = request.json['policy']
    return make_response('Role policy updated.', 200)
