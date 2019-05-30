from flask import request, make_response, jsonify

from fusillade import directory, Group
from fusillade.utils.authorize import assert_authorized
from fusillade.api.paging import get_next_token, get_page


def put_new_group(token_info: dict):
    json_body = request.json
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:PutGroup'],
                      [f'arn:hca:fus:*:*:group'])
    group = Group.create(directory, json_body['group_id'], statement=json_body.get('policy'))
    group.add_roles(json_body.get('roles', []))  # Determine what response to return if roles don't exist
    return make_response(f"New role {json_body['group_id']} created.", 201)


def get_groups(token_info: dict):
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:GetRole'],
                      [f'arn:hca:fus:*:*:group'])
    next_token, per_page = get_next_token(request.args)
    return get_page(Group.list_all, next_token, per_page, directory)


def get_group(token_info: dict, group_id: str):
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:GetGroup'],
                      [f'arn:hca:fus:*:*:group/{group_id}/'])
    group = Group(directory, group_id)
    return make_response(jsonify(name=group.name, policy=group.statement), 200)


def put_group_policy(token_info: dict, group_id: str):
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:GetGroup'],
                      [f'arn:hca:fus:*:*:group/{group_id}/policy'])
    group = Group(directory, group_id)
    group.statement = request.json['policy']
    return make_response(f"{group_id} policy modified.", 200)


def get_group_users(token_info: dict, group_id: str):
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:GetUser'],
                      [f'arn:hca:fus:*:*:group/{group_id}/users'])
    next_token, per_page = get_next_token(request.args)
    group = Group(directory, group_id)
    return get_page(group.get_users_page, next_token, per_page)


def get_groups_roles(token_info: dict, group_id: str):
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:GetRole'],
                      [f'arn:hca:fus:*:*:group/{group_id}/roles'])
    next_token, per_page = get_next_token(request.args)
    group = Group(directory, group_id)
    return get_page(group.get_roles, next_token, per_page)


def put_groups_roles(token_info: dict, group_id: str):
    assert_authorized(token_info['https://auth.data.humancellatlas.org/email'],
                      ['fus:PutRole'],
                      [f'arn:hca:fus:*:*:group/{group_id}/roles'])
    group = Group(directory, group_id)
    action = request.args['action']
    if action == 'add':
        group.add_roles(request.json['roles'])
    elif action == 'remove':
        group.remove_roles(request.json['roles'])
    return make_response(f"Roles: {request.json['roles']} added to {group_id} policy modified.", 200)


def delete_group(group_id):
    pass
