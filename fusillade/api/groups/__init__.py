from flask import request, make_response, jsonify

from fusillade import directory, Group
from fusillade.utils.authorize import authorize
from fusillade.api.paging import get_next_token, get_page


@authorize(['fus:PutGroup'], ['arn:hca:fus:*:*:group'])
def put_new_group(token_info: dict):
    json_body = request.json
    group = Group.create(directory, json_body['group_id'], statement=json_body.get('policy'))
    group.add_roles(json_body.get('roles', []))  # Determine what response to return if roles don't exist
    return make_response(f"New role {json_body['group_id']} created.", 201)


@authorize(['fus:GetGroup'], ['arn:hca:fus:*:*:group'])
def get_groups(token_info: dict):
    next_token, per_page = get_next_token(request.args)
    return get_page(Group.list_all, next_token, per_page, directory)


@authorize(['fus:GetGroup'], ['arn:hca:fus:*:*:group/{group_id}/'], ['group_id'])
def get_group(token_info: dict, group_id: str):
    group = Group(directory, group_id)
    return make_response(jsonify(name=group.name, policy=group.statement), 200)


@authorize(['fus:PutGroup'], ['arn:hca:fus:*:*:group/{group_id}/policy'], ['group_id'])
def put_group_policy(token_info: dict, group_id: str):
    group = Group(directory, group_id)
    group.statement = request.json['policy']
    return make_response(f"{group_id} policy modified.", 200)


@authorize(['fus:GetUser'], ['arn:hca:fus:*:*:group/{group_id}/users'], ['group_id'])
def get_group_users(token_info: dict, group_id: str):
    next_token, per_page = get_next_token(request.args)
    group = Group(directory, group_id)
    return get_page(group.get_users_page, next_token, per_page)


@authorize(['fus:GetRole'], ['arn:hca:fus:*:*:group/{group_id}/roles'], ['group_id'])
def get_groups_roles(token_info: dict, group_id: str):
    next_token, per_page = get_next_token(request.args)
    group = Group(directory, group_id)
    return get_page(group.get_roles, next_token, per_page)


@authorize(['fus:PutRole'], ['arn:hca:fus:*:*:group/{group_id}/roles'], ['group_id'])
def put_groups_roles(token_info: dict, group_id: str):
    group = Group(directory, group_id)
    action = request.args['action']
    if action == 'add':
        group.add_roles(request.json['roles'])
    elif action == 'remove':
        group.remove_roles(request.json['roles'])
    return make_response(f"Roles: {request.json['roles']} added to {group_id} policy modified.", 200)


@authorize(['fus:DeleteGroup'], ['arn:hca:fus:*:*:group/{group_id}/'], ['group_id'])
def delete_group(token_info: dict, group_id):
    pass
