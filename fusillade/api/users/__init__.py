from flask import request, make_response, jsonify
from fusillade import User, directory


def put_new_user():
    json_body = request.json
    user = User(directory, json_body['username'], local=True)
    user.provision_user(statement=json_body.get('policy'))
    user.add_roles(json_body.get('roles', []))
    user.add_groups(json_body.get('groups', []))
    return make_response('', 201)


def get_user(user_id):
    user = User(directory, user_id, local=True)
    return make_response(jsonify(name=user.name, status=user.status, policy=user.statement), 200)


def put_user(user_id):
    user = User(directory, user_id, local=True)
    new_status = request.args['status']
    if new_status == 'enabled':
        user.enable()
        resp = make_response('', 200)
    elif new_status == 'disabled':
        user.disable()
        resp = make_response('', 200)
    else:
        resp = make_response('', 500)
    return resp


def put_user_policy(user_id):
    user = User(directory, user_id, local=True)
    user.statement = request.json['policy']
    return make_response('', 200)


def get_users_groups(user_id):
    user = User(directory, user_id, local=True)
    return make_response(jsonify(groups=user.groups), 200)


def put_users_groups(user_id):
    user = User(directory, user_id, local=True)
    action = request.args['action']
    if action == 'add':
        user.add_groups(request.json['groups'])
    elif action == 'remove':
        user.remove_groups(request.json['groups'])
    return make_response('', 200)


def get_users_roles(user_id):
    user = User(directory, user_id, local=True)
    return make_response(jsonify(roles=user.roles), 200)


def put_users_roles(user_id):
    user = User(directory, user_id, local=True)
    action = request.args['action']
    if action == 'add':
        user.add_roles(request.json['roles'])
    elif action == 'remove':
        user.remove_roles(request.json['roles'])
    return make_response('', 200)
