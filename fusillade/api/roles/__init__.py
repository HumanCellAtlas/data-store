from flask import request, make_response, jsonify

from fusillade import Role, directory


def put_new_role():
    json_body = request.json
    Role.create(directory, json_body['name'], statement=json_body.get('policy'))
    return make_response('', 201)


def get_roles():
    pass


def get_role(role_id):
    role = Role(directory, role_id)
    return make_response(jsonify(name=role.name, policy=role.statement), 200)


def put_role_policy(role_id):
    role = Role(directory, role_id)
    role.statement = request.json['policy']
    return make_response('', 200)
