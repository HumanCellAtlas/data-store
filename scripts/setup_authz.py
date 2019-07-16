#!/usr/bin/env python
"""
This script creates new or updates existing roles from 'data-store/iam
"""
import json
import os

import jwt
import requests
import time

auth_url = os.getenv('AUTH_URL')

# supply google service account credentials and register them in fusillade.
# This will allow the server to configure the application to run using fusillade
with open(f"{os.environ('DSS_HOME')}/{os.environ('GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME')}") as fp:
    google_service_account_credentials = json.load(fp)


def get_service_jwt(audience=None):
    iat = time.time()
    exp = iat + 3600
    payload = {'iss': google_service_account_credentials["client_email"],
               'sub': google_service_account_credentials["client_email"],
               'aud': audience or "https://dev.data.humancellatlas.org/",
               'iat': iat,
               'exp': exp,
               'scope': ['email', 'openid', 'offline_access'],
               'https://auth.data.humancellatlas.org/email': google_service_account_credentials["client_email"]
               }
    additional_headers = {'kid': google_service_account_credentials["private_key_id"]}
    signed_jwt = jwt.encode(payload, google_service_account_credentials["private_key"], headers=additional_headers,
                            algorithm='RS256').decode()
    return signed_jwt


def get_auth_header(token):
    return {"Authorization": f"Bearer {token}"}


def setup_fusillade():
    global fusillade_setup

    def add_role(name, policy):
        resp = requests.get(
            f"{auth_url}/v1/role/{name}",
            headers=headers
        )
        if resp.status_code == requests.codes.not_found:
            print(f"adding role {name}")
            resp = requests.post(
                f"{auth_url}/v1/role/",
                headers=headers,
                json={
                    "role_id": f"{name}",
                    "policy": json.dumps(policy)
                }
            )
            resp.raise_for_status()
        elif resp.status_code == requests.codes.ok:
            requests.put(
                f"{auth_url}/v1/role/{name}/policy",
                json={"policy": json.dumps(policy)},
                headers=headers,
            )
        elif resp.status_code != requests.codes.ok:
            if resp.status_code == requests.codes.forbidden:
                print(f"the service account {google_service_account_credentials['client_email']} has insufficent "
                      f"permissions to setup fusillade.")
            resp.raise_for_status()
        print(f"role {name} created")

    if not fusillade_setup:
        headers = {'Content-Type': "application/json"}
        headers.update(get_auth_header(get_service_jwt()))
        path = f"{os.getenv('DSS_HOME')}/iam/authorization_policies"
        files = os.listdir(path)
        for file in files:
            with open(f"{path}/{file}", 'r') as fp:
                data = json.load(fp)
            add_role(f"dss_{file.split('.')[0]}", data)


if __name__ == "__main__":
    try:
        setup_fusillade()
    except:
        print("setup failed!")
        exit(1)
