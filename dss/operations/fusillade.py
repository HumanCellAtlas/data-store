"""
DSS-Fusillade integration operations.
"""
import os
import typing
import argparse
import time
from functools import lru_cache

import jwt
import requests
import boto3

from dss.config import Replica
from dss.operations import dispatch
from dss.events.handlers.notify_v2 import build_bundle_metadata_document


logger = logging.getLogger(__name__)


fusillade = dispatch.target("fusillade", help=__doc__)


@events.action("authorize")
def authorize(argv: typing.List[str], args: argparse.Namespace):
    """
    Create or update roles defined in 'data-store/authorization_roles'. Usage requires a Google Cloud Platform
    service account authorized to created and modify roles in fusillade. Roles are created, and policies attached,
    for each file in the 'data-store/authorization_roles' directory. The role name will be the name of file without the
    file extension, prefixed with 'dss_'. For example the file "data-store/authorization_roles/admin.json" would create
    the role, "dss_admin" in fusillade.
    """
    auth_url = os.environ.get('AUTH_URL')

    headers = {'Content-Type': "application/json"}
    headers.update(_get_auth_header(_get_service_jwt()))

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
                print(f"the service account {_google_service_account_credentials()['client_email']} has insufficent "
                      f"permissions to setup fusillade.")
            resp.raise_for_status()
        print(f"role {name} created")

    path = f"{os.getenv('DSS_HOME')}/authorization_roles"
    files = os.listdir(path)
    for file in files:
        with open(f"{path}/{file}", 'r') as fp:
            data = json.load(fp)
        add_role(f"dss_{file.split('.')[0]}", data)

def _get_service_jwt(audience=None):
    iat = time.time()
    exp = iat + 3600
    payload = {'iss': _google_service_account_credentials()["client_email"],
               'sub': _google_service_account_credentials()["client_email"],
               'aud': audience or "https://dev.data.humancellatlas.org/",
               'iat': iat,
               'exp': exp,
               'scope': ['email', 'openid', 'offline_access'],
               'https://auth.data.humancellatlas.org/email': _google_service_account_credentials()["client_email"]
               }
    additional_headers = {'kid': _google_service_account_credentials()["private_key_id"]}
    signed_jwt = jwt.encode(payload, _google_service_account_credentials()["private_key"], headers=additional_headers,
                            algorithm='RS256').decode()
    return signed_jwt

def _get_auth_header(token):
    return {"Authorization": f"Bearer {token}"}

@lru_cache()
def _google_service_account_credentials():
    secret_id = '/'.join([os.environ.get('DSS_SECRETS_STORE'),
                          os.environ.get('DSS_DEPLOYMENT_STAGE'),
                          os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME')])
    return json.loads(boto3.client('secretsmanager').get_secret_value(SecretId=secret_id)['SecretString'])
