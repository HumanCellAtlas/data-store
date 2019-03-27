import os, sys, base64, json, functools
from furl import furl
import boto3
from botocore.vendored import requests
from chalice import Chalice, Response
import jwt, yaml

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))
sys.path.insert(0, pkg_root)  # noqa

from fusillade.clouddirectory import CloudDirectory, User, Role
from fusillade import Config
from fusillade.errors import FusilladeException

iam = boto3.client("iam")
app = Chalice(app_name='fusillade')
app.debug = True

with open(os.path.join(pkg_root, "service_config.json")) as fh:
    service_config = yaml.load(fh.read())


@app.route("/")
def serve_swagger_ui():
    with open(os.path.join(pkg_root, "index.html")) as fh:
        swagger_ui_html = fh.read()
    return Response(status_code=200,
                    headers={"Content-Type": "text/html"},
                    body=swagger_ui_html)


@app.route('/swagger.json')
def serve_swagger_definition():
    with open(os.path.join(pkg_root, "swagger.yml")) as fh:
        swagger_defn = yaml.load(fh.read())
    return swagger_defn


@functools.lru_cache(maxsize=32)
def get_openid_config(openid_provider):
    if openid_provider.startswith("https://"):
        openid_provider = furl(openid_provider).host
    res = requests.get(f"https://{openid_provider}/.well-known/openid-configuration")
    res.raise_for_status()
    return res.json()


@functools.lru_cache(maxsize=32)
def get_public_keys(openid_provider):
    keys = requests.get(get_openid_config(openid_provider)["jwks_uri"]).json()["keys"]
    return {
        key["kid"]: rsa.RSAPublicNumbers(
            e=int.from_bytes(base64.urlsafe_b64decode(key["e"] + "==="), byteorder="big"),
            n=int.from_bytes(base64.urlsafe_b64decode(key["n"] + "==="), byteorder="big")
        ).public_key(backend=default_backend())
        for key in keys
    }


@app.route('/login')
def login():
    return Response(status_code=301, headers=dict(Location="/authorize"), body="")


@app.route('/authorize')
def authorize():
    query_params = app.current_request.query_params if app.current_request.query_params else {}
    openid_provider = os.environ["OPENID_PROVIDER"]
    query_params["openid_provider"] = openid_provider
    if "client_id" in query_params:
        # TODO: audit this
        auth_params = dict(client_id=query_params["client_id"],
                           response_type="code",
                           scope=query_params["scope"],
                           redirect_uri=query_params["redirect_uri"],
                           state=query_params["state"])
        if "audience" in query_params:
            auth_params["audience"] = query_params["audience"]
    else:
        state = base64.b64encode(json.dumps(query_params).encode())
        # TODO: set random state
        # openid_provider = query_params["openid_provider"]
        oauth2_config = Config.get_oauth2_config()
        auth_params = dict(client_id=oauth2_config[openid_provider]["client_id"],
                           response_type="code",
                           scope="openid email profile",
                           redirect_uri=oauth2_config[openid_provider]["redirect_uri"],
                           state=state)
    dest = furl(get_openid_config(openid_provider)["authorization_endpoint"]).add(query_params=auth_params).url
    return Response(status_code=302, headers=dict(Location=dest), body="")


@app.route('/.well-known/openid-configuration')
def serve_openid_config():
    openid_config = get_openid_config(os.environ["OPENID_PROVIDER"])
    auth_host = app.current_request.headers['host']
    openid_config.update(authorization_endpoint=f"https://{auth_host}/authorize",
                         token_endpoint=f"https://{auth_host}/oauth/token",
                         jwks_uri=f"https://{auth_host}/.well-known/jwks.json",
                         revocation_endpoint="https://{auth_host}/oauth/revoke",
                         userinfo_endpoint="https://{auth_host}/userinfo")
    return openid_config


def proxy_response(dest_url, **extra_query_params):
    query_params = app.current_request.query_params or {}
    if query_params or extra_query_params:
        dest_url = furl(dest_url).add(dict(query_params, **extra_query_params)).url
    proxy_res = requests.request(method=app.current_request.method,
                                 url=dest_url,
                                 headers=app.current_request.headers,
                                 data=app.current_request.raw_body)
    if proxy_res.headers.get("Content-Type", "").startswith("application/json"):
        body = proxy_res.json()
    else:
        body = proxy_res.content.decode()
    for header in "connection", "content-length", "date":
        proxy_res.headers.pop(header, None)
    return Response(status_code=proxy_res.status_code,
                    headers=dict(proxy_res.headers),
                    body=body)


@app.route('/.well-known/jwks.json')
def serve_jwks_json():
    openid_config = get_openid_config(os.environ["OPENID_PROVIDER"])
    return proxy_response(openid_config["jwks_uri"])


@app.route('/oauth/revoke')
def revoke():
    openid_config = get_openid_config(os.environ["OPENID_PROVIDER"])
    return proxy_response(openid_config["revocation_endpoint"])


@app.route('/userinfo')
def userinfo():
    openid_config = get_openid_config(os.environ["OPENID_PROVIDER"])
    return proxy_response(openid_config["userinfo_endpoint"])


@app.route('/oauth/token', methods=["POST"], content_types=["application/x-www-form-urlencoded",
                                                            "application/x-www-form-urlencoded;charset=UTF-8"])
def serve_oauth_token():
    # TODO: client id/secret mgmt
    openid_provider = os.environ["OPENID_PROVIDER"]
    openid_config = get_openid_config(openid_provider)
    return proxy_response(openid_config["token_endpoint"])


@app.route('/echo')
def echo():
    return str(app.current_request.__dict__)


@app.route('/cb')
def cb():
    query_params = app.current_request.query_params
    state = json.loads(base64.b64decode(query_params["state"]))
    openid_provider = os.environ["OPENID_PROVIDER"]
    openid_config = get_openid_config(openid_provider)
    token_endpoint = openid_config["token_endpoint"]

    if "redirect_uri" in state and "client_id" in state:
        # OIDC proxy flow
        resp_params = dict(code=query_params["code"], state=state.get("state"))
        dest = furl(state["redirect_uri"]).add(resp_params).url
        return Response(status_code=302, headers=dict(Location=dest), body="")
    else:
        # Simple flow
        oauth2_config = Config.get_oauth2_config()
        res = requests.post(token_endpoint, dict(code=query_params["code"],
                                                 client_id=oauth2_config[openid_provider]["client_id"],
                                                 client_secret=oauth2_config[openid_provider]["client_secret"],
                                                 redirect_uri=oauth2_config[openid_provider]["redirect_uri"],
                                                 grant_type="authorization_code"))
        res.raise_for_status()
        token_header = jwt.get_unverified_header(res.json()["id_token"])
        public_keys = get_public_keys(openid_provider)
        tok = jwt.decode(res.json()["id_token"],
                         key=public_keys[token_header["kid"]],
                         audience=oauth2_config[openid_provider]["client_id"])
        assert tok["email_verified"]
        if "redirect_uri" in state:
            # Simple flow - redirect with QS
            resp_params = dict(res.json(), decoded_token=json.dumps(tok), state=state.get("state"))
            dest = furl(state["redirect_uri"]).add(resp_params).url
            return Response(status_code=302, headers=dict(Location=dest), body="")
        else:
            # Simple flow - JSON
            return {
                "headers": dict(app.current_request.headers),
                "query": query_params,
                "token_endpoint": token_endpoint,
                "res": res.json(),
                "tok": tok,
            }


directory_name = Config.get_directory_name()
try:
    directory = CloudDirectory.from_name(directory_name)
except FusilladeException:
    from fusillade.clouddirectory import publish_schema, create_directory

    schema_name = Config.get_schema_name()
    schema_arn = publish_schema(schema_name, version="0.1")
    directory = create_directory(directory_name, schema_arn)


@app.route('/policies/evaluate', methods=["POST"])
def evaluate_policy():
    json_body = app.current_request.json_body
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
    return dict(principal=principal, action=action, resource=resource, result=result)


@app.route('/users', methods=["PUT"])
def put_user():
    return {}


@app.route('/users/{user_id}', methods=["GET"])
def get_user(user_id):
    return {}


@app.route('/groups', methods=["PUT"])
def put_group():
    return {}


@app.route('/groups/{group_id}', methods=["GET"])
def get_group(group_id):
    return {}
