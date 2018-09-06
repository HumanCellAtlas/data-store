import os, sys, hashlib, base64, json, functools
from urllib.parse import quote
from furl import furl
import boto3
from botocore.vendored import requests
from chalice import Chalice, CognitoUserPoolAuthorizer, Response
import jwt, yaml

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

cognito = boto3.client("cognito-identity")
ad = boto3.client("clouddirectory")
iam = boto3.client("iam")
secretsmanager = boto3.client("secretsmanager")
app = Chalice(app_name='authd')
app.debug = True

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))
with open(os.path.join(pkg_root, "index.html")) as fh:
    swagger_ui_html = fh.read()

with open(os.path.join(pkg_root, "swagger.yml")) as fh:
    swagger_defn = yaml.load(fh.read())

with open(os.path.join(pkg_root, "service_config.json")) as fh:
    service_config = yaml.load(fh.read())

@app.route("/")
def serve_swagger_ui():
    return Response(status_code=200,
                    headers={"Content-Type": "text/html"},
                    body=swagger_ui_html)

@app.route('/swagger.json')
def serve_swagger_definition():
    return swagger_defn

oauth2_config = json.loads(secretsmanager.get_secret_value(SecretId="fusillade.oauth2_config")["SecretString"])

@functools.lru_cache(maxsize=32)
def get_openid_config(openid_provider):
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
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    openid_provider = furl(os.environ["OPENID_PROVIDER"]).host
    app.current_request.query_params["openid_provider"] = openid_provider
    if "client_id" in app.current_request.query_params:
        # TODO: audit this
        auth_params = dict(client_id=app.current_request.query_params["client_id"],
                           response_type="code",
                           scope=app.current_request.query_params["scope"],
                           redirect_uri=app.current_request.query_params["redirect_uri"],
                           state=app.current_request.query_params["state"])
        if "audience" in app.current_request.query_params:
            auth_params["audience"] = app.current_request.query_params["audience"]
    else:
        state = base64.b64encode(json.dumps(app.current_request.query_params).encode())
        # TODO: set random state
        # openid_provider = app.current_request.query_params["openid_provider"]
        auth_params = dict(client_id=oauth2_config[openid_provider]["client_id"],
                           response_type="code",
                           scope="openid email",
                           redirect_uri=oauth2_config[openid_provider]["redirect_uri"],
                           state=state)
    dest = furl(get_openid_config(openid_provider)["authorization_endpoint"]).add(query_params=auth_params).url
    return Response(status_code=302, headers=dict(Location=dest), body="")


cognito_id_pool_id = "us-east-1:56424a42-0f50-4196-9ac2-fd19df4adb12"

@app.route('/.well-known/openid-configuration')
def serve_openid_config():
    openid_provider = furl(os.environ["OPENID_PROVIDER"]).host
    openid_config = get_openid_config(openid_provider)
    auth_host = app.current_request.headers['host']
    openid_config.update(authorization_endpoint=f"https://{auth_host}/authorize",
                         token_endpoint=f"https://{auth_host}/oauth/token",
                         jwks_uri=f"https://{auth_host}/.well-known/jwks.json",
                         revocation_endpoint="https://{auth_host}/oauth/revoke",
                         userinfo_endpoint="https://{auth_host}/userinfo")
    return openid_config

def proxy_response(dest_url, **extra_query_params):
    if app.current_request.query_params or extra_query_params:
        dest_url = furl(dest_url).add(dict(app.current_request.query_params or {}, **extra_query_params)).url
    proxy_res = requests.request(method=app.current_request.method,
                                 url=dest_url,
                                 headers=app.current_request.headers,
                                 data=app.current_request.raw_body)
    if proxy_res.headers.get("Content-Type", "").startswith("application/json"):
        body = proxy_res.json()
    else:
        body = proxy_res.content.decode()
    for header in "connection", "content-length", "date":
        del proxy_res.headers[header]
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

@app.route('/oauth/token', methods=["POST"], content_types=["application/x-www-form-urlencoded", "application/x-www-form-urlencoded;charset=UTF-8"])
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
    state = json.loads(base64.b64decode(app.current_request.query_params["state"]))
    openid_provider = os.environ["OPENID_PROVIDER"]
    openid_config = get_openid_config(openid_provider)
    token_endpoint = openid_config["token_endpoint"]

#    cognito_logins = {openid_provider: res.json()["id_token"]}
#    cognito_id = cognito.get_id(IdentityPoolId=cognito_id_pool_id, Logins=cognito_logins)["IdentityId"]
#    cognito_credentials = cognito.get_credentials_for_identity(IdentityId=cognito_id, Logins=cognito_logins)["Credentials"]
#    cognito_credentials["Expiration"] = cognito_credentials["Expiration"].isoformat()
#    cognito_session = boto3.Session(aws_access_key_id=cognito_credentials["AccessKeyId"],
#                                    aws_secret_access_key=cognito_credentials["SecretKey"],
#                                    aws_session_token=cognito_credentials["SessionToken"])
    if "redirect_uri" in state and "client_id" in state:
        # OIDC proxy flow
        resp_params = dict(code=app.current_request.query_params["code"], state=state.get("state"))
        dest = furl(state["redirect_uri"]).add(resp_params).url
        return Response(status_code=302, headers=dict(Location=dest), body="")
    else:
        # Simple flow
        res = requests.post(token_endpoint, dict(code=app.current_request.query_params["code"],
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
                "query": app.current_request.query_params,
                "token_endpoint": token_endpoint,
                "res": res.json(),
                "tok": tok,
#        "cognito_credentials": cognito_credentials,
#        "cognito_caller_id": cognito_session.client("sts").get_caller_identity()
            }


schema_name = "authd4"
directory_name = "authd4"
schema_version = "1"

for directory in ad.list_directories()["Directories"]:
    if directory["Name"] == directory_name:
        directory_arn = directory["DirectoryArn"]
        break

schema_arn = "{}/schema/{}/{}".format(directory_arn, schema_name, schema_version)

def get_object_attribute_list(facet="User", **kwargs):
    return [dict(Key=dict(SchemaArn=schema_arn, FacetName=facet, Name=k), Value=dict(StringValue=v))
            for k, v in kwargs.items()]

def provision_user(email):
    username = quote(email)
    default_group = service_config["Services"][0]["DefaultProvisioningGroups"][0]
    policy = service_config["Services"][0]["DefaultGroupPolicies"][default_group]
    for statement in policy["Statement"]:
        statement["Resource"] = statement["Resource"].format(user_id=email)
    print("POLICY:")
    print(policy)
    try:
        res = ad.create_object(DirectoryArn=directory_arn,
                               SchemaFacets=[dict(SchemaArn=schema_arn, FacetName="User")],
                               ObjectAttributeList=get_object_attribute_list(username=username, email=email),
                               ParentReference=dict(Selector="/"),
                               LinkName=username)
        print(res["ObjectIdentifier"])
    except ad.exceptions.LinkNameAlreadyInUseException as e:
        print(e)
    res = ad.create_object(
        DirectoryArn=directory_arn,
        SchemaFacets=[dict(SchemaArn=schema_arn, FacetName="IAMPolicy")],
        ObjectAttributeList=[
            dict(Key=dict(SchemaArn=schema_arn, FacetName="IAMPolicy", Name="policy_type"),
                 Value=dict(StringValue="IAMPolicy")),
            dict(Key=dict(SchemaArn=schema_arn, FacetName="IAMPolicy", Name="policy_document"),
                 Value=dict(BinaryValue=json.dumps(policy).encode()))
        ]
    )
    policy_id = res["ObjectIdentifier"]
    for pid in ad.list_object_policies(DirectoryArn=directory_arn, ObjectReference=dict(Selector="/" + username))["AttachedPolicyIds"]:
        ad.detach_policy(DirectoryArn=directory_arn,
                         PolicyReference=dict(Selector="$" + pid),
                         ObjectReference=dict(Selector="/" + username))
    ad.attach_policy(DirectoryArn=directory_arn,
                     PolicyReference=dict(Selector="$" + policy_id),
                     ObjectReference=dict(Selector="/" + username))

@app.route('/policies/evaluate', methods=["POST"])
def evaluate_policy():
    principal = app.current_request.json_body["principal"]
    action = app.current_request.json_body["action"]
    resource = app.current_request.json_body["resource"]
    try:
        provision_user(principal)
    except Exception:
        pass

    username = quote(principal)
    policies_to_evaluate = []
    for ppl in ad.lookup_policy(DirectoryArn=directory_arn, ObjectReference=dict(Selector="/" + username))["PolicyToPathList"]:
        for policy_info in ppl["Policies"]:
            if "PolicyId" in policy_info:
                print(policy_info["PolicyId"])
                for attr in ad.list_object_attributes(DirectoryArn=directory_arn,
                                                      ObjectReference=dict(Selector="$" + policy_info["PolicyId"]))["Attributes"]:
                    if attr["Key"]["Name"] == "policy_document":
                        policies_to_evaluate.append(attr["Value"]["BinaryValue"].decode())

    result = iam.simulate_custom_policy(PolicyInputList=policies_to_evaluate,
                                        ActionNames=[action],
                                        ResourceArns=[resource])  # "arn:hca:dss:*:*:subscriptions/{}/*".format(username)]))

    # result["EvaluationResults"][0]["EvalDecision"]]
    del result["ResponseMetadata"]
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
