import os, sys, hashlib, base64, json
from urllib.parse import urlsplit, urlunsplit, urlencode, quote

import boto3
from botocore.vendored import requests
from chalice import Chalice, CognitoUserPoolAuthorizer, Response
import jwt, yaml

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

openid_provider = "humancellatlas.auth0.com"

oauth2_config = json.loads(secretsmanager.get_secret_value(SecretId="fusillade.oauth2_config")["SecretString"])

def get_openid_config(domain):
    res = requests.get(f"https://{domain}/.well-known/openid-configuration")
    res.raise_for_status()
    return res.json()

@app.route('/login')
def login():
    if app.current_request.query_params is None:
        app.current_request.query_params = {}
    app.current_request.query_params["openid_provider"] = openid_provider
    state = base64.b64encode(json.dumps(app.current_request.query_params).encode())
    # TODO: set random state
    # openid_provider = app.current_request.query_params["openid_provider"]
    auth_params = dict(client_id=oauth2_config[openid_provider]["client_id"],
                       response_type="code",
                       scope="openid email",
                       redirect_uri=oauth2_config[openid_provider]["redirect_uri"],
                       state=state)
    dest = get_openid_config(openid_provider)["authorization_endpoint"] + "?" + urlencode(auth_params)
    return Response(status_code=302, headers=dict(Location=dest), body="")


cognito_id_pool_id = "us-east-1:56424a42-0f50-4196-9ac2-fd19df4adb12"

@app.route('/.well-known/openid-configuration')
def serve_openid_config():
    return get_openid_config(openid_provider)

@app.route('/echo')
def echo():
    return str(app.current_request.__dict__)

@app.route('/cb')
def cb():
    # TODO: verify state and JWT
    #if "client_id" in app.current_request.query_params:
        # OIDC flow
    #else:
        # Simplified flow

    state = json.loads(base64.b64decode(app.current_request.query_params["state"]))
    openid_provider = state["openid_provider"]
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
        # OIDC flow
        resp_params = dict(code=app.current_request.query_params["code"], state=state.get("state"))
        dest = state["redirect_uri"] + "?" + urlencode(resp_params)
        return Response(status_code=302, headers=dict(Location=dest), body="")
    else:
        # Simple flow
        res = requests.post(token_endpoint, dict(code=app.current_request.query_params["code"],
                                                 client_id=oauth2_config[openid_provider]["client_id"],
                                                 client_secret=oauth2_config[openid_provider]["client_secret"],
                                                 redirect_uri=oauth2_config[openid_provider]["redirect_uri"],
                                                 grant_type="authorization_code"))
        res.raise_for_status()
        tok = jwt.decode(res.json()["id_token"], verify=False)
        if "redirect_uri" in state:
            # Simple flow - redirect with QS
            resp_params = dict(res.json(), decoded_token=json.dumps(tok), state=state.get("state"))
            dest = state["redirect_uri"] + "?" + urlencode(resp_params)
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
