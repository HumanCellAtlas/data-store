import os, sys, json, logging, base64, re

import boto3
from botocore.vendored import requests

class GCPAPIClient:
    instance_metadata_url = "http://metadata.google.internal/computeMetadata/v1/"
    svc_acct_token_url = instance_metadata_url + "instance/service-accounts/default/token"
    svc_acct_email_url = instance_metadata_url + "instance/service-accounts/default/email"
    project_id_metadata_url = instance_metadata_url + "project/project-id"

    def __init__(self, **session_kwargs):
        self._project = None
        self._session = None
        self._session_kwargs = session_kwargs

    def get_session(self):
        if self._session is None:
            self._session = requests.Session(**self._session_kwargs)
            self._session.headers.update(Authorization="Bearer " + self.get_oauth2_token())
        return self._session

    def get_oauth2_token(self):
        res = requests.get(self.svc_acct_token_url, headers={"Metadata-Flavor": "Google"})
        res.raise_for_status()
        return res.json()["access_token"]

    def request(self, method, resource, **kwargs):
        res = self.get_session().request(method=method, url=self.base_url + resource, **kwargs)
        res.raise_for_status()
        return res if kwargs.get("stream") is True or method == "delete" else res.json()

    def get(self, resource, **kwargs):
        return self.request(method="get", resource=resource, **kwargs)

    def post(self, resource, **kwargs):
        return self.request(method="post", resource=resource, **kwargs)

    def patch(self, resource, **kwargs):
        return self.request(method="patch", resource=resource, **kwargs)

    def put(self, resource, **kwargs):
        return self.request(method="put", resource=resource, **kwargs)

    def delete(self, resource, **kwargs):
        return self.request(method="delete", resource=resource, **kwargs)

    def get_project(self):
        if self._project is None:
            res = requests.get(self.project_id_metadata_url, headers={"Metadata-Flavor": "Google"})
            self._project = res.content.decode()
        return self._project

class GRTCClient(GCPAPIClient):
    base_url = "https://runtimeconfig.googleapis.com/v1beta1/"

def dss_gs_event_relay(data, context):
    if data["resourceState"] == "not_exists":
        print("Ignoring object deletion event")
    elif data["metageneration"] == 1:
        # Metageneration is updated on metadata changes and starts at 1
        print("Ignoring object metadata update event")
    elif re.match(r".+\.part\d+$", data["name"]):
        print("Ignoring multipart object upload event")
    else:
        grtc_client = GRTCClient()
        config_ns = f"projects/{grtc_client.get_project()}/configs"
        var_ns = f"{config_ns}/{os.environ['ENTRY_POINT']}/variables"
        for config_var in "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "sqs_queue_url":
            os.environ[config_var] = base64.b64decode(grtc_client.get(f"{var_ns}/{config_var}")["value"]).decode()
        sqs = boto3.resource("sqs")
        queue = sqs.Queue(os.environ["sqs_queue_url"])
        print(json.dumps(queue.send_message(MessageBody=json.dumps(data))))

globals()[os.environ["ENTRY_POINT"]] = dss_gs_event_relay
