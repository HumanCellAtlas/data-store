#!/usr/bin/env python
"""
This script manages the deployment of Google Cloud Functions.
"""

import os, sys, time, io, zipfile, random, string, binascii, datetime, argparse, base64, subprocess

import boto3
import google.cloud.storage
import google.cloud.exceptions
from google.cloud.client import ClientWithProject
from google.cloud._http import JSONConnection
from urllib3.util.retry import Retry

class GCPClient(ClientWithProject):
    SCOPE = ["https://www.googleapis.com/auth/cloud-platform",
             "https://www.googleapis.com/auth/cloudruntimeconfig"]

class GoogleRuntimeConfigConnection(JSONConnection):
    API_BASE_URL = "https://runtimeconfig.googleapis.com"
    API_VERSION = "v1beta1"
    API_URL_TEMPLATE = "{api_base_url}/{api_version}{path}"

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("src_dir", help="Name of directory containing cloud function source")
parser.add_argument("--entry-point", help="Name of entry point to deploy with", required=True)
args = parser.parse_args()
args.gcf_name = "-".join([args.src_dir, os.environ["DSS_DEPLOYMENT_STAGE"]])

gcp_region = os.environ["GCP_DEFAULT_REGION"]
gcp_key_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
gs = google.cloud.storage.Client.from_service_account_json(gcp_key_file)
gcp_client = GCPClient()
gcp_client._http.adapters["https://"].max_retries = Retry(status_forcelist={503, 504})
grtc_conn = GoogleRuntimeConfigConnection(client=gcp_client)
gcf_ns = f"projects/{gcp_client.project}/locations/{gcp_region}/functions"

boto3_session = boto3.session.Session()
aws_account_id = boto3.client("sts").get_caller_identity()["Account"]
relay_sns_topic = "dss-gs-bucket-events-" + os.environ["DSS_GS_BUCKET"]
config_vars = {
    "AWS_ACCESS_KEY_ID": boto3_session.get_credentials().access_key,
    "AWS_SECRET_ACCESS_KEY": boto3_session.get_credentials().secret_key,
    "AWS_REGION": boto3_session.region_name,
    "sns_topic_arn": f"arn:aws:sns:{boto3_session.region_name}:{aws_account_id}:{relay_sns_topic}"
}

config_ns = f"projects/{gcp_client.project}/configs"
try:
    print(grtc_conn.api_request("POST", f"/{config_ns}", data=dict(name=f"{config_ns}/{args.entry_point}")))
except google.cloud.exceptions.Conflict:
    print(f"GRTC config {args.entry_point} found")

var_ns = f"{config_ns}/{args.entry_point}/variables"
for k, v in config_vars.items():
    print("Writing GRTC variable", k)
    b64v = base64.b64encode(v.encode()).decode()
    try:
        grtc_conn.api_request("POST", f"/{var_ns}", data=dict(name=f"{var_ns}/{k}", value=b64v))
    except google.cloud.exceptions.Conflict:
        grtc_conn.api_request("PUT", f"/{var_ns}/{k}", data=dict(name=f"{var_ns}/{k}", value=b64v))

subprocess.run([
    'gcloud', 'beta', 'functions', 'deploy', args.gcf_name,
    '--timeout=%i' % 60,
    '--memory=%i' % 256,
    '--source=%s' % args.src_dir,
    '--entry-point=%s' % args.entry_point,
    '--trigger-bucket=%s' % os.environ["DSS_GS_BUCKET"],
], timeout=240)
