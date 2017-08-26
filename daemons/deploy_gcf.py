#!/usr/bin/env python
"""
This script manages the deployment of Google Cloud Functions.
"""

import os, sys, time, io, zipfile, random, string, binascii, datetime, argparse, base64

import boto3
import google.cloud.storage
import google.api.core.exceptions
from google.cloud.client import ClientWithProject
from google.cloud._http import JSONConnection

class GCPClient(ClientWithProject):
    SCOPE = ["https://www.googleapis.com/auth/cloud-platform",
             "https://www.googleapis.com/auth/cloudruntimeconfig"]

class GoogleCloudFunctionsConnection(JSONConnection):
    API_BASE_URL = "https://cloudfunctions.googleapis.com"
    API_VERSION = "v1beta2"
    API_URL_TEMPLATE = "{api_base_url}/{api_version}{path}"

class GoogleRuntimeConfigConnection(JSONConnection):
    API_BASE_URL = "https://runtimeconfig.googleapis.com"
    API_VERSION = "v1beta1"
    API_URL_TEMPLATE = "{api_base_url}/{api_version}{path}"

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("gcf_name", help="Name of directory containing cloud function source")
parser.add_argument("--entry-point", help="Name of entry point to deploy with", required=True)
args = parser.parse_args()

gcp_region = os.environ["GCP_DEFAULT_REGION"]
gcp_key_file = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
gs = google.cloud.storage.Client.from_service_account_json(gcp_key_file)
gcp_client = GCPClient()
grtc_conn = GoogleRuntimeConfigConnection(client=gcp_client)
gcf_conn = GoogleCloudFunctionsConnection(client=gcp_client)
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
except google.api.core.exceptions.Conflict:
    print(f"GRTC config {args.entry_point} found")

var_ns = f"{config_ns}/{args.entry_point}/variables"
for k, v in config_vars.items():
    print("Writing GRTC variable", k)
    b64v = base64.b64encode(v.encode()).decode()
    try:
        grtc_conn.api_request("POST", f"/{var_ns}", data=dict(name=f"{var_ns}/{k}", value=b64v))
    except google.api.core.exceptions.Conflict:
        grtc_conn.api_request("PUT", f"/{var_ns}/{k}", data=dict(name=f"{var_ns}/{k}", value=b64v))

now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
deploy_filename = "{}-deploy-{}-{}.zip".format(args.gcf_name, now, binascii.hexlify(os.urandom(4)).decode())
deploy_blob = gs.bucket(os.environ["DSS_GS_BUCKET_TEST_FIXTURES"]).blob(deploy_filename)
with io.BytesIO() as buf:
    with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED) as zbuf:
        for root, dirs, files in os.walk(args.gcf_name):
            for f in files:
                archive_path = os.path.relpath(os.path.join(root, f), args.gcf_name)
                if archive_path.startswith("node_modules"):
                    continue
                print("Adding", archive_path)
                zbuf.write(os.path.join(root, f), archive_path)
        zbuf.close()
    deploy_blob.upload_from_string(buf.getvalue())
    print("Uploaded", deploy_blob)

gcf_data = {
    "name": f"{gcf_ns}/{args.gcf_name}",
    "entryPoint": args.entry_point,
    "timeout": "60s",
    "availableMemoryMb": 256,
    "sourceArchiveUrl": f"gs://{deploy_blob.bucket.name}/{deploy_blob.name}",
    "eventTrigger": {
        "eventType": "providers/cloud.storage/eventTypes/object.change",
        "resource": f"projects/_/buckets/{os.environ['DSS_GS_BUCKET']}"
    }
}

try:
    print(gcf_conn.api_request("POST", f"/{gcf_ns}", data=gcf_data))
except google.api.core.exceptions.Conflict:
    print(gcf_conn.api_request("PUT", f"/{gcf_ns}/{args.gcf_name}", data=gcf_data))

sys.stderr.write("Waiting for deployment...")
sys.stderr.flush()
while gcf_conn.api_request("GET", f"/{gcf_ns}/{args.gcf_name}")["status"] == "DEPLOYING":
    sys.stderr.write(".")
    sys.stderr.flush()
    time.sleep(1)
sys.stderr.write("done\n")

res = gcf_conn.api_request("GET", f"/{gcf_ns}/{args.gcf_name}")
print(res)
assert gcf_conn.api_request("GET", f"/{gcf_ns}/{args.gcf_name}")["status"] == "READY"

deploy_blob.delete()
