#!/usr/bin/env python
"""
This script manages the deployment of Google Cloud Functions.
"""

import os, sys, time, io, zipfile, random, string, binascii, datetime, argparse, base64

import json
import boto3
import socket
import tempfile
import httplib2
import google.cloud.storage
import google.cloud.exceptions
from apitools.base.py import http_wrapper
from google.cloud.client import ClientWithProject
from google.cloud._http import JSONConnection
from urllib3.util.retry import Retry

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
parser.add_argument("src_dir", help="Name of directory containing cloud function source")
parser.add_argument("--entry-point", help="Name of entry point to deploy with", required=True)
args = parser.parse_args()
args.gcf_name = "-".join([args.src_dir, os.environ["DSS_DEPLOYMENT_STAGE"]])

gcp_region = os.environ["GCP_DEFAULT_REGION"]
with tempfile.NamedTemporaryFile() as fp:
    creds = boto3.client("secretsmanager").get_secret_value(
        SecretId='{}/{}/{}'.format(
            os.environ['DSS_SECRETS_STORE'],
            os.environ['DSS_DEPLOYMENT_STAGE'],
            "gcp-credentials.json"
        )
    )['SecretString']
    fp.write(creds.encode("utf-8"))
    fp.flush()
    gcp_client = GCPClient.from_service_account_json(fp.name)
gcp_client._http.adapters["https://"].max_retries = Retry(status_forcelist={503, 504})
grtc_conn = GoogleRuntimeConfigConnection(client=gcp_client)
gcf_conn = GoogleCloudFunctionsConnection(client=gcp_client)
gcf_ns = f"projects/{gcp_client.project}/locations/{gcp_region}/functions"

aws_access_key_info = json.loads(
    boto3.client('secretsmanager').get_secret_value(
        SecretId='{}/{}/{}'.format(
            os.environ['DSS_SECRETS_STORE'],
            os.environ['DSS_DEPLOYMENT_STAGE'],
            os.environ['EVENT_RELAY_AWS_ACCESS_KEY_SECRETS_NAME']
        )
    )['SecretString']
)

boto3_session = boto3.session.Session()
aws_account_id = boto3.client("sts").get_caller_identity()["Account"]
relay_sns_topic_name = "dss-gs-bucket-events-" + os.environ["DSS_GS_BUCKET"]
relay_sns_topic = boto3_session.resource("sns").create_topic(Name=relay_sns_topic_name)

sync_sqs_queue_name = "dss-sync-" + os.environ["DSS_DEPLOYMENT_STAGE"]
sync_sqs_queue = boto3_session.resource("sqs").create_queue(QueueName=sync_sqs_queue_name)
sender_arn = f"arn:aws:sns:*:{aws_account_id}:{relay_sns_topic_name}"
queue_access_policy = {"Statement": [{"Sid": "dss-deploy-gcf-qap",
                                      "Action": ["SQS:SendMessage"],
                                      "Effect": "Allow",
                                      "Resource": sync_sqs_queue.attributes["QueueArn"],
                                      "Principal": {"AWS": "*"},
                                      "Condition": {"ArnLike": {"aws:SourceArn": sender_arn}}}]}
sync_sqs_queue.set_attributes(Attributes=dict(Policy=json.dumps(queue_access_policy)))

relay_sns_topic.subscribe(Protocol="sqs", Endpoint=sync_sqs_queue.attributes["QueueArn"])

config_vars = {
    "AWS_ACCESS_KEY_ID": aws_access_key_info['AccessKey']['AccessKeyId'],
    "AWS_SECRET_ACCESS_KEY": aws_access_key_info['AccessKey']['SecretAccessKey'],
    "AWS_DEFAULT_REGION": os.environ['AWS_DEFAULT_REGION'],
    "sns_topic_arn": relay_sns_topic.arn
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

resp = gcf_conn.api_request('POST', f'/{gcf_ns}:generateUploadUrl', content_type='application/zip')
upload_url = resp['uploadUrl']

now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
deploy_filename = "{}-deploy-{}-{}.zip".format(args.gcf_name, now, binascii.hexlify(os.urandom(4)).decode())
with io.BytesIO() as buf:
    with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED) as zbuf:
        for root, dirs, files in os.walk(args.src_dir):
            for f in files:
                archive_path = os.path.relpath(os.path.join(root, f), args.src_dir)
                print("Adding", archive_path)
                zbuf.write(os.path.join(root, f), archive_path)
        zbuf.close()

    upload_data = buf.getvalue()

    # BEGIN: xbrianh - Code reproduced-ish from Google gcloud utility
    upload_request = http_wrapper.Request(
        upload_url, http_method='PUT',
        headers={
            'content-type': 'application/zip',
            # Magic header, request will fail without it.
            # Not documented at the moment this comment was being written.
            'x-goog-content-length-range': '0,104857600',
            'Content-Length': '{0:d}'.format(len(upload_data))
        }
    )
    upload_request.body = upload_data
    if socket.getdefaulttimeout() is not None:
        http_timeout = socket.getdefaulttimeout()
    else:
        http_timeout = 60
    response = http_wrapper.MakeRequest(
        httplib2.Http(timeout=http_timeout),
        upload_request,
    )
    # END

gcf_config = {
    "name": f"{gcf_ns}/{args.gcf_name}",
    "runtime": "python37",
    "entryPoint": args.entry_point,
    "timeout": "60s",
    "availableMemoryMb": 256,
    "sourceUploadUrl": upload_url,
    "environmentVariables": {},
    "eventTrigger": {
        "eventType": "providers/cloud.storage/eventTypes/object.change",
        "resource": "projects/_/buckets/" + os.environ['DSS_GS_BUCKET']
    }
}

try:
    deploy_op = gcf_conn.api_request("POST", f"/{gcf_ns}", data=gcf_config)
except google.cloud.exceptions.Conflict:
    deploy_op = gcf_conn.api_request("PUT", f"/{gcf_ns}/{args.gcf_name}", data=gcf_config)

sys.stderr.write("Waiting for deployment...")
sys.stderr.flush()
for t in range(600):
    if gcf_conn.api_request("GET", f"/{deploy_op['name']}").get("response", {}).get("status") == "READY":
        break
    sys.stderr.write(".")
    sys.stderr.flush()
    time.sleep(5)
else:
    sys.exit("Timeout while waiting for GCF deployment to complete")
sys.stderr.write("done\n")

res = gcf_conn.api_request("GET", f"/{gcf_ns}/{args.gcf_name}")
print(res)
assert res["status"] == "READY"
