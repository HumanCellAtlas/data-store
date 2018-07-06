#!/usr/bin/env python

import os
import glob
import json
import boto3
import argparse
from google.cloud.storage import Client
GCP_PROJECT_ID = Client().project

infra_root = os.path.abspath(os.path.dirname(__file__))


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("component")
args = parser.parse_args()


terraform_backend_template = """terraform {{
  backend "s3" {{
    bucket = "{bucket}"
    key = "dss-{comp}-{stage}.tfstate"
    region = "{region}"
    {profile_setting}
  }}
}}
"""

terraform_providers_template = """
provider aws {{
  region = "{aws_region}"
}}

provider google {{
  project = "{gcp_project_id}"
}}
"""

env_vars_to_infra = [
    "DSS_DEPLOYMENT_STAGE",
    "AWS_DEFAULT_OUTPUT",
    "AWS_DEFAULT_REGION",
    "GCP_DEFAULT_REGION",
    "DSS_S3_BUCKET",
    "DSS_S3_BUCKET_TEST",
    "DSS_S3_BUCKET_TEST_FIXTURES",
    "DSS_S3_BUCKET_INTEGRATION",
    "DSS_S3_BUCKET_STAGING",
    "DSS_S3_BUCKET_PROD",
    "DSS_S3_CHECKOUT_BUCKET",
    "DSS_S3_CHECKOUT_BUCKET_TEST",
    "DSS_S3_CHECKOUT_BUCKET_INTEGRATION",
    "DSS_S3_CHECKOUT_BUCKET_STAGING",
    "DSS_S3_CHECKOUT_BUCKET_PROD",
    "DSS_GS_BUCKET",
    "DSS_GS_BUCKET_TEST",
    "DSS_GS_BUCKET_TEST_FIXTURES",
    "DSS_GS_BUCKET_INTEGRATION",
    "DSS_GS_BUCKET_STAGING",
    "DSS_GS_BUCKET_PROD",
    "DSS_GS_CHECKOUT_BUCKET",
    "DSS_GS_CHECKOUT_BUCKET_TEST",
    "DSS_GS_CHECKOUT_BUCKET_STAGING",
    "DSS_GS_CHECKOUT_BUCKET_PROD",
    "DSS_ES_DOMAIN",
    "DSS_ES_VOLUME_SIZE",
    "DSS_ES_INSTANCE_TYPE",
    "DSS_ES_INSTANCE_COUNT",
    "DSS_SECRETS_STORE",
    "DSS_DEPLOYMENT_STAGE",
    "ES_ALLOWED_SOURCE_IP_SECRETS_NAME",
    "DSS_GS_BUCKET_REGION",
    "DSS_GS_BUCKET_TEST_REGION",
    "DSS_GS_BUCKET_TEST_FIXTURES_REGION",
    "API_DOMAIN_NAME",
    "DSS_CERTIFICATE_DOMAIN",
    "DSS_ZONE_NAME",
]

terraform_variable_info = {'variable': dict()}
for key in env_vars_to_infra:
    terraform_variable_info['variable'][key] = {
        'default': os.environ[key]
    }

with open(os.path.join(infra_root, args.component, "backend.tf"), "w") as fp:
    caller_info = boto3.client("sts").get_caller_identity()
    if os.environ.get('AWS_PROFILE'):
        profile = os.environ['AWS_PROFILE']
        profile_setting = f'profile = "{profile}"'
    else:
        profile_setting = ''
    fp.write(terraform_backend_template.format(
        bucket=os.environ['DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE'].format(account_id=caller_info['Account']),
        comp=args.component,
        stage=os.environ['DSS_DEPLOYMENT_STAGE'],
        region=os.environ['AWS_DEFAULT_REGION'],
        profile_setting=profile_setting,
    ))

with open(os.path.join(infra_root, args.component, "variables.tf"), "w") as fp:
    fp.write(json.dumps(terraform_variable_info, indent=2))

with open(os.path.join(infra_root, args.component, "providers.tf"), "w") as fp:
    fp.write(terraform_providers_template.format(
        aws_region=os.environ['AWS_DEFAULT_REGION'],
        gcp_project_id=GCP_PROJECT_ID,
    ))
