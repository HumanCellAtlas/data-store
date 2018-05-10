#!/usr/bin/env python

import os
import glob
import json
import boto3
from google.cloud.storage import Client
GCP_PROJECT_ID = Client()._credentials.project_id

infra_root = os.path.abspath(os.path.dirname(__file__))

terraform_backend_template = """terraform {{
  backend "s3" {{
    bucket = "{bucket}"
    key = "dss-{comp}-{stage}.tfstate"
    region = "{region}"
  }}
}}
"""

terraform_providers_template = """
provider aws {{
  region = "{aws_region}"
}}

provider aws {{
  region = "us-east-1"
  alias = "us-east-1"
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
]

terraform_variable_info = {'variable': dict()}
for key in env_vars_to_infra:
    terraform_variable_info['variable'][key] = {
        'default': os.environ[key]
    }
ip_list = boto3.client("secretsmanager").get_secret_value(
    SecretId="{}/{}/{}".format(
        os.environ['DSS_SECRETS_STORE'],
        os.environ['DSS_DEPLOYMENT_STAGE'],
        os.environ['ES_ALLOWED_SOURCE_IP_SECRETS_NAME'],
    )
)['SecretString'].strip()
terraform_variable_info['variable']['es_source_ip'] = {
    'default': json.loads(ip_list)
}

for comp in glob.glob(os.path.join(infra_root, "*/")):
    if 0 == len(glob.glob(os.path.join(infra_root, comp, "*.tf"))):
        # No terraform content in this directory
        continue

    with open(os.path.join(infra_root, comp, "backend.tf"), "w") as fp:
        info = boto3.client("sts").get_caller_identity()
        fp.write(terraform_backend_template.format(
            bucket=os.environ['DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE'].format(account_id=info['Account']),
            comp=comp,
            stage=os.environ['DSS_DEPLOYMENT_STAGE'],
            region=os.environ['AWS_DEFAULT_REGION'],
        ))

    with open(os.path.join(infra_root, comp, "variables.tf"), "w") as fp:
        fp.write(json.dumps(terraform_variable_info, indent=2))

    with open(os.path.join(infra_root, comp, "providers.tf"), "w") as fp:
        fp.write(terraform_providers_template.format(
            aws_region=os.environ['AWS_DEFAULT_REGION'],
            gcp_project_id=GCP_PROJECT_ID,
        ))
