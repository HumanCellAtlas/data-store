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

terraform_aws_tagging_template = """
locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.project}-${var.env}-${var.service}",
    "project"   , "${var.project}",
    "env"       , "${var.env}",
    "service"   , "${var.service}",
    "owner"     , "${var.owner}"
  )}"   
}
"""


terraform_variable_template = """
variable "{name}" {{
  default = "{val}"
}}
"""

terraform_backend_template = """# Auto-generated during infra build process.
# Please edit infra/build_deploy_config.py directly.
terraform {{
  backend "s3" {{
    bucket = "{bucket}"
    key = "dss/{comp}-{stage}.tfstate"
    region = "{region}"
    {profile_setting}
  }}
}}
"""

terraform_providers_template = """# Auto-generated during infra build process.
# Please edit infra/build_deploy_config.py directly.
provider aws {{
  region = "{aws_region}"
}}

provider google {{
  project = "{gcp_project_id}"
}}
"""

env_vars_to_infra = [
    "API_DOMAIN_NAME",
    "AWS_DEFAULT_OUTPUT",
    "AWS_DEFAULT_REGION",
    "DSS_BLOB_TTL_DAYS",
    "ACM_CERTIFICATE_IDENTIFIER",
    "DSS_CHECKOUT_BUCKET_OBJECT_VIEWERS",
    "DSS_DEPLOYMENT_STAGE",
    "DSS_ES_DOMAIN",
    "DSS_ES_DOMAIN_INDEX_LOGS_ENABLED",
    "DSS_ES_INSTANCE_COUNT",
    "DSS_ES_INSTANCE_TYPE",
    "DSS_ES_VOLUME_SIZE",
    "DSS_GCP_SERVICE_ACCOUNT_NAME",
    "DSS_GS_BUCKET",
    "DSS_GS_BUCKET_INTEGRATION",
    "DSS_GS_BUCKET_PROD",
    "DSS_GS_BUCKET_STAGING",
    "DSS_GS_BUCKET_TEST",
    "DSS_GS_BUCKET_TEST_FIXTURES",
    "DSS_GS_CHECKOUT_BUCKET",
    "DSS_GS_CHECKOUT_BUCKET_PROD",
    "DSS_GS_CHECKOUT_BUCKET_STAGING",
    "DSS_GS_CHECKOUT_BUCKET_TEST",
    "DSS_GS_CHECKOUT_BUCKET_TEST_USER",
    "DSS_S3_BUCKET",
    "DSS_S3_BUCKET_INTEGRATION",
    "DSS_S3_BUCKET_PROD",
    "DSS_S3_BUCKET_STAGING",
    "DSS_S3_BUCKET_TEST",
    "DSS_S3_BUCKET_TEST_FIXTURES",
    "DSS_S3_CHECKOUT_BUCKET",
    "DSS_S3_CHECKOUT_BUCKET_INTEGRATION",
    "DSS_S3_CHECKOUT_BUCKET_PROD",
    "DSS_S3_CHECKOUT_BUCKET_STAGING",
    "DSS_S3_CHECKOUT_BUCKET_TEST",
    "DSS_S3_CHECKOUT_BUCKET_TEST_USER",
    "DSS_S3_CHECKOUT_BUCKET_UNWRITABLE",
    "DSS_SECRETS_STORE",
    "DSS_ZONE_NAME",
    "ES_ALLOWED_SOURCE_IP_SECRETS_NAME",
    "GCP_DEFAULT_REGION",
]


caller_info = boto3.client("sts").get_caller_identity()
if '@' not in caller_info['UserId']:
    raise ValueError('~/.aw/config needs to have an email under the role_session_name')
owner = caller_info['UserId'].split(':')[1]
project = 'dcp'
env = os.environ.get('DSS_DEPLOYMENT_STAGE')
service = 'dss'

with open(os.path.join(infra_root, args.component, "backend.tf"), "w") as fp:
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
    fp.write("# Auto-generated during infra build process." + os.linesep)
    fp.write("# Please edit infra/build_deploy_config.py directly." + os.linesep)
    for key in env_vars_to_infra:
        val = os.environ[key]
        fp.write(terraform_variable_template.format(name=key, val=val))
    
with open(os.path.join(infra_root, args.component, "providers.tf"), "w") as fp:
    fp.write(terraform_providers_template.format(
        aws_region=os.environ['AWS_DEFAULT_REGION'],
        gcp_project_id=GCP_PROJECT_ID,
    ))

with open(os.path.join(infra_root, args.component, "tagging.tf"), "w") as fp:
    fp.write("# Auto-generated during infra build process." + os.linesep)
    fp.write("# Please edit infra/build_deploy_config.py directly." + os.linesep)
    for key in env_vars_to_infra:
        val = os.environ[key]
        fp.write(terraform_variable_template.format(name=key, val=val))
    
with open(os.path.join(infra_root, args.component, "providers.tf"), "w") as fp:
    fp.write(terraform_providers_template.format(
        aws_region=os.environ['AWS_DEFAULT_REGION'],
        gcp_project_id=GCP_PROJECT_ID,
    ))

with open(os.path.join(infra_root, args.component, "tagging.tf"), "w") as fp:
    fp.write("# Auto-generated during infra build process." + os.linesep)
    fp.write("# Please edit infra/build_deploy_config.py directly." + os.linesep)
    fp.write(terraform_variable_template.format(name="project", val=project))
    fp.write(terraform_variable_template.format(name="env", val=env))
    fp.write(terraform_variable_template.format(name="service", val=service))
    fp.write(terraform_variable_template.format(name="owner", val=owner))
    fp.write(terraform_aws_tagging_template)
