#!/usr/bin/env python
"""
This script generates Terraform scripting needed for daemons that deploy infrastructure.
"""

import os
import glob
import json
import boto3
import argparse


daemons_root = os.path.abspath(os.path.dirname(__file__))


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("daemon")
args = parser.parse_args()


env_vars_to_lambda = os.environ['EXPORT_ENV_VARS_TO_LAMBDA'].split()


terraform_backend_template = """terraform {{
  backend "s3" {{
    bucket = "{bucket}"
    key = "dss-{daemon}-{stage}.tfstate"
    region = "{region}"
    {profile_setting}
  }}
}}
"""

terraform_providers_template = """
provider aws {{
  region = "{aws_region}"
}}
"""

account_id = boto3.client("sts").get_caller_identity()['Account']
backend_bucket = os.environ['DSS_TERRAFORM_BACKEND_BUCKET_TEMPLATE'].format(account_id=account_id)

terraform_variable_info = {'variable': dict()}
for key in env_vars_to_lambda:
    terraform_variable_info['variable'][key] = {
        'default': os.environ[key]
    }

with open(os.path.join(daemons_root, args.daemon, "backend.tf"), "w") as fp:
    if os.environ.get('AWS_PROFILE'):
        profile = os.environ['AWS_PROFILE']
        profile_setting = f'profile = "{profile}"'
    else:
        profile_setting = ''
    fp.write(terraform_backend_template.format(
        bucket=backend_bucket,
        daemon=args.daemon,
        stage=os.environ['DSS_DEPLOYMENT_STAGE'],
        region=os.environ['AWS_DEFAULT_REGION'],
        profile_setting=profile_setting,
    ))

with open(os.path.join(daemons_root, args.daemon, "variables.tf"), "w") as fp:
    fp.write(json.dumps(terraform_variable_info, indent=2))

with open(os.path.join(daemons_root, args.daemon, "providers.tf"), "w") as fp:
    fp.write(terraform_providers_template.format(
        aws_region=os.environ['AWS_DEFAULT_REGION'],
    ))
