"""
Obtain information about IAM (policies and permissions) from cloud providers.
"""
import os
import sys
import select
import typing
import argparse
import json
import requests
import logging
from functools import lru_cache

from botocore.exceptions import ClientError

from dcplib.security import DCPServiceAccountManager
from dss.operations import dispatch
from dss.util.aws.clients import iam as iam_client  # type: ignore

"""
This module is quite long, so here is a table of contents and summary.

Table of Contents:
- fusillade utility functions
- functions to dump/list all policies
    - cloud providers have separate, parallel sets of methods
    - all cloud providers have an extract method to extract policy information
    - all cloud providers have wrapper methods to list/dump the information
- functions to dump/list policies grouped by asset type
    - an asset is a user, a group, or a role
    - these functions have a different logical structure from the above
    - aws has a single function to list policies grouped by asset
    - fusillade has separate functions for each type of asset: users, groups, and roles
- dss-ops command line utility functionality
    - list action
"""


logger = logging.getLogger(__name__)


IAMSEPARATOR = " : "
ANONYMOUS_POLICY_NAME = "UNNAMED_POLICY"


# ---
# Fusillade utility functions/classes
# ---
class FusilladeClient(object):
    """
    Fusillade client.
    A simple wrapper around an authorization URL and a header.
    """
    pass

def get_fus_role_attached_policies(fus_client, action, role):
    """
    Get policies attached to a Fusillade role using /v1/role/{role} and requesting the policies field.
    """
    pass


# ---
# Dump/list AWS policies
# ---
def extract_aws_policies(action: str, client, managed: bool):
    """
    Call the AWS IAM API to retrieve policies and perform an action with them.
    """
    pass

def list_aws_policies(client, managed: bool):
    """Return a list of names of AWS policies"""
    pass

def dump_aws_policies(client, managed: bool):
    """Return a list of dictionaries containing AWS policy documents"""
    pass


# ---
# Dump/list GCP policies
# ---
def extract_gcp_policies(action: str, client, managed: bool):
    """
    Call the GCP IAM API to retrieve policies and perform an action with them.
    """
    pass

def list_gcp_policies(client, managed: bool):
    """Return a list of names of GCP policies"""
    pass

def dump_gcp_policies(client, managed: bool):
    """Return a list of dictionaries containing GCP policy documents"""
    pass


# ---
# Dump/list Fusillade policies
# ---
def extract_fus_policies(action: str, fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies and perform an action with them.
    """
    pass

def list_fus_policies(fus_client, do_headers) -> typing.List[str]:
    """Return a list of names of Fusillade policies"""
    pass

def dump_fus_policies(fus_client, do_headers):
    """Return a list of dictionaries containing Fusillade policy documents"""
    pass


# ---
# List AWS policies grouped by asset type
# ---
def list_aws_policies_grouped(asset_type, client, managed: bool, do_headers: bool = True):
    """
    Call the AWS IAM API to retrieve policies grouped by asset and create a list of policy names.
    """
    pass

def list_aws_user_policies(*args, **kwargs):
    """Extract a list of policies that apply to each user"""
    pass

def list_aws_group_policies(*args, **kwargs):
    """Extract a list of policies that apply to each group"""
    pass

def list_aws_role_policies(*args, **kwargs):
    """Extract a list of policies that apply to each resource"""
    pass


# ---
# List GCP policies grouped by asset type
# ---
def list_gcp_policies_grouped(asset_type, client, managed: bool, do_headers: bool = True):
    """
    Call the GCP IAM API to retrieve policies grouped by asset and create a list of policy names.
    """
    pass

def list_gcp_user_policies(*args, **kwargs):
    """Extract a list of policies that apply to each user"""
    pass

def list_gcp_group_policies(*args, **kwargs):
    """Extract a list of policies that apply to each group"""
    pass

def list_gcp_role_policies(*args, **kwargs):
    """Extract a list of policies that apply to each resource"""
    pass


# ---
# List Fusillade policies grouped by asset type
# ---
def list_fus_user_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by user.
    """
    pass

def list_fus_group_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by group.
    """
    pass

def list_fus_role_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by role.
    """
    pass


# ---
# dss-ops.py command line utility
# ---
iam = dispatch.target("iam", arguments={}, help=__doc__)


@iam.action(
    "list",
    arguments={
        "cloud_provider": dict(
            choices=["aws", "gcp", "fusillade"], help="The cloud provider whose policies are being listed"
        ),
        "--group-by": dict(
            required=False,
            choices=["users", "groups", "roles"],
            help="Group the listed policies by asset type (user, group, or role)",
        ),
        "--output": dict(
            type=str, required=False, help="Specify an output file name (output sent to stdout by default)"
        ),
        "--force": dict(
            action="store_true",
            help="If output file already exists, overwrite it (default is not to overwrite)",
        ),
        "--include-managed": dict(
            action="store_true", help="Include policies provided and managed by the cloud provider"
        ),
        "--exclude-headers": dict(
            action="store_true", help="Exclude headers on the list being output"
        )
    },
)
def list_policies(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM policy names, optionally grouped by asset"""
    if args.output:
        if os.path.exists(args.output) and not args.force:
            raise RuntimeError(f"Error: cannot overwrite {args.output} without --force flag")

    if args.cloud_provider == "aws":
        pass
    elif args.cloud_provider == "gcp":
        pass
    elif args.cloud_provider == "fusillade":
        pass
    else:
        raise RuntimeError(f"Error: IAM functionality not implemented for {args.cloud_provider}")
