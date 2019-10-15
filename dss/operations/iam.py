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

    :param action: what action to take with the IAM policies (list, dump)
    :param client: the boto client to use
    :param managed: (boolean) if true, include AWS-managed policies
    :returns: a list of items whose type depends on the action param
        (policy names if action is list, json documents if action is dump)
    """
    master_list = []  # holds main results

    if managed:
        paginator_scope = "All"
    else:
        paginator_scope = "Local"

    paginator = client.get_paginator("list_policies")
    for page in paginator.paginate(Scope=paginator_scope):
        for policy in page["Policies"]:

            if action == "list":
                policy_name = policy["PolicyName"]
                master_list.append(policy_name)
            elif action == "dump":
                # first save as strings
                master_list.append(json.dumps(policy, sort_keys=True, default=str))

    if action == "list":
        # Sort names, remove duplicates
        master_list = sorted(list(set(master_list)))
    elif action == "dump":
        # Convert to strings, remove dupes, convert to back dicts
        master_list = list(set(master_list))
        master_list = [json.loads(j) for j in master_list]

    # also need to get all inline policies
    return master_list


def list_aws_policies(client, managed: bool):
    """Return a list of names of AWS policies"""
    return extract_aws_policies("list", client, managed)


def dump_aws_policies(client, managed: bool):
    """Return a list of dictionaries containing AWS policy documents"""
    return extract_aws_policies("dump", client, managed)


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

    :param asset_type: the type of asset to group policies by ("users", "groups", or "roles")
    :param client: the boto client to use
    :param managed: (boolean) if true, include AWS-managed policies
    :returns: list of tuples of two strings in the form (asset_name, policy_name)
    """
    extracted_list = []

    # Prepare to extract labels for JSON returned by API
    def _make_aws_api_labels_dict_entry(*args) -> typing.Dict[str, str]:
        """
        Convenience function to unpack 4 values into a dictionary of labels, useful for processing
        API results.

        :params arg[0]: label of asset type
        :params arg[1]: label of asset name
        :params arg[2]: label of asset details
        :params arg[3]: label of asset policy details
        :returns: dictionary of organized labels
        """
        assert len(args) == 4, "Error: need 4 arguments!"
        return dict(
            extracted_list_label=args[0], name_label=args[1], detail_list_label=args[2], policy_list_label=args[3]
        )

    def _get_aws_api_labels_dict() -> typing.Dict[str, typing.Dict[str, str]]:
        """Store the labels used to unwrap JSON results from the AWS API"""
        labels = {
            "user": _make_aws_api_labels_dict_entry("User", "UserName", "UserDetailList", "UserPolicyList"),
            "group": _make_aws_api_labels_dict_entry("Group", "GroupName", "GroupDetailList", "GroupPolicyList"),
            "role": _make_aws_api_labels_dict_entry("Role", "RoleName", "RoleDetailList", "RolePolicyList"),
        }
        return labels

    # Extract labels needed
    labels = _get_aws_api_labels_dict()
    if asset_type not in labels:
        raise RuntimeError(f"Error: asset type {asset_type} is not valid, try one of: {labels}")
    extracted_list_label, filter_label, name_label, detail_list_label, policy_list_label = (
        labels[asset_type]["extracted_list_label"],
        labels[asset_type]["extracted_list_label"],
        labels[asset_type]["name_label"],
        labels[asset_type]["detail_list_label"],
        labels[asset_type]["policy_list_label"],
    )

    # Get the response, using paging if necessary
    response_detail_list: typing.List[typing.Any] = []
    paginator = client.get_paginator("get_account_authorization_details")
    for page in paginator.paginate(Filter=[filter_label]):
        response_detail_list += page[detail_list_label]

    # Inline vs managed policies:
    # https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_managed-vs-inline.html

    # Order of enumeration:
    # 1. inline policies
    # 2. managed policies

    for asset_detail in response_detail_list:
        asset_name = asset_detail[name_label]

        # 1. Inline policies
        # Check if any policies are present
        if policy_list_label in asset_detail:
            for inline_policy in asset_detail[policy_list_label]:
                policy_name = inline_policy["PolicyName"]

                extracted_list.append((asset_name, policy_name))

        # 2. Managed policies
        # Check if any managed policies are present
        if "AttachedManagedPolicies" in asset_detail:
            # Listing of managed policies
            for managed_policy in asset_detail["AttachedManagedPolicies"]:
                policy_name = managed_policy["PolicyName"]
                policy_arn = managed_policy["PolicyArn"]
                arn_scope = policy_arn.split("::")[1].split(":")[0]

                # Make sure this is a policy we want to include in our final returned results
                if (managed and arn_scope == "aws") or (arn_scope != "aws"):
                    extracted_list.append((asset_name, policy_name))

    # Eliminate dupes
    extracted_list = sorted(list(set(extracted_list)))

    if do_headers:
        # Add headers
        extracted_list = [(extracted_list_label, "Policy")] + extracted_list

    return extracted_list


def list_aws_user_policies(*args, **kwargs):
    """Extract a list of policies that apply to each user"""
    return list_aws_policies_grouped("user", *args, **kwargs)


def list_aws_group_policies(*args, **kwargs):
    """Extract a list of policies that apply to each group"""
    return list_aws_policies_grouped("group", *args, **kwargs)


def list_aws_role_policies(*args, **kwargs):
    """Extract a list of policies that apply to each resource"""
    return list_aws_policies_grouped("role", *args, **kwargs)


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
        "--exclude-headers": dict(action="store_true", help="Exclude headers on the list being output"),
    },
)
def list_policies(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM policy names, optionally grouped by asset"""
    if args.output:
        if os.path.exists(args.output) and not args.force:
            raise RuntimeError(f"Error: cannot overwrite {args.output} without --force flag")

    managed = args.include_managed
    do_headers = not args.exclude_headers

    if args.cloud_provider == "aws":

        if args.group_by is None:
            contents = list_aws_policies(iam_client, managed)
        else:
            if args.group_by == "users":
                contents = list_aws_user_policies(iam_client, managed, do_headers)
            elif args.group_by == "groups":
                contents = list_aws_group_policies(iam_client, managed, do_headers)
            elif args.group_by == "roles":
                contents = list_aws_role_policies(iam_client, managed, do_headers)

            # Join the tuples
            contents = [IAMSEPARATOR.join(c) for c in contents]

    elif args.cloud_provider == "gcp":
        pass
    elif args.cloud_provider == "fusillade":
        pass
    else:
        raise RuntimeError(f"Error: IAM functionality not implemented for {args.cloud_provider}")

    # Print list to output
    if args.output:
        stdout_ = sys.stdout
        sys.stdout = open(args.output, "w")
    for c in contents:
        print(c)
    if args.output:
        sys.stdout = stdout_
