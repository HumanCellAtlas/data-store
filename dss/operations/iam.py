"""
Obtain information about IAM (policies and permissions) from cloud providers.
"""
import os
import sys
import select
import typing
import argparse
import json
import logging

from botocore.exceptions import ClientError

from dss.operations import dispatch
from dss.util.aws.clients import iam as iam_client  # type: ignore


logger = logging.getLogger(__name__)


SEPARATOR = " : "


def _make_aws_api_labels_dict_entry(*args):
    """
    Convenience function to unpack 4 values into a dictionary of labels, useful for processing
    API results.
    """
    assert len(args) == 4, "Error: need 4 arguments!"
    return dict(
        extracted_list_label=args[0], name_label=args[1], detail_list_label=args[2], policy_list_label=args[3]
    )


def _get_aws_api_labels_dict():
    labels = {
        "user": _make_aws_api_labels_dict_entry("User", "UserName", "UserDetailList", "UserPolicyList"),
        "group": _make_aws_api_labels_dict_entry("Group", "GroupName", "GroupDetailList", "GroupPolicyList"),
        "role": _make_aws_api_labels_dict_entry("Role", "RoleName", "RoleDetailList", "RolePolicyList"),
    }
    return labels


# ---
# All policies
# ---
def extract_aws_policies(action, client, managed):
    """
    Call the AWS IAM API to retrieve policies and perform an action with them.

    :param action: what action to take with the IAM policies (list, dump)
    :param client: the boto client to use
    :param managed: (boolean) if true, include AWS-managed policies
    :returns: a list of items (policy names if action is list
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
        # Sort strings and remove duplicates,
        # then convert back to dictionaries
        master_list = list(set(master_list))
        master_list = [json.loads(j) for j in master_list]

    # also need to get all inline policies
    return master_list


def list_aws_policies(client, managed):
    """Extract a list of policies"""
    return extract_aws_policies("list", client, managed)


def dump_aws_policies(client, managed):
    """Dump policy documents to JSON"""
    return extract_aws_policies("dump", client, managed)


# ---
# Policies grouped by asset type
# ---
def list_aws_policies_grouped(asset_type, client, managed):
    """
    Call the AWS IAM API to retrieve policies grouped by asset and create a list of policy names.

    :param asset_type: the type of asset to group policies by
    :param client: the boto client to use
    :param managed: (boolean) if true, include AWS-managed policies
    :returns: a list of items of the form (asset_name, policy_name)
    """
    extracted_list = []

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
    response_detail_list = []
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

    # Add headers to the final results list
    extracted_list = [(extracted_list_label, "PolicyName")] + extracted_list

    return extracted_list


def list_aws_user_policies(*args):
    """Extract a list of policies that apply to each user"""
    return list_aws_policies_grouped("user", *args)


def list_aws_group_policies(*args):
    """Extract a list of policies that apply to each group"""
    return list_aws_policies_grouped("group", *args)


def list_aws_role_policies(*args):
    """Extract a list of policies that apply to each resource"""
    return list_aws_policies_grouped("role", *args)


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
    },
)
def list_policies(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM policy names, optionally grouped by asset"""
    if args.output:
        if os.path.exists(args.output) and not args.force:
            raise RuntimeError(f"Error: cannot overwrite {args.output} without --force flag")

    managed = False
    if args.cloud_provider == "aws":

        if args.group_by is None:
            contents = list_aws_policies(iam_client, managed)
        else:
            if args.group_by == "users":
                contents = list_aws_user_policies(iam_client, managed)
            elif args.group_by == "groups":
                contents = list_aws_group_policies(iam_client, managed)
            elif args.group_by == "roles":
                contents = list_aws_role_policies(iam_client, managed)

            # Join the tuples
            contents = [SEPARATOR.join(c) for c in contents]

        if args.output:
            stdout_ = sys.stdout
            sys.stdout = open(args.output, "w")
        for c in contents:
            print(c)
        if args.output:
            sys.stdout = stdout_
    else:
        raise RuntimeError(f"Error: IAM functionality not implemented for {args.cloud_provider}")
