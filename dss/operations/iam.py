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
DSS2FUS = {
    "dev": "testing",
    "integration": "integration",
    "staging": "staging",
    "prod": "prod"
}


# ---
# Fusillade utility functions/classes
# ---
class FusilladeClient(object):
    """
    Fusillade client.
    A simple wrapper around an authorization URL and a header.
    """

    AUTH_DEPLOYMENTS = {
        "dev": "https://auth.dev.data.humancellatlas.org",
        "integration": "https://auth.integration.data.humancellatlas.org",
        "staging": "https://auth.staging.data.humancellatlas.org",
        "testing": "https://auth.testing.data.humancellatlas.org",
        "production": "https://auth.data.humancellatlas.org",
    }

    def __init__(self, stage):
        auth_url, headers = self.get_auth_url_headers(stage)
        self.auth_url = auth_url
        self.headers = headers

    def get_auth_url_headers(self, stage):
        """
        Get authorization url and headers to allow Fusillade requests.
        """
        auth_url = self.AUTH_DEPLOYMENTS[stage]

        secret = "deployer_service_account.json"
        secret_id = "/".join(["dcp", "fusillade", stage, secret])
        service_account = DCPServiceAccountManager.from_secrets_manager(
            secret_id, "https://auth.data.humancellatlas.org/"
        )
        # Create the headers using the DCP service account manager
        headers = {"Content-Type": "application/json"}
        headers.update(**service_account.get_authorization_header())

        # This info will be used to create a Fusillade client (a simple wrapper around these 2 strings)
        return auth_url, headers

    @lru_cache(maxsize=128)
    def call_api(self, path, key):
        auth_url = self.auth_url
        headers = self.headers
        resp = requests.get(f"{auth_url}{path}", headers=headers)
        resp.raise_for_status()
        return resp.json()[key]

    @lru_cache(maxsize=128)
    def paginate(self, path, key=None):
        """
        Pagination utility for Fusillade
        """
        auth_url = self.auth_url
        headers = self.headers
        resp = requests.get(f"{auth_url}{path}", headers=headers)
        resp.raise_for_status()
        items = []
        while "Link" in resp.headers:
            items.extend(resp.json()[key])
            next_url = resp.headers["Link"].split(";")[0][1:-1]
            resp = requests.get(next_url, headers=headers)
            resp.raise_for_status()
        else:
            if key is None:
                items.extend(resp.json())
            else:
                items.extend(resp.json()[key])
        return items


def get_fus_role_attached_policies(fus_client, action, role):
    """
    Utility method to get policies attached to a Fusillade role using /v1/role/{role} and requesting the policies field.

    :param fus_client: Fusillade API client
    :param action: what to do with the policies (list or dump)
        (list returns a list of names only, dump returns list of JSON policy documents)
    :param role: get policies attached to this role
    :returns: list containing the information requested
    """
    # @chmreid TODO: figure out the type/structure of this api call.
    # The API call returns a dictionary with one key and one (string) value (serialized json
    # policy document). what if there are multiple policies - does the serialized json become
    # a list? is it a list of strings?
    result = []

    inline_policies_strpayload = fus_client.call_api(f"/v1/role/{role}", "policies")
    try:
        inline_policies = json.loads(inline_policies_strpayload["IAMPolicy"])
    except (KeyError, TypeError):
        pass
    else:
        if isinstance(inline_policies, dict):
            if "Id" not in inline_policies:
                inline_policies["Id"] = ANONYMOUS_POLICY_NAME
            if action == "list":
                result.append(inline_policies["Id"])
            elif action == "dump":
                result.append(inline_policies)

        elif isinstance(inline_policies, list):
            for ipolicy in inline_policies:
                if "Id" not in ipolicy:
                    ipolicy["Id"] = ANONYMOUS_POLICY_NAME
                if action == "list":
                    result.append(ipolicy["Id"])
                elif action == "dump":
                    result.append(ipolicy)

        else:
            raise RuntimeError(f"Error: could not interpret return value from API /v1/role/{role}")

    return result


# ---
# AWS utility functions/classes
# ---
def _get_aws_api_list_endpoints_dict() -> typing.Dict[str, str]:
    """Store AWS API endpoints to list each asset type"""
    return {
        "users": "list_users",
        "groups": "list_groups",
        "roles": "list_roles"
    }


def _get_aws_api_labels_dict() -> typing.Dict[str, typing.Dict[str, str]]:
    """Store the labels used to unwrap JSON results from the AWS API"""
    return {
        "users": _make_aws_api_labels_dict_entry("Users", "UserName", "UserDetailList", "UserPolicyList"),
        "groups": _make_aws_api_labels_dict_entry("Groups", "GroupName", "GroupDetailList", "GroupPolicyList"),
        "roles": _make_aws_api_labels_dict_entry("Roles", "RoleName", "RoleDetailList", "RolePolicyList"),
    }


def _make_aws_api_labels_dict_entry(*args) -> typing.Dict[str, str]:
    """
    Convenience function to unpack 4 values into a dictionary of labels, useful for processing API results.

    Example:
        >>> _make_aws_api_labels_dict_entry("Users", "UserName", "UserDetailList", "UserPolicyList")
        {
            "extracted_list_label": "Users",
            "name_label": "UserName",
            "detail_list_label": "UserDetailList",
            "policy_list_label": "UserPolicyList"
        }

    :params arg[0]: label of asset type
    :params arg[1]: label of asset name
    :params arg[2]: label of asset details
    :params arg[3]: label of asset policy details
    :returns: dictionary of organized labels
    """
    assert len(args) == 4, "Error: need 4 arguments!"
    return dict(
        extracted_list_label=args[0],
        name_label=args[1],
        detail_list_label=args[2],
        policy_list_label=args[3],
    )


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
        master_list = [json.loads(j) for j in set(master_list)]

    # also need to get all inline policies
    return master_list


def list_aws_policies(client, managed: bool):
    """Return a list of names of AWS policies"""
    return extract_aws_policies("list", client, managed)


def dump_aws_policies(client, managed: bool):
    """Return a list of dictionaries containing AWS policy documents"""
    return extract_aws_policies("dump", client, managed)


def list_aws_assets(asset_type, client):
    """Use the AWS API to compile a flat list of asset names"""
    master_list = []  # holds main results

    # Prepare labels for extracting asset info
    aws_labels = _get_aws_api_labels_dict()
    if asset_type not in aws_labels:
        raise RuntimeError(f"Error: unrecognized AWS asset type: {asset_type}")
    asset_labels = aws_labels[asset_type]

    # Map asset types to AWS API endpoints
    endpoints = _get_aws_api_list_endpoints_dict()

    paginator = client.get_paginator(endpoints[asset_type])
    for page in paginator.paginate():
        for policy in page[asset_labels['extracted_list_label']]:
            role_name = policy[asset_labels['name_label']]
            master_list.append(role_name)

    # Sort names, remove duplicates
    master_list = sorted(list(set(master_list)))
    return master_list


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

    :param action: what action to take (list, dump)
    :param fus_client: the Fusillade client to use
    :returns: a list of items whose type depends on the action param
        (policy names if action is list, json documents if action is dump)
    """
    master_list = []

    users = list(fus_client.paginate("/v1/users", "users"))

    # NOTE: This makes unnecessary duplicate API calls.
    # It would be better to get list of groups/roles then mark ones attached to users.
    for user in users:
        # Are there any user inline policies?
        # inline_policies = fus_client.call_api(f'/v1/user/{user}','policies')

        # Next get managed policies - these are policies attached via roles or groups
        membership = {
            "group": list(fus_client.paginate(f"/v1/user/{user}/groups", "groups")),
            "role": list(fus_client.paginate(f"/v1/user/{user}/roles", "roles")),
        }
        managed_policies = []
        for asset_type in ["group", "role"]:
            api_url = f"/v1/{asset_type}/"
            # Now iterate over each group or role this user is part of, and enumerate policies
            for asset in membership[asset_type]:
                # @chmreid TODO: figure out this API call. If multiple policies attached, is string payload a list?
                managed_policy = fus_client.call_api(api_url + asset, "policies")
                try:
                    iam_policy = managed_policy["IAMPolicy"]
                except (KeyError, TypeError):
                    pass
                else:
                    try:
                        d = json.loads(iam_policy)
                    except TypeError:
                        d = iam_policy
                    except json.decoder.JSONDecodeError:
                        msg = f"Warning: malformed policy document for user {user} and {asset_type} {asset}:\n"
                        msg += f"{iam_policy}"
                        logger.warning(msg)
                        d = {}  # Malformed JSON
                    if "Id" not in d:
                        d["Id"] = ANONYMOUS_POLICY_NAME
                    managed_policies.append(d)

        if action == "list":
            # Extract policy name
            for policy in managed_policies:
                master_list.append(policy["Id"])
        elif action == "dump":
            # Export policy json document
            master_list.append(policy)

    if action == "list":
        # Sort and eliminate dupes
        master_list = sorted(list(set(master_list)))
        # Headers
        if do_headers:
            master_list = ["Policies:"] + master_list
    elif action == "dump":
        # Convert to strings, remove dupes, convert to back dicts
        master_list = list(set(master_list))
        master_list = [json.loads(j) for j in master_list]

    return master_list


def list_fus_policies(fus_client, do_headers: bool) -> typing.List[str]:
    """Return a list of names of Fusillade policies"""
    return extract_fus_policies("list", fus_client, do_headers)


def dump_fus_policies(fus_client, do_headers: bool):
    """Return a list of dictionaries containing Fusillade policy documents"""
    return extract_fus_policies("dump", fus_client, do_headers)


def list_fus_assets(asset_type: str, fus_client) -> typing.List[str]:
    """Return a list of names of Fusillade assets"""
    if asset_type == "users":
        return list_fus_users(fus_client)
    elif asset_type == "groups":
        return list_fus_groups(fus_client)
    elif asset_type == "roles":
        return list_fus_roles(fus_client)


def list_fus_users(fus_client) -> typing.List[str]:
    """Return a list of names of Fusillade users"""
    users = list(fus_client.paginate("/v1/users", "users"))
    users = sorted(list(set(users)))
    return users


def list_fus_groups(fus_client) -> typing.List[str]:
    """Return a list of names of Fusillade roles"""
    groups = list(fus_client.paginate("/v1/groups", "groups"))
    groups = sorted(list(set(groups)))
    return groups


def list_fus_roles(fus_client) -> typing.List[str]:
    """Return a list of names of Fusillade roles"""
    roles = list(fus_client.paginate("/v1/roles", "roles"))
    roles = sorted(list(set(roles)))
    return roles


# ---
# List AWS policies grouped by asset type
# ---
def list_aws_policies_grouped(asset_type, client, managed: bool, do_headers: bool = True) -> typing.List[typing.Any]:
    """
    Call the AWS IAM API to retrieve policies grouped by asset and create a list of policy names.

    :param asset_type: the type of asset to group policies by ("users", "groups", or "roles")
    :param client: the boto client to use
    :param managed: (boolean) if true, include AWS-managed policies
    :returns: list of tuples of two strings in the form (asset_name, policy_name)
    """
    extracted_list = []

    # Extract labels needed
    labels = _get_aws_api_labels_dict()
    if asset_type not in labels:
        raise RuntimeError(f"Error: asset type \"{asset_type}\" is not valid, try one of: {labels.keys()}")
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


# ---
# List GCP policies grouped by asset type
# ---
def list_gcp_policies_grouped(asset_type, client, managed: bool, do_headers: bool = True):
    """
    Call the GCP IAM API to retrieve policies grouped by asset and create a list of policy names.
    """
    pass


# ---
# List Fusillade policies grouped by asset type
# ---
def list_fus_policies_grouped(group_by, fus_client, do_headers: bool = True):
    if group_by == "users":
        return list_fus_user_policies(fus_client, do_headers)
    elif group_by == "groups":
        return list_fus_group_policies(fus_client, do_headers)
    elif group_by == "roles":
        return list_fus_role_policies(fus_client, do_headers)

def list_fus_user_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by user.

    :param fus_client: the Fusillade API client
    :returns: list of tuples of two strings in the form (user_name, policy_name)
    """
    users = list(fus_client.paginate("/v1/users", "users"))

    result = []

    # NOTE: This makes unnecessary duplicate API calls.
    # It would be better to get list of groups/roles then mark ones attached to users.
    for user in users:
        # Are there any user inline policies?
        # inline_policies = fus_client.call_api(f'/v1/user/{user}','policies')

        # Next get managed policies - these are policies attached via roles or groups
        membership = {
            "group": list(fus_client.paginate(f"/v1/user/{user}/groups", "groups")),
            "role": list(fus_client.paginate(f"/v1/user/{user}/roles", "roles")),
        }
        managed_policies = []
        for asset_type in ["group", "role"]:
            api_url = f"/v1/{asset_type}/"
            for asset in membership[asset_type]:
                # @chmreid TODO: figure out this API call. If multiple policies attached, is string payload a list?
                managed_policy = fus_client.call_api(api_url + asset, "policies")
                try:
                    iam_policy = managed_policy["IAMPolicy"]
                except (KeyError, TypeError):
                    pass
                else:
                    d = json.loads(iam_policy)
                    if "Id" not in d:
                        d["Id"] = ANONYMOUS_POLICY_NAME
                    managed_policies.append(d)

        for policy in managed_policies:
            result.append((user, policy["Id"]))

    # Eliminate dupes
    result = sorted(list(set(result)))

    if do_headers:
        # Add headers
        result = [("User", "Policy")] + result

    return result


def list_fus_group_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by group.

    :param fus_client: the Fusillade API client
    :param do_headers: include column headers in the output list
    :returns: list of tuples of two strings in the form (group_name, policy_name)
    """
    groups = list(fus_client.paginate("/v1/groups", "groups"))

    result = []

    for group in groups:
        # First get inline policies directly attached
        # @chmreid TODO: figure out the type/structure of this api call
        # inline_policies = fus_client.call_api(f'/v1/group/{group}','policies')

        # Next get managed policies (attached via roles)
        roles_membership = list(fus_client.paginate(f"/v1/group/{group}/roles", "roles"))
        for role in roles_membership:
            attached_names = get_fus_role_attached_policies(fus_client, "list", role)
            for attached_name in attached_names:
                result.append((group, attached_name))

    # Eliminate dupes
    result = sorted(list(set(result)))

    if do_headers:
        # Add headers
        result = [("Group", "Policy")] + result

    return result


def list_fus_role_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by role.

    :param fus_client: the Fusillade API client
    :param do_headers: include column headers in the output list
    :returns: list of tuples of two strings in the form (role_name, policy_name)
    """
    roles = list(fus_client.paginate("/v1/roles", "roles"))

    result = []

    for role in roles:
        attached_names = get_fus_role_attached_policies(fus_client, "list", role)
        for attached_name in attached_names:
            result.append((role, attached_name))

    # Eliminate dupes
    result = sorted(list(set(result)))

    if do_headers:
        # Add headers
        result = [("Role", "Policy")] + result

    return result


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
        ),
        "--quiet": dict(action="store_true", help="Suppress warning messages")
    },
)
def list_policies(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM policy names, optionally grouped by asset"""
    if args.output:
        if os.path.exists(args.output) and not args.force:
            raise RuntimeError(f"Error: cannot overwrite {args.output} without --force flag")

    managed = args.include_managed  # noqa
    do_headers = not args.exclude_headers

    if args.cloud_provider == "aws":
        if args.group_by is None:
            contents = list_aws_policies(iam_client, managed)
        elif args.group_by in ['users', 'groups', 'roles']:
            contents = list_aws_policies_grouped(args.group_by, iam_client, managed, do_headers)
        else:
            raise RuntimeError(f"Invalid --group-by argument passed: {args.group_by}")

        # Join the tuples
        if args.group_by is not None:
            contents = [IAMSEPARATOR.join(c) for c in contents]

    elif args.cloud_provider == "gcp":
        raise NotImplementedError("Error: IAM functionality for GCP not implemented")

    elif args.cloud_provider == "fusillade":
        fus_stage = DSS2FUS[os.environ["DSS_DEPLOYMENT_STAGE"]]
        client = FusilladeClient(fus_stage)

        if args.group_by is None:
            # list policies
            contents = list_fus_policies(client, do_headers)
        elif args.group_by in ['users', 'groups', 'roles']:
            contents = list_fus_policies_grouped(args.grouped_by, client, do_headers)
        else:
            RuntimeError(f"Invalid --group-by argument passed: {args.group_by}")

        # Join the tuples
        if args.group_by is not None:
            contents = [IAMSEPARATOR.join(c) for c in contents]

    else:
        raise NotImplementedError(f"Error: IAM functionality not implemented for {args.cloud_provider}")

    # Print list to output
    if args.output:
        stdout_ = sys.stdout
        sys.stdout = open(args.output, "w")
    for c in contents:
        print(c)
    if args.output:
        sys.stdout = stdout_


list_asset_args = {
    "cloud_provider": dict(
        choices=["aws", "gcp", "fusillade"], help="The cloud provider whose policies are being listed"
    ),
    "--output": dict(
        type=str, required=False, help="Specify an output file name (output sent to stdout by default)"
    ),
    "--force": dict(
        action="store_true",
        help="If output file already exists, overwrite it (default is not to overwrite)",
    ),
    "--exclude-headers": dict(
        action="store_true", help="Exclude headers on the list being output"
    )
}


def list_asset_action(asset_type, argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM assets available"""
    if args.output:
        if os.path.exists(args.output) and not args.force:
            raise RuntimeError(f"Error: cannot overwrite {args.output} without --force flag")

    if args.cloud_provider == "aws":
        contents = list_aws_assets(asset_type, iam_client)

    elif args.cloud_provider == "gcp":
        raise NotImplementedError("Error: IAM functionality for GCP not implemented")

    elif args.cloud_provider == "fusillade":
        dss_stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        fus_stage = DSS2FUS[dss_stage]
        fus_client = FusilladeClient(fus_stage)
        contents = list_fus_assets(asset_type, fus_client)

    else:
        raise NotImplementedError(f"Error: IAM functionality not implemented for {args.cloud_provider}")

    # Print list to output
    if args.output:
        stdout_ = sys.stdout
        sys.stdout = open(args.output, "w")
    for c in contents:
        print(c)
    if args.output:
        sys.stdout = stdout_


@iam.action("list-users", arguments=list_asset_args)
def list_users(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM users available"""
    list_asset_action("users", argv=argv, args=args)

@iam.action("list-groups", arguments=list_asset_args)
def list_groups(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM groups available"""
    list_asset_action("groups", argv=argv, args=args)

@iam.action("list-roles", arguments=list_asset_args)
def list_roles(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM roles available"""
    list_asset_action("roles", argv=argv, args=args)
