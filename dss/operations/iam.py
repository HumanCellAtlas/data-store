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
from functools import lru_cache

from botocore.exceptions import ClientError

from dss.operations import dispatch
from dss.util.aws.clients import iam as iam_client  # type: ignore


logger = logging.getLogger(__name__)


SEPARATOR = " : "


# ---
# Fusillade utility functions/classes
# ---
class FusilladeClient(object):
    """
    Fusillade client.
    A simple wrapper around an authorization URL and a header.
    """
    AUTH_DEPLOYMENTS = {
        'dev': "https://auth.dev.data.humancellatlas.org",
        'integration': "https://auth.integration.data.humancellatlas.org",
        'staging': "https://auth.staging.data.humancellatlas.org",
        "testing": "https://auth.testing.data.humancellatlas.org",
        "production": "https://auth.data.humancellatlas.org"
    }

    def __init__(self, stage=None):
        if stage==None:
            RuntimeError("You must provide a stage argument to FusilladeClient(stage)")
        auth_url, headers = self.get_auth_url_headers(stage)
        self.auth_url = auth_url
        self.headers = headers

    def get_auth_url_headers(self,stage):
        """
        Get authorization url and headers to allow Fusillade requests.
        """
        auth_url = self.AUTH_DEPLOYMENTS[stage]

        # @chmreid TODO: what permissions does this require?
        secret="deployer_service_account.json"
        secret_id = '/'.join(['dcp', 'fusillade', stage, secret])
        service_account = DCPServiceAccountManager.from_secrets_manager(
            secret_id,
            "https://auth.data.humancellatlas.org/"
        )
        # Create the headers using the DCP service account manager
        headers = {'Content-Type': "application/json"}
        headers.update(**service_account.get_authorization_header())

        # This info will be used to create a
        # Fusillade client (a simple wrapper
        # around these two strings...)
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
            next_url = resp.headers['Link'].split(';')[0][1:-1]
            resp = requests.get(next_url, headers=headers)
            resp.raise_for_status()
        else:
            if key==None:
                items.extend(resp.json())
            else:
                items.extend(resp.json()[key])
        return items


# ---
# AWS utility functions and variables
# ---
def _make_aws_api_labels_dict_entry(*args) -> typing.Dict[str,str]:
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


def _get_aws_api_labels_dict() -> typing.Dict[str,str]:
    """Store the labels used to unwrap JSON results from the AWS API"""
    labels = {
        "user": _make_aws_api_labels_dict_entry("User", "UserName", "UserDetailList", "UserPolicyList"),
        "group": _make_aws_api_labels_dict_entry("Group", "GroupName", "GroupDetailList", "GroupPolicyList"),
        "role": _make_aws_api_labels_dict_entry("Role", "RoleName", "RoleDetailList", "RolePolicyList"),
    }
    return labels


# ---
# Dump/list all policies
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


def extract_fus_policies(action: str, client):
    """
    Call the Fusillade API to retrieve policies and perform an action with them.

    :param action: what action to take (list, dump)
    :param client: the Fusillade client to use
    :returns: a list of items whose type depends on the action param
        (policy names if action is list, json documents if action is dump)
    """
    master_list = []

    users = list(fus_client.paginate('/v1/users', 'users'))
    groups = list(fus_client.paginate('/v1/groups', 'groups'))
    roles = list(fus_client.paginate('/v1/roles', 'roles'))
    for user in users:
        membership = {
            'group': fus_client.call_api(f'/v1/user/{user}/groups', 'groups'),
            'role': fus_client.call_api(f'/v1/user/{user}/roles', 'roles')
        }
        managed_policies = []
        for asset_type in ['group','role']:
            api_url = f'/v1/{asset_type}/'
            for asset in membership[asset_type]:
                managed_policy = fus_client.call_api(api_url+asset, 'policies')
                try:
                    iam_policy = managed_policy["IAMPolicy"]
                except (KeyError, TypeError):
                    pass
                else:
                    d = json.loads(iam_policy)
                    managed_policies.append(d)

        if action=='list':
            # Extract policy name
            for policy in managed_policies:
                if 'Id' not in policy:
                    d['Id'] = 'UNNAMED_POLICY'
                master_list.append((user,policy['Id']))
        elif action=='dump':
            # Export policy json document
            master_list.append(policy)

    if action == "list":
        # Sort names, remove duplicates
        master_list = sorted(list(set(master_list)))
    elif action == "dump":
        # Convert to strings, remove dupes, convert to back dicts
        master_list = list(set(master_list))
        master_list = [json.loads(j) for j in master_list]

    return master_list


def list_fus_policies(fus_client) -> typing.List[str]:
    """Return a list of names of Fusillade policies"""
    return extract_fus_policies("list", fus_client)


def dump_fus_policies(fus_client):
    """Return a list of dictionaries containing Fusillade policy documents"""
    return extract_fus_policies("dump", fus_client)


# ---
# List policies grouped by asset type
# ---
def list_aws_policies_grouped(asset_type: str, client, managed: bool):
    """
    Call the AWS IAM API to retrieve policies grouped by asset and create a list of policy names.

    :param asset_type: the type of asset to group policies by
    :param client: the boto client to use
    :param managed: (boolean) if true, include AWS-managed policies
    :returns: list of tuples of two strings in the form (asset_name, policy_name)
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


def list_fus_user_policies(fus_client):
    """
    Call the Fusillade API to retrieve policies grouped by user.

    :param fus_client: the Fusillade API client
    :returns: list of tuples of two strings in the form (user_name, policy_name)
    """
    users = list(fus_client.paginate('/v1/users', 'users'))
    groups = list(fus_client.paginate('/v1/groups', 'groups'))
    roles = list(fus_client.paginate('/v1/roles', 'roles'))

    result = []

    for user in users:
        # First get inline policies - these are directly attached to the user
        # @chmreid TODO: figure out the type/structure of this api call
        #inline_policies = fus_client.call_api(f'/v1/user/{user}','policies')

        # Next get managed policies - these are policies attached via roles or groups
        membership = {
            'group': fus_client.call_api(f'/v1/user/{user}/groups', 'groups'),
            'role': fus_client.call_api(f'/v1/user/{user}/roles', 'roles')
        }
        managed_policies = []
        for asset_type in ['group','role']:
            api_url = f'/v1/{asset_type}/'
            for asset in membership[asset_type]:
                managed_policy = fus_client.call_api(api_url+asset, 'policies')
                try:
                    iam_policy = managed_policy["IAMPolicy"]
                except (KeyError, TypeError):
                    pass
                else:
                    d = json.loads(iam_policy)
                    if 'Id' not in iam_policy:
                        d['Id'] = 'UNNAMED_POLICY'
                    managed_policies.append(d)

        for policy in managed_policies:
            result.append((user,policy['Id']))

    return result


def list_fus_group_policies(fus_client):
    """
    Call the Fusillade API to retrieve policies grouped by group.

    :param fus_client: the Fusillade API client
    :returns: list of tuples of two strings in the form (group_name, policy_name)
    """
    groups = [j for j in fus_client.paginate('/v1/group', 'groups')]
    roles = [j for j in fus_client.paginate('/v1/role', 'roles')]

    result = []

    for group in groups:
        # First get inline policies directly attached
        # @chmreid TODO: figure out the type/structure of this api call
        #inline_policies = fus_client.call_api(f'/v1/group/{group}','policies')

        # Next get managed policies (attached via roles)
        managed_policies = []
        roles_membership = fus_client.call_api(f'/v1/group/{group}/roles', 'roles')
        for role in roles_membership:
            managed_policy = fus_client.call_api(f'/v1/role/{role}', 'policies')
            try:
                iam_policy = managed_policy["IAMPolicy"]
            except (KeyError, TypeError):
                pass
            else:
                d = json.loads(iam_policy)
                if 'Id' not in iam_policy:
                    d['Id'] = 'UNNAMED_POLICY'
                managed_policies.append(d)

        for policy in managed_policies:
            result.append((group,policy['Id']))

    return result


def list_fus_role_policies(fus_client):
    """
    Call the Fusillade API to retrieve policies grouped by role.

    :param fus_client: the Fusillade API client
    :returns: list of tuples of two strings in the form (role_name, policy_name)
    """
    groups = [j for j in fus_client.paginate('/v1/group', 'groups')]
    roles = [j for j in fus_client.paginate('/v1/role', 'roles')]

    result = []

    for role in roles:
        # Get inline policies directly attached
        # @chmreid TODO: figure out the type/structure of this api call
        inline_policies = fus_client.call_api(f'/v1/role/{role}','policies')

        for policy in inline_policies:
            result.append((role,policy['Id']))

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
            action="store_true",
            help="Include policies provided and managed by the cloud provider"
        ),
    },
)
def list_policies(argv: typing.List[str], args: argparse.Namespace):
    """Print a simple, flat list of IAM policy names, optionally grouped by asset"""
    if args.output:
        if os.path.exists(args.output) and not args.force:
            raise RuntimeError(f"Error: cannot overwrite {args.output} without --force flag")

    managed = args.include_managed

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

    elif args.cloud_provider == "fusillade":

        stage = os.environ['DSS_DEPLOYMENT_STAGE']
        client = FusilladeClient(stage=stage)

        if args.group_by is None:
            # list policies
            list_fus_policies(client)
        else:
            # list policies grouped by asset
            if args.group_by == "users":
                list_fus_user_policies(client)
            elif args.group_by == "groups":
                list_fus_group_policies(client)
            elif args.group_by == "roles":
                list_fus_role_policies(client)

    else:
        raise RuntimeError(f"Error: IAM functionality not implemented for {args.cloud_provider}")
