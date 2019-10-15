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

    AUTH_DEPLOYMENTS = {
        "dev": "https://auth.dev.data.humancellatlas.org",
        "integration": "https://auth.integration.data.humancellatlas.org",
        "staging": "https://auth.staging.data.humancellatlas.org",
        "testing": "https://auth.testing.data.humancellatlas.org",
        "production": "https://auth.data.humancellatlas.org",
    }

    def __init__(self, stage=None):
        if stage is None:
            RuntimeError("You must provide a stage argument to FusilladeClient(stage)")
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
    Get policies attached to a Fusillade role using /v1/role/{role} and requesting the policies field.

    :param fus_client: Fusillade API client
    :param action: what to do with the policies (list or dump)
        (list returns a list of names only, dump returns list of JSON policy documents)
    :param role: get policies attached to this role
    :returns: list containing the information requested
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
                    d = json.loads(iam_policy)
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

    :param fus_client: the Fusillade API client
    :returns: list of tuples of two strings in the form (user_name, policy_name)
    """
    pass

def list_fus_group_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by group.

    :param fus_client: the Fusillade API client
    :param do_headers: include column headers in the output list
    :returns: list of tuples of two strings in the form (group_name, policy_name)
    """
    pass

def list_fus_role_policies(fus_client, do_headers: bool = True):
    """
    Call the Fusillade API to retrieve policies grouped by role.

    :param fus_client: the Fusillade API client
    :param do_headers: include column headers in the output list
    :returns: list of tuples of two strings in the form (role_name, policy_name)
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

    managed = args.include_managed  # noqa
    do_headers = not args.exclude_headers

    if args.cloud_provider == "aws":
        pass
    elif args.cloud_provider == "gcp":
        pass
    elif args.cloud_provider == "fusillade":

        stage = os.environ["DSS_DEPLOYMENT_STAGE"]
        client = FusilladeClient(stage=stage)

        if args.group_by is None:
            # list policies
            contents = list_fus_policies(client, do_headers)
        else:
            # list policies grouped by asset
            if args.group_by == "users":
                contents = list_fus_user_policies(client, do_headers)
            elif args.group_by == "groups":
                contents = list_fus_group_policies(client, do_headers)
            elif args.group_by == "roles":
                contents = list_fus_role_policies(client, do_headers)

            # Join the tuples
            contents = [IAMSEPARATOR.join(c) for c in contents]

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
