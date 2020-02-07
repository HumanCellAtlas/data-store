"""
Get/set secret variable values from the AWS Secrets Manager
"""
import os
import sys
import select
import typing
import argparse
import json
import logging
import copy
import subprocess

from botocore.exceptions import ClientError

from dss.operations import dispatch
from dss.operations.util import polite_print
from dss.util.aws.clients import secretsmanager as sm_client  # type: ignore

logger = logging.getLogger(__name__)


def get_secret_store_prefix() -> str:
    """
    Use information from the environment to assemble the necessary prefix for accessing variables in the
    SecretsManager.
    """
    store_name = os.environ["DSS_SECRETS_STORE"]
    stage_name = os.environ["DSS_DEPLOYMENT_STAGE"]
    store_prefix = f"{store_name}/{stage_name}"
    return store_prefix


def fix_secret_variable_prefix(secret_name: str) -> str:
    """
    Given a secret name, check if it already has the secrets store prefix.
    """
    prefix = get_secret_store_prefix()
    if not (secret_name.startswith(prefix) or secret_name.startswith("/" + prefix)):
        secret_name = f"{prefix}/{secret_name}"
    return secret_name


def fetch_secret_safely(secret_name: str) -> dict:
    """
    Fetch a secret from the store safely, raising errors if the secret is not found or is marked for deletion.
    """
    try:
        response = sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if "Error" in e.response:
            errtype = e.response["Error"]["Code"]
            if errtype == "ResourceNotFoundException":
                raise RuntimeError(f"Error: secret {secret_name} was not found!")
        raise RuntimeError(f"Error: could not fetch secret {secret_name} from secrets manager")
    else:
        return response


secrets = dispatch.target("secrets", arguments={}, help=__doc__)


json_flag_options = dict(
    default=False, action="store_true", help="format the output as JSON if this flag is present"
)
dryrun_flag_options = dict(
    default=False, action="store_true", help="do a dry run of the actual operation"
)
quiet_flag_options = dict(
    default=False, action="store_true", help="suppress output"
)


@secrets.action(
    "list",
    arguments={
        "--json": json_flag_options,
    },
)
def list_secrets(argv: typing.List[str], args: argparse.Namespace):
    """
    Print a list of names of every secret variable in the secrets manager for the DSS secrets manager
    for $DSS_DEPLOYMENT_STAGE.
    """
    store_prefix = get_secret_store_prefix()

    paginator = sm_client.get_paginator("list_secrets")

    secret_names = []
    for response in paginator.paginate():
        for secret in response["SecretList"]:

            # Get resource IDs
            secret_name = secret["Name"]

            # Only save secrets for this store and stage
            secret_names.append(fix_secret_variable_prefix(secret_name))

            if secret_name.startswith(store_prefix):
                secret_names.append(secret_name)

    secret_names.sort()

    if args.json is True:
        print(json.dumps(secret_names, indent=4))
    else:
        for secret_name in secret_names:
            print(secret_name)


@secrets.action(
    "get",
    arguments={
        "secret_name": dict(help="the name of the secret to retrieve"),
        "--outfile": dict(required=False, type=str, help="specify an output file where the secret will be saved"),
        "--force": dict(
            default=False,
            action="store_true",
            help="overwrite the output file, if it already exists; must be used with --output flag.",
        ),
    },
)
def get_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Get the value of the secret variable specified by secret_name.
    """
    # Note: this function should not print anything except the final JSON, in case the user pipes the JSON
    # output of this script to something else

    if args.outfile:
        if os.path.exists(args.outfile) and not args.force:
            raise RuntimeError(
                f"Error: file {args.outfile} already exists, use the --force flag to overwrite it"
            )

    secret_name = fix_secret_variable_prefix(args.secret_name)

    # Attempt to obtain secret
    try:
        response = sm_client.get_secret_value(SecretId=secret_name)
        secret_val = response["SecretString"]
    except ClientError:
        # A secret variable with that name does not exist
        raise RuntimeError(f"Error: Resource not found: {secret_name}")
    else:
        # Get operation was successful, secret variable exists
        if args.outfile:
            sys.stdout = open(args.outfile, "w")
        print(secret_val)
        if args.outfile:
            sys.stdout = sys.__stdout__


@secrets.action(
    "set",
    arguments={
        "secret_name": dict(help="name of secret to set (limit 1 at a time)"),
        "--infile": dict(help="specify an input file whose contents is the secret value"),
        "--force": dict(
            default=False, action="store_true", help="force the action to happen (no interactive prompt)"
        ),
        "--dry-run": dryrun_flag_options,
        "--quiet": quiet_flag_options
    },
)
def set_secret(argv: typing.List[str], args: argparse.Namespace):
    """Set the value of the secret variable."""
    secret_name = fix_secret_variable_prefix(args.secret_name)

    # Decide what to use for infile
    secret_val = None
    if args.infile is not None:
        if os.path.isfile(args.infile):
            with open(args.infile, 'r') as f:
                secret_val = f.read()
        else:
            raise RuntimeError(f"Error: specified input file {args.infile} does not exist!")
    else:
        # Use stdin (input piped to this script) as secret value.
        # stdin provides secret value, flag --secret-name provides secret name.
        if not select.select([sys.stdin], [], [], 0.0)[0]:
            raise RuntimeError("Error: stdin was empty! A secret value must be provided via stdin")
        secret_val = sys.stdin.read()

    # Create or update
    try:
        # Start by trying to get the secret variable
        sm_client.get_secret_value(SecretId=secret_name)

    except ClientError:
        # A secret variable with that name does not exist, so create it
        if args.dry_run:
            polite_print(
                args.quiet,
                f"Secret variable {secret_name} not found in secrets manager, dry-run creating it"
            )
        else:
            if args.infile:
                polite_print(
                    args.quiet,
                    f"Secret variable {secret_name} not found in secrets manager, creating from input file"
                )
            else:
                polite_print(
                    args.quiet,
                    f"Secret variable {secret_name} not found in secrets manager, creating from stdin"
                )
            sm_client.create_secret(Name=secret_name, SecretString=secret_val)

    else:
        # Get operation was successful, secret variable exists
        # Prompt the user before overwriting, unless --force flag present
        if not args.force and not args.dry_run:
            # Prompt the user to make sure they really want to do this
            confirm = f"""
            *** WARNING!!! ***

            The secret you are setting currently has a value. Calling the
            set secret function will overwrite the current value of the
            secret!

            Note:
            - To do a dry run of this operation first, use the --dry-run flag.
            - To ignore this warning, use the --force flag.

            Are you really sure you want to update the secret "{secret_name}"
            in secrets store "{get_secret_store_prefix()}"?
            (Type 'y' or 'yes' to confirm):
            """
            response = input(confirm)
            if response.lower() not in ["y", "yes"]:
                print("You safely aborted the set secret operation!")
                sys.exit(0)

        if args.dry_run:
            polite_print(
                args.quiet,
                f"Secret variable {secret_name} found in secrets manager, dry-run updating it"
            )
        else:
            if args.infile:
                polite_print(
                    args.quiet,
                    f"Secret variable {secret_name} found in secrets manager, updating from input file"
                )
            else:
                polite_print(
                    args.quiet,
                    f"Secret variable {secret_name} found in secrets manager, updating from stdin"
                )
            sm_client.update_secret(SecretId=secret_name, SecretString=secret_val)


@secrets.action(
    "delete",
    arguments={
        "secret_name": dict(help="name of secret to delete (limit 1 at a time)"),
        "--force": dict(
            default=False,
            action="store_true",
            help="force the delete operation to happen non-interactively (no user prompt)",
        ),
        "--dry-run": dict(default=False, action="store_true", help="do a dry run of the actual operation"),
        "--quiet": quiet_flag_options
    },
)
def del_secret(argv: typing.List[str], args: argparse.Namespace):
    """
    Delete the value of the secret variable specified by the
    --secret-name flag from the secrets manager
    """
    secret_name = fix_secret_variable_prefix(args.secret_name)

    try:
        # Start by trying to get the secret variable
        sm_client.get_secret_value(SecretId=secret_name)

    except ClientError:
        # No secret var found
        polite_print(
            args.quiet,
            f"Secret variable {secret_name} not found in secrets manager!"
        )

    except sm_client.exceptions.InvalidRequestException:
        # Already deleted secret var
        polite_print(
            args.quiet,
            f"Secret variable {secret_name} already marked for deletion in secrets manager!"
        )

    else:
        # Get operation was successful, secret variable exists
        if not args.force and not args.dry_run:
            # Make sure the user really wants to do this
            confirm = f"""
            *** WARNING!!! ****

            You are about to delete secret {secret_name} from the secrets
            manager. Are you sure you want to delete the secret?
            (Type 'y' or 'yes' to confirm):
            """
            response = input(confirm)
            if response.lower() not in ["y", "yes"]:
                raise RuntimeError("You safely aborted the delete secret operation!")

        if args.dry_run:
            # Delete it for fakes
            polite_print(
                args.quiet,
                f"Secret variable {secret_name} found in secrets manager, dry-run deleting it"
            )
        else:
            # Delete it for real
            polite_print(
                args.quiet,
                f"Secret variable {secret_name} found in secrets manager, deleting it"
            )
            sm_client.delete_secret(SecretId=secret_name)


class SecretsChecker(object):
    """
    A class to aid in checking deployed secrets in the secrets manager.
    Will only check canonical HCA DSS stages ('dev', 'integration', 'staging').
    Checking occurs as follows:

    #1
    For the json returned from the secret in GOOGLE_APPLICATION_SECRETS_SECRETS_NAME:
        `auth_uri` should be in ['https://auth.data.humancellatlas.org/authorize',
                                 'https://auth.dev.data.humancellatlas.org/authorize']
        `token_uri` should be in ['https://auth.data.humancellatlas.org/oauth/token',
                                  'https://auth.dev.data.humancellatlas.org/oauth/token']

    #2
    For the json returned from the secret in GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME:
        `project_id` should be `human-cell-atlas-travis-test`
        `type` should be `service_account`
        `client_email` should be the user account returned from the terraform output "service_account".
                       For example: dev should be `travis-test@human-cell-atlas-travis-test.iam.gserviceaccount.com`.
    """
    def __init__(self, stage):
        """Initialize variables useful to the secrets checker"""
        self.stage = stage
        self.stages = {'dev': 'environment',
                       'integration': 'environment.integration',
                       'staging': 'environment.staging'}

        self.missing_secrets = []
        self.malformed_secrets = []
        self.incomplete_secrets = []
        self.error_message = f'\n\n' \
                             f'Deploying to {self.stage.upper()} could not be completed.\n' \
                             f'It looks like an AWS secret has an unexpected value.\n' \
                             f'Please do not change AWS secrets for releases.\n'

        if self.stage not in self.stages:
            print(f'Custom stage "{self.stage}" provided. Secret checking skipped.')
            return

        self.stage_env = copy.deepcopy(os.environ)
        self.stage_env = self.get_stage_env(self.stages[self.stage])
        self.service_account = self.fetch_terraform_output("service_account", "gcp_service_account").strip()

        self.project = os.environ['GCP_PROJECT_ID']
        self.email = [f'{self.service_account}@{self.project}.iam.gserviceaccount.com']

        self.type = ['service_account']
        self.auth_url = os.getenv('AUTH_URL')
        self.auth_uri = [f'{self.auth_url}/oauth/authorize']
        self.token_uri = [f'{self.auth_url}/oauth/token']

        self.app_secret_name = os.environ['GOOGLE_APPLICATION_SECRETS_SECRETS_NAME']
        self.gcp_cred_secret_name = os.environ['GOOGLE_APPLICATION_CREDENTIALS_SECRETS_NAME']
        self.app_secret = self.fetch_secret(self.app_secret_name)
        self.gcp_cred_secret = self.fetch_secret(self.gcp_cred_secret_name)

    def run_cmd(self, cmd, cwd=os.getcwd(), shell=True):
        """Run a command and return stdout"""
        p = subprocess.Popen(cmd,
                             shell=shell,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=cwd,
                             env=self.stage_env)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(f'While checking secrets, an error occured:\n'
                               f'stdout: {stdout}\n\n'
                               f'stderr: {stderr}\n')
        return stdout.decode('utf-8')

    def get_stage_env(self, env_file):
        """Source the environment file for this stage, then export the current environment as a dict"""
        dump = 'python -c "import os, json; print(json.dumps(dict(os.environ)))"'
        cmd = ['bash', '-c', f'source environment && source {env_file} && {dump}']
        return json.loads(self.run_cmd(cmd, shell=False))

    def fetch_secret(self, secret_name):
        """Fetch the value of a secret from the AWS secrets manager"""
        # Add /dcp/dss/$STAGE store prefix to secret name
        secret_name = fix_secret_variable_prefix(secret_name)
        try:
            secret = fetch_secret_safely(secret_name)
            secret = json.loads(secret['SecretString'])
        except (RuntimeError, KeyError, json.decoder.JSONDecodeError):
            self.missing_secrets.append(secret_name)
            return
        if not (('installed' not in secret) or ('client_email' not in secret)) and (self.stage in self.stages):
            self.malformed_secrets.append(secret_name)
            return
        return secret

    def fetch_terraform_output(self, output_name, output_infra_dir):
        """
        Use the makefiles in the data-store repository to create terraform resource files and use these to
        check environment variables. See: https://www.terraform.io/docs/commands/output.html
        """
        dss_root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        output_infra_dir = os.path.abspath(os.path.join(dss_root_dir, 'infra', output_infra_dir))

        # populate infra vars for the current stage using terraform
        self.run_cmd(cmd=f'make -C infra', cwd=dss_root_dir)
        self.run_cmd(cmd=f'terraform refresh', cwd=output_infra_dir)

        # query terraform as to what the needed var is and return it
        terraform_output = self.run_cmd(cmd=f'terraform output {output_name}', cwd=output_infra_dir)
        if not terraform_output:
            print(f'Terraform output returned nothing.\n'
                  f'Check your terraform setup by running "terraform output {output_name}" in dir:\n'
                  f'data-store/infra/gcp_service_account\n\n')
        return terraform_output.strip()

    def check(self, current, expected, secret):
        """Check that the secret value is what is expected - if not, add to the list of incomplete secrets"""
        if current not in expected:
            self.incomplete_secrets.append({'secret': secret,
                                            'current': current,
                                            'expected': expected})

    def run(self):
        """Run the secrets check"""
        # do not check user-custom deploys
        if self.stage in self.stages:
            print(f'Now checking the secrets for {self.stage}...')
            self.check(self.app_secret['installed']['auth_uri'], self.auth_uri, secret=self.app_secret_name)
            self.check(self.app_secret['installed']['token_uri'], self.token_uri, secret=self.app_secret_name)
            self.check(self.gcp_cred_secret['type'], self.type, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['project_id'], self.project, secret=self.gcp_cred_secret_name)
            self.check(self.gcp_cred_secret['client_email'], self.email, secret=self.gcp_cred_secret_name)
            print(f'Secret check complete for {self.stage}.')

        if self.missing_secrets or self.incomplete_secrets or self.malformed_secrets:
            for s in self.incomplete_secrets:
                self.error_message += f'\n' \
                                      f'The following secret                : {s["secret"]}\n' \
                                      f'Had the unexpected setting          : {s["current"]}\n' \
                                      f'When one of these items was expected: {s["expected"]}\n'
            self.error_message += '\n'
            for s in self.missing_secrets:
                self.error_message += f'The following secret was missing    : {s}\n'
            self.error_message += '\n'
            for s in self.malformed_secrets:
                self.error_message += f'The following secret seems malformed: {s}\n'
            raise ValueError(self.error_message)


@secrets.action(
    "check",
    arguments={}
)
def check_secrets(argv: typing.List[str], args: argparse.Namespace):
    """
    Ensure that the secrets in an HCA stage deployment are not accidentally
    changed to personal user credentials (or otherwise).  Requires Terraform.
    """
    stage = os.environ['DSS_DEPLOYMENT_STAGE']
    s = SecretsChecker(stage)
    s.run()
