#!/usr/bin/env python
"""
This script ensures Fusillade roles (defined in this repo at roles.json)
are correctly applied to the right Fusillade stage, and that the current
state of the Fusillade cloud directory contains the roles defined in roles.json.
Operator should provide the Fusillade stage as the first arg or using the
FUS_DEPLOYMENT_STAGE environment var.
"""
import subprocess
import os
import sys
import json
import copy


FUS2DSS = {
    "testing": "dev",
    "dev": None,
    "integration": "integration",
    "staging": "staging",
    "prod": "prod"
}
DSS_ENV_FILES = {
    "dev": "environment",
    "integration": "environment.integration",
    "staging": "environment.staging",
    "prod": "environment.prod"
}
IAMSEPARATOR = " : "


class FusilladeChecker(object):
    """
    This class defines functionality to check that the data store roles
    specified in roles.json have been deployed correctly to Fusillade.

    Steps:
    - collect info from roles.json in repo and extract list of roles
    - check that each role from roles.json is present in roles listed in fusillade
    """
    STAGES = {
        'dev': 'environment',
        'testing': 'environment',
        'integration': 'environment.integration',
        'staging': 'environment.staging',
        'prod': 'environment.prod'
    }
    def __init__(self, fus_stage):
        """Set up stage info and check values"""
        if fus_stage not in DSS2FUS:
            print(f'Custom stage "{fus_stage}" provided. Skipping Fusillade check.')
            return
        else:
            self.fus_stage = fus_stage
            self.dss_stage = FUS2DSS[fus_stage]

        this_dir = os.path.abspath(os.path.dirname(__file__))
        self.scripts_dir = this_dir
        self.dss_dir = os.path.join(this_dir, '..')

        # Bootstrap: stage_env must be set before we can run get_stage_env
        self.stage_env = copy.deepcopy(os.environ)

        # Set up the environment vars for this stage
        env_file = DSS_ENV_FILES[self.dss_stage]
        self.stage_env = self.get_stage_env(os.path.join(self.dss_dir, env_file))

    def get_stage_env(self, env_file):
        """Return a serialized JSON of the environment variables for this stage"""
        dump = 'python -c "import os, json; print(json.dumps(dict(os.environ)))"'
        cmd = ['bash', '-c', f'source {env_file} && {dump}']
        return json.loads(self.run_cmd(cmd, shell=False))

    def run(self):
        """Run the Fusillade check, raise an exception if the check fails"""
        roles_json = self.get_roles_from_json()
        roles_fus = self.get_roles_from_fus()
        # Assert each role in json is in list roles output
        errors = []
        for r in roles_json:
            if r not in roles_fus:
                errors.append(f"Role {r} is in roles.json but is not in Fusillade cloud directory")
        if len(errors) > 0:
            err_msg = "Encountered errors:\n"
            err_msg += "\n".join(errors)
            raise RuntimeError(err_msg)

    def get_roles_from_json(self) -> list:
        """Extract a list of roles from roles.json in this repo"""
        roles_dir = os.path.join(os.path.dirname(__file__), '..')
        roles_json = os.path.join(roles_dir, 'roles.json')
        with open(roles_json, 'r') as f:
            roles_dict = json.load(f)
        roles_list = [k for k in roles_dict['roles']]
        return roles_list

    def get_roles_from_fus(self) -> list:
        """List all Fusillade roles using dss-ops and return the list of roles"""
        # Call dss-ops script, iam list action
        ops_script = os.path.join(self.scripts_dir, 'dss-ops.py')
        ops_action = "iam list fusillade"
        ops_args = f"--group-by roles --exclude-headers --quiet"
        cmd = f"{ops_script} {ops_action} {ops_args}"
        raw_resp = self.run_cmd(cmd)
        resp_lines = [j for j in raw_resp.split("\n") if len(j)>0]
        roles = [k.split(IAMSEPARATOR)[0] for k in resp_lines]
        return roles

    def run_cmd(self, cmd, cwd=os.getcwd(), shell=True):
        """Wrapper to run a command and return stdout"""
        p = subprocess.Popen(cmd,
                             shell=shell,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             cwd=cwd,
                             env=self.stage_env)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise RuntimeError(f'While checking Fusillade, an error occured:\n'
                               f'stdout: {stdout}\n\n'
                               f'stderr: {stderr}\n')
        return stdout.decode('utf-8')


def main(stage = None):
    if len(sys.argv) > 1:
        stage = sys.argv[1]
    elif 'FUS_DEPLOYMENT_STAGE' in os.environ:
        stage = os.environ['FUS_DEPLOYMENT_STAGE']
    else:
        msg = "Error: FUS_DEPLOYMENT_STAGE environment variable not defined. Please define it "
        msg += "or provide 'stage' as the first environment."
        raise RuntimeError(msg)
    s = FusilladeChecker(stage)
    s.run()


if __name__ == '__main__':
    main()
