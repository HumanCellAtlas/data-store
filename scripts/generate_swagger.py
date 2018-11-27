from collections import defaultdict
import sys
import os

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


class SecureSwagger(object):
    def __init__(self, config=None):
        self.path_section = False
        self.call_section = None
        self.infile = os.path.join(pkg_root, 'swagger_template')
        self.outfile = os.path.join(pkg_root, 'dss-api.yml')
        self.config = config if config else os.path.join(pkg_root, 'swagger_template')
        self.security_endpoints = self.security_from_config()

    def security_from_config(self):
        """
        Endpoints included in the config file will require security in the generated swagger yaml.

        Example config file content:
        post /search
        head /files/{uuid}
        get /files/{uuid}

        All endpoints apart from these will be open and callable without auth in the generated swagger yaml.
        """
        security_endpoints = defaultdict(list)
        with open(self.config, 'r') as f:
            for line in f:
                request_type, api_path = line.strip().split()
                security_endpoints[api_path].append(request_type)
        return security_endpoints

    def insert_security_flag(self, line):
        """Checks the lines of a swagger/yaml file and determines if a security flag should be written in."""
        # if not indented at all, we're in a new section, so reset
        if not line.startswith(' ') and self.path_section and line.strip() != '':
            self.path_section = False

        # check if we're in the paths section
        if line.startswith('paths:'):
            self.path_section = True
        # check if we're in an api path section
        elif line.startswith('  /'):
            self.call_section = None
            for api_path in self.security_endpoints:
                # make sure it's one of the specified api paths, otherwise ignore
                if line.startswith(f'  {api_path}:'):
                    self.call_section = self.security_endpoints[api_path]
        # if properly indented and we're in the correct (2) sections, this will be a call request
        elif line.startswith('    ') and not line.startswith('     ') and self.path_section and self.call_section:
            for call in self.call_section:
                # verify it's one of the specified calls we need to secure
                if line.startswith(f'    {call}:'):
                    return True
        return False

    def generate_swagger_with_secure_endpoints(self):
        with open(self.outfile, 'w') as w:
            with open(self.infile, 'r') as f:
                for line in f:
                    w.write(line)
                    if self.insert_security_flag(line):
                        w.write('      security:\n')
                        w.write('        - dcpAuth: []\n')

# SecureSwagger('/home/quokka/config_auth_dss/data-store/security.config').generate_swagger_with_secure_endpoints()
