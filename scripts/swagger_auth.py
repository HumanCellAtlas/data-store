#!/usr/bin/env python
import argparse
import sys
import os
import json
from collections import defaultdict

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

"""A script to add/subtract auth from the data-store's swagger file."""

default_auth = {"/files/{uuid}": ["put"],
                "/subscriptions": ["get", "put"],
                "/subscriptions/{uuid}": ["get", "delete"],
                "/collections": ["get", "put"],
                "/collections/{uuid}": ["get", "patch", "delete"],
                "/bundles/{uuid}": ["put", "patch", "delete"]
               }

# all endpoints
full_auth = {"/search": ["post"],
             "/files/{uuid}": ["head", "get", "put"],
             "/subscriptions": ["get", "put"],
             "/subscriptions/{uuid}": ["get", "delete"],
             "/collections": ["get", "put"],
             "/collections/{uuid}": ["get", "patch", "delete"],
             "/bundles": ["get"],
             "/bundles/{uuid}": ["get", "put", "patch", "delete"],
             "/bundles/{uuid}/checkout": ["post"],
             "/bundles/checkout/{checkout_job_id}": ["get"]
            }


class SecureSwagger(object):
    def __init__(self, infile: str=None, outfile: str=None, config: dict=None):
        """
        A class for modifying a swagger yml file with auth on endpoints specified in
        a config file.

        :param infile: Swagger yml file.
        :param outfile: The name of the generated swagger yml file (defaults to the same file).
        :param config: A json file containing the api endpoints that need auth.
        """
        # used to track which section we're when parsing the yml
        self.path_section = False  # bool flag to notify if we're in the section containing the API call definitions
        self.call_section = None  # an api endpoint, e.g.: /subscription, /file/{uuid}, etc.
        self.request_section = None  # a request call, e.g.: get, put, delete, etc.

        self.infile = infile or os.path.join(pkg_root, 'dss-api.yml')
        self.intermediate_file = os.path.join(pkg_root, 'tmp.yml')
        self.outfile = outfile or os.path.join(pkg_root, 'dss-api.yml')
        self.config = default_auth if config is None else config

        for endpoint in self.config:
            if not isinstance(self.config[endpoint], list):
                raise TypeError('Auth config dict keys are strings, values are lists of strings.  '
                                'Example: {"/search": ["put"]}.  Check your input!')

        self.security_endpoints = defaultdict(list)

    def security_line(self, line: str, checking_flags: bool, all_endpoints=False):
        """
        Checks a line from the swagger/yml file and updates section values appropriately.

        If checking_flags is True, this will return True/False:
            True if a call/path matches one in self.security_endpoints.
            False otherwise.

        If checking_flags is False, this will create the self.security_endpoints dictionary:
            If all_endpoints=True, self.security_endpoints will include all endpoints in the swagger file.
            If all_endpoints=False, self.security_endpoints will include only auth endpoints in the swagger file.
        """
        # If not indented at all, we're in a new section, so reset.
        if not line.startswith(' ') and self.path_section and line.strip() != '':
            self.path_section = False

        # Check if we're in the paths section.
        if line.startswith('paths:'):
            self.path_section = True

        # Check if we're in an api path section.
        elif line.startswith('  /') and line.strip().endswith(':'):
            self.parse_api_section(line, checking_flags)

        # Check for an endpoint's security flag
        elif line.startswith('      security:'):
            if not checking_flags and not all_endpoints:
                # If we're checking for secured endpoints only, record the path and call.
                self.security_endpoints[self.call_section].append(self.request_section)

        # If properly indented and we're in the correct 2 sections, this will be a call request.
        elif self.call_indent(line) and self.path_section and self.call_section and line.strip().endswith(':'):
            if checking_flags:
                for call in self.call_section:
                    # Verify it's one of the specified calls we need to secure_auth.
                    if line.startswith(f'    {call}:'):
                        return True
            else:
                self.request_section = line.strip()[:-1]
                if all_endpoints:
                    # If we're checking for all endpoints present, record the path and call.
                    self.security_endpoints[self.call_section].append(self.request_section)

    @staticmethod
    def call_indent(line: str) -> bool:
        return line.startswith('    ') and not line.startswith('     ')

    def parse_api_section(self, line: str, checking_flags: bool) -> None:
        if checking_flags:
            self.call_section = None
            for api_path in self.security_endpoints:
                # Make sure it's one of the specified api paths, otherwise ignore.
                if line.startswith(f'  {api_path}:'):
                    self.call_section = self.security_endpoints[api_path]
        else:
            self.call_section = line.strip()[:-1]

    def make_swagger_from_authconfig(self) -> None:
        """Modify a swagger file's auth based on a config dict."""
        self.security_endpoints = self.config

        # generate a new swagger as an intermediate file
        with open(self.intermediate_file, 'w') as w:
            with open(self.infile, 'r') as r:
                for line in r:
                    # ignore security lines already in the swagger yml
                    if not (line.startswith('      security:') or line.startswith('        - dcpAuth: []')):

                        w.write(line)
                        # returns true based on config file paths
                        if self.security_line(line, checking_flags=True):
                            w.write('      security:\n')
                            w.write('        - dcpAuth: []\n')

        # the contents of the intermediate file become the contents of the output file
        if os.path.exists(self.outfile):
            os.remove(self.outfile)
        os.rename(self.intermediate_file, self.outfile)

    def get_authconfig_from_swagger(self, all_endpoints=False):
        """
        Return a dictionary representing the endpoints that have auth enabled in a swagger file.

        If all_endpoints is True, instead return all endpoints, not just those with auth.
        """
        self.security_endpoints = defaultdict(list)

        with open(self.infile, 'r') as f:
            for line in f:
                self.security_line(line, checking_flags=False, all_endpoints=all_endpoints)
        return self.security_endpoints


def ensure_auth_defaults_are_still_set():
    """To be run on travis to make sure that no one makes a PR with a custom swagger accidentally."""
    if SecureSwagger().get_authconfig_from_swagger() != default_auth:
        raise TypeError('Swagger file auth does not match defaults.  Please modify accordingly.')


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Swagger Security Endpoint')
    parser.add_argument('-i', '--input_swagger', dest="input_swagger", default=None,
                        help='An input swagger yaml file/path that will be modified to contain '
                             'new security auth based on the input config.')
    parser.add_argument('-o', '--output_swagger', dest="output_swagger", default=None,
                        help='The file/path of the swagger output yaml.')
    parser.add_argument('-c', '--config_security', dest="config_security", default=default_auth,
                        type=json.loads, help='''A dict of API calls stating which calls to add 
                        auth to. For example: -s='{"/path": "call"}'.''')
    parser.add_argument('-s', '--secure', dest="secure", default=False, action='store_true',
                        help='Change the swagger file to include auth on all endpoints.')
    parser.add_argument('-t', '--travis', dest="travis", action='store_true', default=False,
                        help='Run on travis to check that swagger has default auth.')
    o = parser.parse_args(argv)

    if o.travis:
        ensure_auth_defaults_are_still_set()
    else:
        config = full_auth if o.secure else o.config_security
        s = SecureSwagger(o.input_swagger, o.output_swagger, config)
        s.make_swagger_from_authconfig()


if __name__ == '__main__':
    main(sys.argv[1:])
