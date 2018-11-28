import argparse
import sys
import os
import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


class SecureSwagger(object):
    def __init__(self, infile=None, outfile=None, config=None):
        """
        A class for modifying a swagger yml file with auth on endpoints specified in
        a config file.

        :param infile: Swagger yml file.
        :param outfile: The name of the generated swagger yml file (defaults to the same file).
        :param config: A json file containing the api endpoints that need auth.
        """
        self.path_section = False  # used to track which section we're when parsing the yml
        self.call_section = None  # used to track which section we're when parsing the yml
        self.infile = infile if infile else os.path.join(pkg_root, 'dss-api.yml')
        self.intermediate_file = os.path.join(pkg_root, 'tmp.yml')
        self.outfile = outfile if outfile else os.path.join(pkg_root, 'dss-api.yml')
        self.config = config if config else os.path.join(pkg_root, 'hca_auth_defaults.json')
        self.security_endpoints = self.security_from_config()

    def security_from_config(self):
        """
        Endpoints included in the config json file will require security in the generated swagger yml.

        Example config file content:
        {
          "/files/{uuid}": [
            "put"
          ],
          "/subscriptions": [
            "get",
            "put"
          ]
        }

        All endpoints apart from these will be open and callable without auth in the generated swagger yml.
        """
        with open(self.config, 'r') as f:
            return json.loads(f.read())

    def insert_security_flag(self, line):
        """Checks the lines of a swagger/yml file and determines if a security flag should be written in."""
        # If not indented at all, we're in a new section, so reset.
        if not line.startswith(' ') and self.path_section and line.strip() != '':
            self.path_section = False

        # Check if we're in the paths section.
        if line.startswith('paths:'):
            self.path_section = True
        # Check if we're in an api path section.
        elif line.startswith('  /'):
            self.call_section = None
            for api_path in self.security_endpoints:
                # Make sure it's one of the specified api paths, otherwise ignore.
                if line.startswith(f'  {api_path}:'):
                    self.call_section = self.security_endpoints[api_path]
        # If properly indented and we're in the correct (2) sections, this will be a call request.
        elif line.startswith('    ') and not line.startswith('     ') and self.path_section and self.call_section:
            for call in self.call_section:
                # Verify it's one of the specified calls we need to secure.
                if line.startswith(f'    {call}:'):
                    return True
        return False

    def generate_swagger_with_secure_endpoints(self):
        # generate a new swagger as an intermediate file
        with open(self.intermediate_file, 'w') as w:
            with open(self.infile, 'r') as r:
                for line in r:
                    # ignore security lines already in the swagger yml
                    if not (line.startswith('      security:') or line.startswith('        - dcpAuth: []')):

                        w.write(line)
                        if self.insert_security_flag(line):
                            w.write('      security:\n')
                            w.write('        - dcpAuth: []\n')

        # the contents of the intermediate file become the contents of the output file
        if os.path.exists(self.outfile):
            os.remove(self.outfile)
        os.rename(self.intermediate_file, self.outfile)


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Swagger Security Endpoint')
    parser.add_argument('-i', '--input_swagger_template', dest="input_swagger_template", default=None,
                        help='An input swagger template devoid of any security flags (to be filled in by this '
                             'script from the configuration file).')
    parser.add_argument('-o', '--output_swagger_yaml', dest="output_swagger_yaml", default=None,
                        help='The name/path of the swagger output yaml file.')
    parser.add_argument('-s', '--security_config', dest="security_config", default=None,
                        help='A list of api requests that will be flagged for auth in the generated swagger '
                             'yaml file.')
    o = parser.parse_args(argv)

    s = SecureSwagger(o.input_swagger_template, o.output_swagger_yaml, o.security_config)
    s.generate_swagger_with_secure_endpoints()


if __name__ == '__main__':
    main(sys.argv[1:])
