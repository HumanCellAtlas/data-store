from collections import defaultdict
import argparse
import sys
import os

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


class SecureSwagger(object):
    def __init__(self, infile=None, outfile=None, config=None):
        """

        :param infile:
        :param outfile:
        :param config:
        """
        self.path_section = False
        self.call_section = None
        self.infile = infile if infile else os.path.join(pkg_root, 'swagger_template')
        self.outfile = outfile if outfile else os.path.join(pkg_root, 'dss-api.yml')
        self.config = config if config else os.path.join(pkg_root, 'security.config')
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
        with open(self.outfile, 'w') as w:
            with open(self.infile, 'r') as f:
                for line in f:

                    if line.startswith('      security:') or line.startswith('        - dcpAuth: []'):
                        # Should this raise or ignore these lines? Raise seems better here to avoid weird use-cases.
                        raise RuntimeError('Invalid swagger template used.  File should not have security flags.')

                    w.write(line)
                    if self.insert_security_flag(line):
                        w.write('      security:\n')
                        w.write('        - dcpAuth: []\n')


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
