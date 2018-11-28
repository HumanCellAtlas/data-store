import os
from collections import defaultdict

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))  # noqa


def determine_auth_configuration_from_swagger():
    """

    :return:
    """
    path_section = False  # bool flag to notify if we're in the section containing the API call definitions
    call_section = None  # an api endpoint, e.g.: /subscription, /file/{uuid}, etc.
    request_section = None  # a request call, e.g.: get, put, delete, etc.
    security_endpoints = defaultdict(list)
    with open(os.path.join(pkg_root, 'dss-api.yml'), 'r') as f:
        for line in f:
            # If not indented at all, we're in a new section, so reset.
            if not line.startswith(' ') and path_section and line.strip() != '':
                path_section = False

            # Check if we're in the paths section.
            if line.startswith('paths:'):
                path_section = True
            # Check if we're in an api path section.
            elif line.startswith('  /') and line.strip().endswith(':'):
                call_section = line.strip()[:-1]
            # If properly indented and we're in the correct section, this will be a call request.
            elif line.startswith('    ') and not line.startswith('     ') and \
                    path_section and line.strip().endswith(':'):
                request_section = line.strip()[:-1]
                security_endpoints[call_section].append(request_section)
    return security_endpoints

print(determine_auth_configuration_from_swagger())
