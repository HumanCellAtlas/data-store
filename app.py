import json
import os
import sys

import yaml
from botocore.vendored import requests
from chalice import Response as chalice_response

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))
sys.path.insert(0, pkg_root)  # noqa

from fusillade import Config, logging
from fusillade.api import FusilladeServer

logging.configure_lambda_logging()

with open(os.path.join(pkg_root, "service_config.json")) as fh:
    service_config = json.load(fh)
    Config.version = service_config['version']
    Config.directory_schema_version = (service_config['directory_schema']['Version'],
                                       service_config['directory_schema']['MinorVersion'])

swagger_spec_path = os.path.join(pkg_root, "fusillade-api.yml")
swagger_internal_spec_path = os.path.join(pkg_root, "fusillade-internal-api.yml")
app = FusilladeServer(app_name='fusillade', swagger_spec_path=swagger_spec_path,
                      swagger_internal_spec_path=swagger_internal_spec_path)
Config.app = app


@app.route("/")  # TODO use connexion swagger ui and remove
def serve_swagger_ui():
    with open(os.path.join(pkg_root, "index.html")) as fh:
        swagger_ui_html = fh.read()
    return chalice_response(status_code=requests.codes.ok,
                            headers={"Content-Type": "text/html"},
                            body=swagger_ui_html)


@app.route('/swagger.json')
def serve_swagger_definition():
    with open(swagger_spec_path) as fh:
        swagger_defn = yaml.load(fh.read())
    return swagger_defn
