import os
import sys

import domovoi
import requests

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.logging import configure_daemon_logging


configure_daemon_logging()
app = domovoi.Domovoi(configure_logs=False)


def define_build(branch_name: str, build_type: str, schedule_expression: str, **env):
    @app.scheduled_function(schedule_expression)
    def build(event, context):
        travis_token = os.environ["TRAVIS_TOKEN"]
        rule_name = event["resources"][0].split(":")[-1]
        body = {
            'request': {
                'branch': branch_name,
                'message': f"{build_type} build started by {context.function_name} from {rule_name}",
                'config': {
                    'merge_mode': "deep_merge",
                    'env': {
                        'matrix': [f"{k}={v}" for k, v in env.items()]
                    },
                },
            }
        }
        headers = {
            'Accept': "application/json",
            'Travis-API-Version': "3",
            'Authorization': f"token {travis_token}",
        }
        return requests.post(
            "https://api.travis-ci.org/repo/HumanCellAtlas%2Fdata-store/requests",
            headers=headers,
            json=body,
        ).json()


define_build("master", "Integration test", "rate(60 minutes)", TRAVIS_DSS_INTEGRATION_MODE=1)
