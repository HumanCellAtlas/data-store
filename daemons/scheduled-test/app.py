import os

import domovoi
import requests


app = domovoi.Domovoi()


def define_test_type(test_type: str, envvar: str, schedule_expression: str):
    @app.scheduled_function(schedule_expression)
    def integration_test(event, context):
        travis_token = os.environ["TRAVIS_TOKEN"]
        rule_name = event["resources"][0].split(":")[-1]
        body = {
            'request': {
                'branch': "master",
                'message': f"{test_type} test started by {context.function_name} from {rule_name}",
                'config': {
                    'merge_mode': "deep_merge",
                    'env': {
                        'matrix': [f"{envvar}=1"]
                    },
                },
            }}
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


define_test_type("Integration", "TRAVIS_DSS_INTEGRATION_MODE", "rate(60 minutes)")
