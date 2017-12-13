import os

import domovoi
import requests


app = domovoi.Domovoi()


@app.scheduled_function("rate(60 minutes)")
def integration_test(*args, **kwargs):
    travis_token = os.environ["TRAVIS_TOKEN"]
    body = {
        'request': {
            'branch': "master",
            'message': "INTEGRATION TESTS",
            'config': {
                'merge_mode': "deep_merge",
                'env': {
                    'matrix': ["TRAVIS_DSS_INTEGRATION_MODE=1"]
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
    )
