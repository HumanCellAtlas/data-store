import json

import requests
from connexion.lifecycle import ConnexionResponse
from flask import request


def version():
    data = {
        'version_info': {
            'version': 0.0
        }
    }
    return ConnexionResponse(
        status_code=requests.codes.ok,
        headers={'Content-Type': "application/json"},
        body=data
    )


def health_check(*args, **kwargs):
    health_status = 'OK'
    return ConnexionResponse(status_code=200,
                             headers={"Content-Type": "application/json"},
                             body=json.dumps(health_status, indent=4, sort_keys=True, default=str))


def echo():
    return str(request.__dict__)
