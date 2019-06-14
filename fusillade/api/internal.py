import requests
from connexion.lifecycle import ConnexionResponse
from flask import request
from furl import furl

from fusillade.config import Config


def version():
    data = {
        'version_info': {
            'version': Config.version
        }
    }
    return ConnexionResponse(
        status_code=requests.codes.ok,
        headers={'Content-Type': "application/json"},
        body=data
    )


def health_check(*args, **kwargs):
    health_checks = dict()
    health_checks.update(**Config.get_directory().get_health_status(),
                         **get_openip_health_status())
    if all([check == 'ok' for check in health_checks.values()]):
        body = dict(
            health_status='ok',
            services=health_checks
        )
        status = 200
        headers = {"Content-Type": "application/json"}
    else:
        body = dict(
            health_status='unhealthy',
            services=health_checks
        )
        status = 500
        headers = {"Content-Type": "application/json+problem"}
    return ConnexionResponse(status_code=status,
                             headers=headers,
                             body=body)


def get_openip_health_status() -> dict:
    status = requests.get(
        furl(scheme="https", host=Config.get_openid_provider(), path="/testall").url).status_code
    status = 'ok' if status == 200 else 'unhealthy'
    return dict(openip_health_status=status)


def echo():
    return str(request.__dict__)
