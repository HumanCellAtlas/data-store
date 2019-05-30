import functools
import os
from requests import request, Response


class IntegrationTestHarness:
    def __init__(self):
        if "localhost" in os.environ["API_DOMAIN_NAME"]:
            self.domain = "http://" + os.environ["API_DOMAIN_NAME"]
        else:
            self.domain = "https://" + os.environ["API_DOMAIN_NAME"]

    @functools.lru_cache(maxsize=128, typed=False)
    def __getattr__(self, item):
        item = item.upper()
        return functools.partial(self.request, method=item)

    def request(self, path, headers=None, data='', method="GET"):
        resp_obj = Response()
        if not headers:
            headers = {}
        url = path if os.environ["API_DOMAIN_NAME"] in path else self.domain + path
        response = request(method, url, headers=headers, data=data, allow_redirects=False)
        resp_obj.status_code = response.status_code
        resp_obj.headers = response.headers
        resp_obj.body = response.content
        return resp_obj
