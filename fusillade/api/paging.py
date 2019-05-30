from flask import request, make_response, jsonify
from furl import furl


def get_page(func, next_token, per_page, *args):
    if args:
        result, next_token = func(*args, next_token=next_token, per_page=per_page)
    else:
        result, next_token = func(next_token, per_page)
    if next_token:
        next_url = build_next_url(next_token, per_page)
        headers = {'Link': build_link_header({next_url: {"rel": "next"}})}
        return make_response(jsonify(result), 206, headers)
    else:
        return make_response(jsonify(result), 200)


def get_next_token(query_params: dict):
    next_token = query_params.get('next_token')
    per_page = int(query_params['per_page']) if query_params.get('per_page') else None
    return next_token, per_page


def build_next_url(next_token: str, per_page: int) -> str:
    url = furl(request.host_url, path=request.path, query_params={'next_token': next_token, 'per_page': per_page})
    if not url.scheme:
        if 'localhost' == url.host:
            url.scheme = 'http'
        else:
            url.scheme = 'https'
    return url.url


def build_link_header(links):
    """
    Builds a Link header according to RFC 5988.
    The format is a dict where the keys are the URI with the value being
    a dict of link parameters:
        {
            '/page=3': {
                'rel': 'next',
            },
            '/page=1': {
                'rel': 'prev',
            },
            ...
        }
    See https://tools.ietf.org/html/rfc5988#section-6.2.2 for registered
    link relation types.
    """
    _links = []
    for uri, params in links.items():
        link = [f"<{uri}>"]
        for key, value in params.items():
            link.append(f'{key}="{str(value)}"')
        _links.append('; '.join(link))
    return ', '.join(_links)
