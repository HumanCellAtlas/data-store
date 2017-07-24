from urllib.parse import SplitResult, parse_qs, urlencode, urlparse, urlunsplit


def paginate(boto3_paginator, *args, **kwargs):
    for page in boto3_paginator.paginate(*args, **kwargs):
        for result_key in boto3_paginator.result_keys:
            for value in page.get(result_key.parsed.get("value"), []):
                yield value


class UrlBuilder:
    def __init__(self):
        self.splitted = SplitResult("", "", "", "", "")
        self.query = list()

    def set(self, scheme: str=None, netloc: str=None, path: str=None, fragment: str=None) -> "UrlBuilder":
        kwargs = dict()
        if scheme is not None:
            kwargs['scheme'] = scheme
        if netloc is not None:
            kwargs['netloc'] = netloc
        if path is not None:
            kwargs['path'] = path
        if fragment is not None:
            kwargs['fragment'] = fragment
        self.splitted = self.splitted._replace(**kwargs)

        return self

    def add_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        self.query.append((query_name, query_value))

        return self

    def __str__(self) -> str:
        result = self.splitted._replace(query=urlencode(self.query, doseq=True))

        return urlunsplit(result)
