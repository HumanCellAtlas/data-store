from urllib.parse import SplitResult, urlencode, urlunsplit

import typing

from dss.storage.hcablobstore import BundleFileMetadata


def create_blob_key(file_info: typing.Dict[str, str]) -> str:
    return "blobs/" + ".".join((
        file_info[BundleFileMetadata.SHA256],
        file_info[BundleFileMetadata.SHA1],
        file_info[BundleFileMetadata.S3_ETAG],
        file_info[BundleFileMetadata.CRC32C]
    ))


def paginate(boto3_paginator, *args, **kwargs):
    for page in boto3_paginator.paginate(*args, **kwargs):
        for result_key in boto3_paginator.result_keys:
            for value in page.get(result_key.parsed.get("value"), []):
                yield value


class UrlBuilder:
    def __init__(self):
        self.splitted = SplitResult("", "", "", "", "")
        self.query = list()

    def set(
            self,
            scheme: str=None,
            netloc: str=None,
            path: str=None,
            query: typing.Sequence[typing.Tuple[str, str]]=None,
            fragment: str=None) -> "UrlBuilder":
        kwargs = dict()
        if scheme is not None:
            kwargs['scheme'] = scheme
        if netloc is not None:
            kwargs['netloc'] = netloc
        if path is not None:
            kwargs['path'] = path
        if query is not None:
            self.query = query
        if fragment is not None:
            kwargs['fragment'] = fragment
        self.splitted = self.splitted._replace(**kwargs)

        return self

    def has_query(self, needle_query_name: str) -> bool:
        """Returns True iff the URL being constructed has a query field with name `needle_query_name`."""
        for query_name, _ in self.query:
            if query_name == needle_query_name:
                return True
        return False

    def add_query(self, query_name: str, query_value: str) -> "UrlBuilder":
        self.query.append((query_name, query_value))

        return self

    def __str__(self) -> str:
        result = self.splitted._replace(query=urlencode(self.query, doseq=True))

        return urlunsplit(result)
