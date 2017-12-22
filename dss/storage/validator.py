"""Utilities in this file are used to removed extra fields from index data before adding to the index."""
import re
from jsonschema import validators
from jsonschema import _utils, _validators

import json
from urllib.request import urlopen
from urllib.error import URLError
import ssl
from typing import List
import logging


def read_json_url(link):
    gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    with urlopen(link, context=gcontext) as f:
        return json.load(f)


def remove_json_fields(json_data: dict, path: List[str], fields: List[str]):
    """
    Removes fields from the path in json_data.

    :param json_data: The JSON data from which to remove fields.
    :param path: A list of indices (either field names or array indices) forming a path through the JSON data.
    :param fields: A list of fields to remove from the JSON data at the location specified by path.
    """
    current = json_data
    for step in path:
        current = current[step]
    for field in fields:
        current.pop(field)


DSS_Draft4Validator = validators.create(
    meta_schema=_utils.load_schema("draft4"),
    validators={
        u"$ref": _validators.ref,
        u"additionalProperties": _validators.additionalProperties,
        u"properties": _validators.properties_draft4,
        u"required": _validators.required_draft4,
    },
    version="draft4",
)

resolver = validators.RefResolver(referrer='', base_uri='')


def scrub_index_data(index_data: dict, bundle_id: str, logger: logging.Logger) -> list:
    extra_fields = []
    extra_documents = []
    for document in index_data.keys():
        core = index_data[document].get('core', None)
        if core is not None:
            try:
                schema = read_json_url(core['schema_url'])
            except URLError as ex:
                extra_documents.append(document)
                logger.warning("%s", f"Unable to retrieve schema from url {core['schema_url']} due to exception: {ex}.")
                continue

            for error in DSS_Draft4Validator(schema, resolver=resolver).iter_errors(index_data[document]):
                if error.validator == 'additionalProperties':
                    path = [document, *error.path]
                    #  Example error message: "Additional properties are not allowed ('extra_lst', 'extra_top' were
                    #  unexpected)" or "'extra', does not match any of the regexes: '^characteristics_.*$'"
                    fields_to_remove = (path,
                                        [field for field in _utils.find_additional_properties(error.instance,
                                                                                              error.schema)])
                    extra_fields.append(fields_to_remove)
        else:
            extra_documents.append(document)
    if extra_documents:
        extra_fields.append(([], extra_documents))
    removed_fields = []
    for path, fields in extra_fields:
        remove_json_fields(index_data, path, fields)
        removed_fields.extend(['.'.join((*path, field)) for field in fields])
    if removed_fields:
        logger.info(f"In {bundle_id}, unexpected additional fields have been removed from the data"
                    f" to be indexed. Removed {removed_fields}.")
    return removed_fields
