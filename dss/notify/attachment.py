import json
from typing import Mapping, Union

import jmespath
from jmespath.exceptions import JMESPathError
import requests

from dss import DSSException
from dss.util.types import JSON

# This 50% of the maximum size of an SQS message. Keep in mind that the payload will be gzipp'ed and base85 eoncoded.
#
size_limit = 128 * 1024


# See Swagger schema for details on the structure of these
#
Definitions = Mapping[str, Mapping[str, str]]
Attachments = Mapping[str, JSON]


def validate(definitions: Definitions) -> None:
    """
    Validate the given attachement definitions. This should be called in a request handling context as it raises
    DSSException referring to HTTP status code, as well as error code and description.
    """
    for name, definition in definitions.items():
        if name.startswith('_'):
            raise DSSException(requests.codes.bad_request,
                               "invalid_attachment_name",
                               f"Attachment names must not start with underscore ({name})")
        type_ = definition['type']
        if type_ == 'jmespath':
            expression = definition['expression']
            try:
                jmespath.compile(expression)
            except JMESPathError as e:
                raise DSSException(requests.codes.bad_request,
                                   "invalid_attachment_expression",
                                   f"Unable to compile JMESPath expression for attachment {name}") from e
        else:
            assert False, type_


def select(definitions: Definitions, document: JSON) -> Attachments:
    """
    Return a defined subset of the given document for the pupose of attaching that subset to a notification about that
    document.
    """
    attachments = {}
    errors = {}
    for name, attachment in definitions.items():
        type_ = attachment['type']
        if type_ == 'jmespath':
            try:
                expression = attachment['expression']
                value = jmespath.search(expression, document)
            except BaseException as e:
                errors[name] = str(e)
            else:
                attachments[name] = value
        else:
            assert False, type_
    if errors:
        attachments['_errors'] = errors
    size = len(json.dumps(attachments).encode('utf-8'))
    if size > size_limit:
        attachments = {'_errors': f"Attachments too large ({size} > {size_limit})"}
    return attachments
