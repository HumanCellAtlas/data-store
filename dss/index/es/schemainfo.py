from urllib.parse import urlparse
import re
from collections import namedtuple
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SchemaInfo(namedtuple('SchemaInfo', ['url', 'version', 'type'])):
    """
    Represents information about the JSON schema referenced by the JSON contents of an HCA metadata file.
    """
    schema_version_re = re.compile(r'([0-9]+)(?:\.[0-9]+){2}')

    @classmethod
    def from_json(cls, data: dict) -> Optional['SchemaInfo']:
        """
        Given a deserialized JSON structure representing the contents of a metadata file in a bundle,
        return an instance of this class describing the schema referenced by that JSON structure, or None if the
        structure does not reference a schema.
        """
        if data.get('describedBy'):
            # Examples of the URLs we're trying to parse:
            #
            # https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/assay.json
            # https://schema.humancellatlas.org/bundle/1.0.0/file
            #
            url = data['describedBy']
            url_path = urlparse(url).path.split('/')
            schema_type = url_path[-1]
            if schema_type.endswith('.json'):
                schema_type = schema_type[:-5]
            if not schema_type:
                raise RuntimeError(f"Schema type can't be empty in schema URL '{url}'")
            schema_version = None
            for path_element in url_path[:-1]:
                match = cls.schema_version_re.fullmatch(path_element)
                if match:
                    if schema_version is not None:
                        raise RuntimeError(f"Found more than one version designator in schema URL '{url}'")
                    schema_version = match.group(1)
            if schema_version is None:
                raise RuntimeError(f"No version designator in schema URL '{url}'")
            info = cls(url, schema_version, schema_type)
        elif data.get('core'):
            # TODO: Remove this if block once we stop supporting the `core` property (see issue #1015).
            core = data['core']
            schema_type = core['type']
            schema_version = core['schema_version'].split(".", 1)[0]
            url = core['schema_url']
            info = cls(url, schema_version, schema_type)
        else:
            # TODO: Remove 'core' from log message once we stop supporting the `core` property (see issue #1015).
            logger.info("Need either 'describedBy' or 'core' property to determine JSON schema. Both are absent.")
            info = None
        return info
