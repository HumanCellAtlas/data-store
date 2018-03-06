import re
from collections import namedtuple
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# https://somewhere.anything/1.2.3/anything/type123.json
# "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/project_bundle.json"
schema_url_regex = re.compile(r'/(?P<major_version>[0-9]+)\.[0-9]+\.[0-9]+[\w/]*/(?P<type>[\w]+)(\.json)?$')


class SchemaInfo(namedtuple('SchemaInfo', ['url', 'version', 'type'])):
    @classmethod
    def from_json(cls, data: dict) -> Optional['SchemaInfo']:
        if data.get('describedBy'):
            url = data['describedBy']
            version, doc_type = schema_url_regex.search(url).group('major_version', 'type')
            info = cls(url, version, doc_type)
        elif data.get('core'):
            # TODO: Remove this if block once we stop supporting the `core` property (see issue #1015).
            doc_type = data['core']['type']
            version = data['core']['schema_version'].split(".")[0]
            url = data['core']['schema_url']
            info = cls(url, version, doc_type)
        else:
            # TODO: Remove 'core' from log message once we stop supporting the `core` property (see issue #1015).
            logger.info("Need either 'describedBy' or 'core' property to determine JSON schema. Both are absent.")
            info = None
        return info
