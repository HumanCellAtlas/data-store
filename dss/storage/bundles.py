import re
from abc import abstractmethod
from collections import namedtuple

BUNDLE_PREFIX = "bundles"
FILE_PREFIX = "files"
TOMBSTONE_SUFFIX = "dead"

UUID_PATTERN = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
UUID_REGEX = re.compile(UUID_PATTERN)

VERSION_PATTERN = "\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}[.]\d{6}Z"
VERSION_REGEX = re.compile(VERSION_PATTERN)

# matches just fully qualified bundle identifiers
DSS_BUNDLE_FQID_PATTERN = f"({UUID_PATTERN})\.({VERSION_PATTERN})"
DSS_BUNDLE_FQID_REGEX = re.compile(DSS_BUNDLE_FQID_PATTERN)

# matches just bundle keys
DSS_BUNDLE_KEY_REGEX = re.compile(f"^{BUNDLE_PREFIX}/{DSS_BUNDLE_FQID_PATTERN}$")
# matches just bundle tombstones
DSS_BUNDLE_TOMBSTONE_REGEX = re.compile(
    f"^{BUNDLE_PREFIX}/({UUID_PATTERN})(?:\.(" + VERSION_PATTERN + "))?\." + TOMBSTONE_SUFFIX + "$")
# matches all bundle objects
DSS_OBJECT_NAME_REGEX = re.compile(
    f"^({BUNDLE_PREFIX}|{FILE_PREFIX})/({UUID_PATTERN})(?:\.({VERSION_PATTERN}))?(\.{TOMBSTONE_SUFFIX})?$")


class ObjectIdentifier(namedtuple('ObjectIdentifier', 'uuid version')):

    @classmethod
    def from_key(cls, key: str):
        match = DSS_OBJECT_NAME_REGEX.match(key)
        object_type, uuid, version, tombstone_suffix = match.groups() if match else (None, None, None, None)
        if object_type == FILE_PREFIX:
            return FileFQID(uuid=uuid, version=version)
        elif object_type == BUNDLE_PREFIX:
            if not tombstone_suffix and uuid and version:
                return BundleFQID(uuid=uuid, version=version)
            elif tombstone_suffix and uuid:
                return TombstoneID(uuid=uuid, version=version)
            else:
                raise ValueError(f"Object name does not contain a valid bundle identifier: {key}")
        else:
            raise ValueError(f"Object name does not contain a valid bundle identifier: {key}")

    def is_fully_qualified(self):
        return self.uuid is not None and self.version is not None

    def to_key(self):
        return f"{self.prefix}/{self}"

    @property
    @abstractmethod
    def prefix(self):
        return NotImplementedError("'prefix' not implemented!")

    def to_key_prefix(self):
        return f"{self.prefix}/{self.uuid}.{self.version or ''}"

    def __str__(self):
        return f"{self.uuid}.{self.version}"

    def __iter__(self):
        """
        When composing a request URL, the Elasticseach client interpolates lists and tuples into the URL path by
        joining their elements with a comma. ObjectIdentifier instances are tuples, so when such an instance is passed
        to a client method, a result looks almost indistinguishable to the actual string representation of an
        ObjectIdentifier instance, which uses a period between the elements.
        """
        raise NotImplementedError(f"{type(self).__name__} instances should not be iterated over.")


class BundleFQID(ObjectIdentifier):

    prefix = BUNDLE_PREFIX


class FileFQID(ObjectIdentifier):

    prefix = FILE_PREFIX


class TombstoneID(ObjectIdentifier):

    prefix = BUNDLE_PREFIX

    def __str__(self):
        if self.version:
            return f"{self.uuid}.{self.version}.{TOMBSTONE_SUFFIX}"
        else:
            return f"{self.uuid}.{TOMBSTONE_SUFFIX}"

    def to_bundle_fqid(self):
        if self.is_fully_qualified():
            return BundleFQID(uuid=self.uuid, version=self.version)
        else:
            raise ValueError(f"{self} does not define a version, therefore it can't be a Bundle FQID.")
