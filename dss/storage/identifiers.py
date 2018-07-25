import re
from collections import namedtuple
from typing import Type

BUNDLE_PREFIX = "bundles"
FILE_PREFIX = "files"
COLLECTION_PREFIX = "collections"
TOMBSTONE_SUFFIX = "dead"

UUID_PATTERN = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
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
    f"^({BUNDLE_PREFIX}|{FILE_PREFIX}|{COLLECTION_PREFIX})/({UUID_PATTERN})(?:\.({VERSION_PATTERN}))?(\.{TOMBSTONE_SUFFIX})?$")  # noqa


class ObjectIdentifierError(ValueError):
    pass


class ObjectIdentifier(namedtuple('ObjectIdentifier', 'uuid version')):
    prefix = None  # type: str

    @classmethod
    def from_key(cls, key: str):
        match = DSS_OBJECT_NAME_REGEX.match(key)
        if match:
            object_type, uuid, version, tombstone_suffix = match.groups()
            if object_type == FILE_PREFIX:
                return FileFQID(uuid=uuid, version=version)
            elif object_type == BUNDLE_PREFIX:
                if not tombstone_suffix and uuid and version:
                    return BundleFQID(uuid=uuid, version=version)
                elif tombstone_suffix and uuid:
                    return BundleTombstoneID(uuid=uuid, version=version)
                else:
                    raise ObjectIdentifierError(f"Key does not contain a valid bundle identifier: {key}")
            elif object_type == COLLECTION_PREFIX:
                if not tombstone_suffix and uuid and version:
                    return CollectionFQID(uuid=uuid, version=version)
                elif tombstone_suffix and uuid:
                    return CollectionTombstoneID(uuid=uuid, version=version)
                else:
                    raise ObjectIdentifierError(f"Key does not contain a valid bundle identifier: {key}")
        raise ObjectIdentifierError(f"Key does not represent a valid identifier: {key}")

    def is_fully_qualified(self):
        return self.uuid is not None and self.version is not None

    def to_key(self):
        return f"{self.prefix}/{self}"

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

    def to_tombstone_id(self, all_versions=False):
        return BundleTombstoneID(uuid=self.uuid, version=None if all_versions else self.version)


class FileFQID(ObjectIdentifier):

    prefix = FILE_PREFIX


class TombstoneID(ObjectIdentifier):

    subject_identity_cls: Type[ObjectIdentifier] = None

    def __str__(self):
        if self.version:
            return f"{self.uuid}.{self.version}.{TOMBSTONE_SUFFIX}"
        else:
            return f"{self.uuid}.{TOMBSTONE_SUFFIX}"

    def to_fqid(self):
        if self.is_fully_qualified():
            return self.subject_identity_cls(uuid=self.uuid, version=self.version)
        else:
            raise ValueError(
                f"{self} does not define a version, therefore it can't be a {self.subject_identity_cls.__name__}.")


class BundleTombstoneID(TombstoneID):
    prefix = BUNDLE_PREFIX
    subject_identity_cls = BundleFQID

class CollectionFQID(ObjectIdentifier):
    prefix = COLLECTION_PREFIX

class CollectionTombstoneID(TombstoneID):
    prefix = COLLECTION_PREFIX
    subject_identity_cls = CollectionFQID
