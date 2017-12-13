import re
import typing

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
    f"^{BUNDLE_PREFIX}/({UUID_PATTERN})(?:\.({VERSION_PATTERN}))?(?:\.{TOMBSTONE_SUFFIX})?$")


def bundle_key_to_bundle_fqid(bundle_key: str) -> str:
    match = DSS_OBJECT_NAME_REGEX.match(bundle_key)
    uuid, version = match.groups() if match else (None, None)
    if uuid and version:
        return format_bundle_fqid(uuid, version)
    else:
        raise ValueError(f"Object name does not contain a valid bundle identifier: {bundle_key}")


def bundle_fqid_to_uuid_version(bundle_key: str) -> (str, str):
    match = DSS_BUNDLE_FQID_REGEX.match(bundle_key)
    uuid, version = match.groups() if match else (None, None)
    if uuid and version:
        return uuid, version
    else:
        raise ValueError(f"Object name does not contain a valid bundle identifier: {bundle_key}")


def format_bundle_fqid(uuid: str, version: str) -> str:
    if UUID_REGEX.match(uuid) and VERSION_REGEX.match(version):
        return f"{uuid}.{version}"
    else:
        raise ValueError(f"Not a valid version regex-version pair: {uuid}, {version}")


def bundle_key(uuid: str, version: str) -> str:
    return f"{BUNDLE_PREFIX}/{format_bundle_fqid(uuid, version)}"


def file_key(uuid: str, version: str) -> str:
    return f"{FILE_PREFIX}/{format_bundle_fqid(uuid, version)}"


def tombstone_key(uuid: str, version: typing.Optional[str]) -> str:
    if version:
        return f"{BUNDLE_PREFIX}/{uuid}.{version}.{TOMBSTONE_SUFFIX}"
    else:
        return f"{BUNDLE_PREFIX}/{uuid}.{TOMBSTONE_SUFFIX}"
