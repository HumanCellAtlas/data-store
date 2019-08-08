"""
Tools for managing checkout buckets
"""
import typing
import argparse
import logging
import json
import time
import collections

from dss import Replica
from dss.operations import dispatch
from dss import Config
from cloud_blobstore import BlobNotFoundError
from dss.api.bundles import get_bundle_manifest
from dss.storage.hcablobstore import compose_blob_key
from dss.storage.identifiers import DSS_BUNDLE_KEY_REGEX, VERSION_REGEX, UUID_REGEX, FILE_PREFIX
from dss.storage.checkout.bundle import verify_checkout as bundle_checkout
from dss.api.files import _verify_checkout as file_checkout
from dss.storage.checkout import cache_flow

logger = logging.getLogger(__name__)

checkout = dispatch.target("checkout", help=__doc__)


def verify_delete(handle, bucket, key):
    logger.warning(f'attempting removal of key: {bucket}/{key}')
    try:
        handle.delete(bucket=bucket, key=key)
        handle.get(bucket=bucket, key=key)
    except BlobNotFoundError:
        logger.warning(f'Success! unable to locate key {bucket}/{key} ')
    else:
        raise RuntimeError(f'attempted to delete {bucket}/{key} but it did not work ;(')


def verify_get(handle, bucket, key):
    """ Helper Function to get file_metadata from cloud-blobstore"""
    try:
        file_metadata = json.loads(handle.get(bucket=bucket, key=key).decode('utf-8'))
    except BlobNotFoundError:
        logger.warning(f'unable to locate {bucket}/{key}')
        return None
    return file_metadata


def verify_blob_existance(handle, bucket, key):
    """Helper Functiont to see if blob data exists"""
    try:
        file_metadata = handle.get(bucket=bucket, key=key)
    except BlobNotFoundError:
        return False
    return True


def sleepy_checkout(function=None, **kwargs):
    status = False
    token = None
    key = kwargs["file_metadata"]['name'] if kwargs.get('file_metadata') else kwargs.get("bundle_uuid")
    logger.warning(f'starting {function.__name__} on {key}')
    while status is not True:
        kwargs['token'] = token
        token, status = function(**kwargs)
        if not status:
            time.sleep(4)  # wait 10 seconds for checkout to complete, then try again


def parse_key(key):
    try:
        version = VERSION_REGEX.search(key).group(0)
        uuid = UUID_REGEX.search(key).group(0)
    except IndexError:
        RuntimeError(f'unable to parse the key {key}')
    return uuid, version


@checkout.action("remove",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys to remove from checkout", required=False)})
def remove(argv: typing.List[str], args: argparse.Namespace):
    """
    Remove a bundle from the checkout bucket
    """
    replica = Replica[args.replica]
    handle = Config.get_blobstore_handle(replica)
    bucket = replica.checkout_bucket
    if args.keys:
        for _key in args.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):
                for key in handle.list(bucket, _key):  # handles checkout/bundle/*
                    verify_delete(handle, bucket, key)
                uuid, version = parse_key(_key)
                manifest = get_bundle_manifest(replica=replica, uuid=uuid, version=version)
                if manifest is None:
                    logger.warning(f"unable to locate manifest for fqid: {bucket}/{_key}")
                    continue
                for _files in manifest['files']:
                    key = compose_blob_key(_files)
                    verify_delete(handle, bucket, key)
            elif FILE_PREFIX in _key:
                # should handle other keys, files/blobs
                file_metadata = verify_get(handle, replica.bucket, _key)
                verify_delete(handle, bucket, key=compose_blob_key(file_metadata))


@checkout.action("verify",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys to check the status of", required=False)})
def verify(argv: typing.List[str], args: argparse.Namespace):
    """
    Verify that keys are in the checkout bucket
    """
    replica = Replica[args.replica]
    handle = Config.get_blobstore_handle(replica)
    bucket = replica.checkout_bucket
    checkout_status = dict()
    for _key in args.keys:
        if DSS_BUNDLE_KEY_REGEX.match(_key):  # handles bundles/fqid keys or fqid
            uuid, version = parse_key(_key)
            bundle_manifest = get_bundle_manifest(replica=replica, uuid=uuid, version=version)
            checkout_bundle_contents = [x[0] for x in handle.list_v2(bucket=bucket, prefix=f'bundles/{uuid}.{version}')]
            bundle_internal_status = list()

            for _file in bundle_manifest['files']:
                temp = collections.defaultdict(blob_checkout=False, bundle_checkout=False, should_be_cached=False)
                bundle_key = f'bundles/{uuid}.{version}/{_file["name"]}'
                blob_key = compose_blob_key(_file)

                blob_status = verify_blob_existance(handle, bucket, blob_key)
                if blob_status:
                    temp['blob_checkout'] = True
                if bundle_key in checkout_bundle_contents:
                    temp['bundle_checkout'] = True
                if cache_flow.should_cache_file(_file['content-type'], _file['size']):
                    temp['should_be_cached'] = True

                for x in ['name', 'uuid', 'version']:
                    temp.update({x: _file[x]})
                bundle_internal_status.append(temp)
            checkout_status[_key] = bundle_internal_status
        elif FILE_PREFIX in _key:
            temp = collections.defaultdict(blob_checkout=False, should_be_cached=False)
            file_metadata = verify_get(handle, replica.bucket, _key)
            blob_key = compose_blob_key(file_metadata)
            blob_status = verify_blob_existance(handle, bucket, blob_key)
            if blob_status:
                temp['blob_checkout'] = True
            if cache_flow.should_cache_file(_file['content-type'], _file['size']):
                temp['should_be_cached'] = True

            for x in ['name', 'uuid', 'version']:
                temp.update({x: _file[x]})
            checkout_status[_key] = collections.defaultdict(uuid=temp)
    print(json.dumps(checkout_status, indent=4, sort_keys=True))


@checkout.action("start",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys to check the status of", required=False)})
def start(argv: typing.List[str], args: argparse.Namespace):
    replica = Replica[args.replica]
    handle = Config.get_blobstore_handle(replica)
    bucket = replica.checkout_bucket
    for _key in args.keys:
        if DSS_BUNDLE_KEY_REGEX.match(_key):
            uuid, version = parse_key(_key)
            bundle_manifest = get_bundle_manifest(uuid=uuid, replica=replica, version=version)
            sleepy_checkout(bundle_checkout, replica=replica, bundle_uuid=uuid, bundle_version=version)
            for _files in bundle_manifest['files']:
                blob_path = compose_blob_key(_files)
                sleepy_checkout(file_checkout, replica=replica, file_metadata=_files, blob_path=blob_path)
        elif FILE_PREFIX in _key:
            uuid, version = parse_key(_key)
            file_metadata = handle.get(replica.bucket, _key)
            blob_path = compose_blob_key(file_metadata)
            sleepy_checkout(file_checkout, replica=replica, file_metadata=file_metadata, blob_path=blob_path)





