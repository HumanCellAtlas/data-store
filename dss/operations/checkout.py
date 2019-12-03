"""
Tools for managing checkout buckets
"""
import sys
import typing
import argparse
import logging
import json
import time
import collections
import multiprocessing
from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from dss import Replica
from dss.operations import dispatch
from dss import Config
from cloud_blobstore import BlobNotFoundError
from dss.api.bundles import get_bundle_manifest
from dss.storage.hcablobstore import compose_blob_key
from dss.storage.identifiers import DSS_BUNDLE_KEY_REGEX, VERSION_REGEX, UUID_REGEX, FILE_PREFIX, TOMBSTONE_SUFFIX
from dss.storage.checkout.bundle import verify_checkout as bundle_checkout
from dss.storage.checkout.file import start_file_checkout
from dss.api.files import _verify_checkout as file_checkout
from dss.storage.checkout import cache_flow

logger = logging.getLogger(__name__)


class CheckoutHandler:
    def __init__(self, argv: typing.List[str], args: argparse.Namespace):
        self.keys = args.keys.copy()
        self.replica = Replica[args.replica]
        self.handle = Config.get_blobstore_handle(self.replica)
        self.checkout_bucket = self.replica.checkout_bucket

    @staticmethod
    def _verify_delete(handle, bucket, key):
        sys.stderr.write(f'Attempting removal of key: {bucket}/{key}')
        try:
            handle.delete(bucket=bucket, key=key)
            handle.get(bucket=bucket, key=key)
        except BlobNotFoundError:
            sys.stderr.write(f'Successfully deleted key {bucket}/{key}')
        else:
            raise RuntimeError(f'Attempted to delete {bucket}/{key} but it did not work ;(')

    @staticmethod
    def _get_metadata(handle, bucket, key):
        """Helper Function to get file_metadata from cloud-blobstore."""
        try:
            file_metadata = json.loads(handle.get(bucket=bucket, key=key).decode('utf-8'))
            return file_metadata
        except BlobNotFoundError:
            sys.stderr.write(f'Unable to locate: {bucket}/{key}')

    @staticmethod
    def _verify_blob_existance(handle, bucket, key):
        """Helper Function to see if blob data exists."""
        try:
            handle.get(bucket=bucket, key=key)
        except BlobNotFoundError:
            return False
        return True

    def _sleepy_checkout(self, function=None, **kwargs):
        status = False
        token = None
        key = kwargs["file_metadata"]['name'] if kwargs.get('file_metadata') else kwargs.get("bundle_uuid")
        kwargs['replica'] = self.replica
        sys.stderr.write(f'Starting {function.__name__} on {key}')
        while status is not True:
            kwargs['token'] = token
            token, status = function(**kwargs)
            if not status:
                time.sleep(4)  # wait 4 seconds for checkout to complete, then try again

    @staticmethod
    def _parse_key(key):
        try:
            version = VERSION_REGEX.search(key).group(0)
            uuid = UUID_REGEX.search(key).group(0)
        except IndexError:
            raise RuntimeError(f'Unable to parse the key: {key}')
        return uuid, version

    def process_keys(self):
        raise NotImplementedError()

    def __call__(self, argv: typing.List[str], args: argparse.Namespace):
        self.process_keys()


checkout = dispatch.target("checkout", help=__doc__)


@checkout.action("remove",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="Keys to remove from checkout.", required=True)})
class Remove(CheckoutHandler):
    def process_keys(self):
        """Remove keys from the checkout bucket."""
        for _key in self.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):
                for key in self.handle.list(self.checkout_bucket, _key):  # handles checkout/bundle/*
                    self._verify_delete(self.handle, self.checkout_bucket, key)
                uuid, version = self._parse_key(_key)
                manifest = get_bundle_manifest(replica=self.replica, uuid=uuid, version=version)
                if manifest is None:
                    sys.stderr.write(f"Unable to locate manifest for: {self.checkout_bucket}/{_key}")
                    continue
                for _files in manifest['files']:
                    key = compose_blob_key(_files)
                    self._verify_delete(self.handle, self.checkout_bucket, key)
            elif _key.startswith(FILE_PREFIX):
                # should handle other keys, files/blobs
                file_metadata = self._get_metadata(self.handle, self.replica.bucket, _key)
                self._verify_delete(self.handle, self.checkout_bucket, key=compose_blob_key(file_metadata))
            else:
                sys.stderr.write(f'Invalid key regex: {_key}')


@checkout.action("verify",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="Keys to check the status of.", required=True)})
class Verify(CheckoutHandler):
    def process_keys(self):
        """Verify that keys are in the checkout bucket."""
        checkout_status = dict(replica=self.replica.name)
        for _key in self.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):  # handles bundles/fqid keys or fqid
                uuid, version = self._parse_key(_key)
                bundle_manifest = get_bundle_manifest(replica=self.replica, uuid=uuid, version=version)
                checkout_bundle_contents = [x[0] for x in self.handle.list_v2(bucket=self.checkout_bucket,
                                                                              prefix=f'bundles/{uuid}.{version}')]
                bundle_internal_status = list()

                for _file in bundle_manifest['files']:
                    temp = collections.defaultdict(blob_checkout=False, bundle_checkout=False, should_be_cached=False)
                    bundle_key = f'bundles/{uuid}.{version}/{_file["name"]}'
                    blob_key = compose_blob_key(_file)

                    blob_status = self._verify_blob_existance(self.handle, self.checkout_bucket, blob_key)
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
            elif _key.startswith(FILE_PREFIX):
                temp = collections.defaultdict(blob_checkout=False, should_be_cached=False)
                file_metadata = self._get_metadata(self.handle, self.replica.bucket, _key)
                if not file_metadata:
                    sys.stderr.write(f'Key not in either main bucket or checkout bucket: {_key}')
                    continue
                blob_key = compose_blob_key(file_metadata)
                blob_status = self._verify_blob_existance(self.handle, self.checkout_bucket, blob_key)
                if blob_status:
                    temp['blob_checkout'] = True
                if cache_flow.should_cache_file(file_metadata['content-type'], file_metadata['size']):
                    temp['should_be_cached'] = True

                for x in ['name', 'uuid', 'version']:
                    temp.update({x: file_metadata[x]})
                checkout_status[_key] = collections.defaultdict(uuid=temp)
            else:
                sys.stderr.write(f'Invalid key regex: {_key}')
        print(json.dumps(checkout_status, sort_keys=True, indent=2))
        return checkout_status  # action_handler does not really use this, its just testing


@checkout.action("add",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="Keys to add to checkout.", required=True)})
class Add(CheckoutHandler):
    """Add keys to the checkout bucket."""
    def process_keys(self):
        for _key in self.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):
                uuid, version = self._parse_key(_key)
                bundle_manifest = get_bundle_manifest(uuid=uuid, replica=self.replica, version=version)
                self._sleepy_checkout(bundle_checkout, bundle_uuid=uuid, bundle_version=version)
                for _files in bundle_manifest['files']:
                    blob_path = compose_blob_key(_files)
                    self._sleepy_checkout(file_checkout, file_metadata=_files, blob_path=blob_path)
            elif _key.startswith(FILE_PREFIX):
                file_metadata = self.handle.get(self.replica.bucket, _key)
                blob_path = compose_blob_key(file_metadata)
                self._sleepy_checkout(file_checkout, file_metadata=file_metadata, blob_path=blob_path)
            else:
                sys.stderr.write(f'Invalid key regex: {_key}')


@checkout.action("sync",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True)})
class Sync(CheckoutHandler):
    """Checkout all files meeting cache criteria."""
    def __init__(self, argv: typing.List[str], args: argparse.Namespace):
        self.keys = []
        self.replica = Replica[args.replica]
        self.handle = Config.get_blobstore_handle(self.replica)
        self.checkout_bucket = self.replica.checkout_bucket

        self.tombstone_cache: Dict[str, bytes] = {}
        self.tombstone_cache_max_len = 100000

    def _is_file_tombstoned(self, key: str):
        if key.endswith(f'.{TOMBSTONE_SUFFIX}'):
            return True

        assert key.startswith(FILE_PREFIX)

        uuid, version = self._parse_key(key)
        if len(self.tombstone_cache) >= self.tombstone_cache_max_len:
            self.tombstone_cache.popitem()

        if uuid not in self.tombstone_cache:
            try:
                self.tombstone_cache[uuid] = self.handle.get(self.replica.bucket,
                                                             key=f'{FILE_PREFIX}/{uuid}.{TOMBSTONE_SUFFIX}')
            except BlobNotFoundError:
                self.tombstone_cache[uuid] = None
        return self.tombstone_cache[uuid]

    def process_keys(self):
        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2) as e:
            for _key in self.handle.list(self.replica.bucket, prefix=f'{FILE_PREFIX}/'):
                e.submit(self.process_key, _key)

    def process_key(self, _key):
        if self._is_file_tombstoned(_key):
            return  # skip if tombstoned

        file_metadata = self._get_metadata(self.handle, self.replica.bucket, _key)
        if not file_metadata:
            return  # skip if missing metadata (edge case where the file was deleted before we got here)

        # check if file meets cache criteria
        if cache_flow.should_cache_file(file_metadata['content-type'], file_metadata['size']):
            blob_key = compose_blob_key(file_metadata)
            checked_out = self._verify_blob_existance(self.handle, self.replica.checkout_bucket, blob_key)
            if not checked_out:
                print(f'Checking out: {_key}')
                start_file_checkout(replica=self.replica, blob_key=blob_key)
                assert self._verify_blob_existance(self.handle, self.replica.checkout_bucket, blob_key)
