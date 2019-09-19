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


class CheckoutHandler:
    def __init__(self, argv: typing.List[str], args: argparse.Namespace):
        if args.keys is not None:
            self.keys = args.keys.copy()
        else:
            self.keys = None
        self.replica = Replica[args.replica]
        self.handle = Config.get_blobstore_handle(self.replica)
        self.bucket = self.replica.checkout_bucket

    def _verify_delete(self, handle, bucket, key):
        logger.warning(f'attempting removal of key: {bucket}/{key}')
        try:
            handle.delete(bucket=bucket, key=key)
            handle.get(bucket=bucket, key=key)
        except BlobNotFoundError:
            logger.warning(f'Successfully deleted key {bucket}/{key}')
        else:
            raise RuntimeError(f'attempted to delete {bucket}/{key} but it did not work ;(')

    def _verify_get(self, handle, bucket, key):
        """ Helper Function to get file_metadata from cloud-blobstore"""
        try:
            file_metadata = json.loads(handle.get(bucket=bucket, key=key).decode('utf-8'))
        except BlobNotFoundError:
            logger.warning(f'unable to locate {bucket}/{key}')
            return None
        return file_metadata

    def _verify_blob_existance(self, handle, bucket, key):
        """Helper Function to see if blob data exists"""
        try:
            handle.get(bucket=bucket, key=key)
        except BlobNotFoundError:
            return False
        return True

    def _sleepy_checkout(self, function=None, **kwargs):
        status = False
        token = None
        key = kwargs["file_metadata"]['name'] if kwargs.get('file_metadata') else kwargs.get("bundle_uuid")
        logger.warning(f'starting {function.__name__} on {key}')
        while status is not True:
            kwargs['token'] = token
            token, status = function(**kwargs)
            if not status:
                time.sleep(4)  # wait 4 seconds for checkout to complete, then try again

    def _parse_key(self, key):
        try:
            version = VERSION_REGEX.search(key).group(0)
            uuid = UUID_REGEX.search(key).group(0)
        except IndexError:
            raise RuntimeError(f'unable to parse the key {key}')
        return uuid, version

    def process_keys(self):
        raise NotImplementedError()

    def __call__(self, argv: typing.List[str], args: argparse.Namespace):
        if self.keys is not None:
            self.process_keys()


checkout = dispatch.target("checkout", help=__doc__)


@checkout.action("remove",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys to remove from checkout", required=True)})
class remove(CheckoutHandler):
    def process_keys(self):
        """
        Remove a bundle from the checkout bucket
        """
        for _key in self.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):
                for key in self.handle.list(self.bucket, _key):  # handles checkout/bundle/*
                    self._verify_delete(self.handle, self.bucket, key)
                uuid, version = self._parse_key(_key)
                manifest = get_bundle_manifest(replica=self.replica, uuid=uuid, version=version)
                if manifest is None:
                    logger.warning(f"unable to locate manifest for fqid: {self.bucket}/{_key}")
                    continue
                for _files in manifest['files']:
                    key = compose_blob_key(_files)
                    self._verify_delete(self.handle, self.bucket, key)
            elif _key.startswith(FILE_PREFIX):
                # should handle other keys, files/blobs
                file_metadata = self._verify_get(self.handle, self.replica.bucket, _key)
                self._verify_delete(self.handle, self.bucket, key=compose_blob_key(file_metadata))


@checkout.action("verify",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys to check the status of", required=True)})
class verify(CheckoutHandler):
    def process_keys(self):
        """
        Verify that keys are in the checkout bucket
        """
        checkout_status = dict(replica=self.replica.name)
        for _key in self.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):  # handles bundles/fqid keys or fqid
                uuid, version = self._parse_key(_key)
                bundle_manifest = get_bundle_manifest(replica=self.replica, uuid=uuid, version=version)
                checkout_bundle_contents = [x[0] for x in self.handle.list_v2(bucket=self.bucket,
                                                                              prefix=f'bundles/{uuid}.{version}')]
                bundle_internal_status = list()

                for _file in bundle_manifest['files']:
                    temp = collections.defaultdict(blob_checkout=False, bundle_checkout=False, should_be_cached=False)
                    bundle_key = f'bundles/{uuid}.{version}/{_file["name"]}'
                    blob_key = compose_blob_key(_file)

                    blob_status = self._verify_blob_existance(self.handle, self.bucket, blob_key)
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
                file_metadata = self._verify_get(self.handle, self.replica.bucket, _key)
                blob_key = compose_blob_key(file_metadata)
                blob_status = self._verify_blob_existance(self.handle, self.bucket, blob_key)
                if blob_status:
                    temp['blob_checkout'] = True
                if cache_flow.should_cache_file(_file['content-type'], _file['size']):
                    temp['should_be_cached'] = True

                for x in ['name', 'uuid', 'version']:
                    temp.update({x: _file[x]})
                checkout_status[_key] = collections.defaultdict(uuid=temp)
        print(json.dumps(checkout_status, sort_keys=True))
        return checkout_status  # action_handler does not really use this, its just testing


@checkout.action("start",
                 arguments={"--replica": dict(choices=[r.name for r in Replica], required=True),
                            "--keys": dict(nargs="+", help="keys to check the status of", required=True)})
class start(CheckoutHandler):
    def process_keys(self):
        for _key in self.keys:
            if DSS_BUNDLE_KEY_REGEX.match(_key):
                uuid, version = self._parse_key(_key)
                bundle_manifest = get_bundle_manifest(uuid=uuid, replica=self.replica, version=version)
                self._sleepy_checkout(bundle_checkout, replica=self.replica, bundle_uuid=uuid, bundle_version=version)
                for _files in bundle_manifest['files']:
                    blob_path = compose_blob_key(_files)
                    self._sleepy_checkout(file_checkout, replica=self.replica,
                                          file_metadata=_files, blob_path=blob_path)
            elif _key.startswith(FILE_PREFIX):
                file_metadata = self.handle.get(self.replica.bucket, _key)
                blob_path = compose_blob_key(file_metadata)
                self._sleepy_checkout(file_checkout, replica=self.replica,
                                      file_metadata=file_metadata, blob_path=blob_path)
