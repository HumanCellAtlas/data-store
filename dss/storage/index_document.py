import ipaddress
import json
import logging
import socket
import typing
import uuid
from abc import abstractmethod, ABCMeta

from collections import defaultdict
from urllib.parse import urlparse

import requests
from cloud_blobstore import BlobStore, BlobStoreError
from elasticsearch.helpers import scan, bulk, BulkIndexError
from requests_http_signature import HTTPSignatureAuth

from dss import Config, DeploymentStage, ESIndexType, ESDocType
from dss import Replica
from dss.hcablobstore import BundleMetadata, BundleFileMetadata
from dss.storage.bundles import ObjectIdentifier, BundleFQID, TombstoneID
from dss.storage.index import IndexManager
from dss.storage.validator import scrub_index_data
from dss.util import create_blob_key
from dss.util.es import ElasticsearchClient, elasticsearch_retry


class IndexDocument(dict, metaclass=ABCMeta):
    """
    An instance of this class represents a document in an Elasticsearch index.
    """

    @abstractmethod
    def index(self, dryrun=False) -> (bool, str):
        """
        Ensure that there is exactly one up-to-date instance of this document in exactly one ES index.

        :param dryrun: if True, only read-only actions will be performed but no ES indices will be modified
        :return: a tuple (modified, index_name) indicating whether an index needed to be updated and what the name of
                 that index is. Note that `modified` may be True even if dryrun is False, indicating that a wet run
                 would have updated the index.
        """
        raise NotImplementedError()

    def __init__(self, replica: Replica, fqid: typing.Union[BundleFQID, TombstoneID], logger: logging.Logger,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.replica = replica
        self.fqid = fqid

    def _write_to_index(self, index_name: str, version: typing.Optional[int]=None):
        """
        Place this document into the given index.

        :param version: if 0, write only if this document is currently absent from the given index
                        if > 0, write only if the specified version of this document is currently present
                        if None, write regardless
        """
        es_client = ElasticsearchClient.get(self.logger)
        body = self.to_json()
        self.logger.debug("%s", f"Writing document to index {index_name}: {body}")
        es_client.index(index=index_name,
                        doc_type=ESDocType.doc.name,
                        id=str(self.fqid),
                        body=body,
                        op_type='create' if version == 0 else 'index',
                        version=version if version else None)

    def to_json(self):
        return json.dumps(self)

    def __eq__(self, other: object) -> bool:
        # noinspection PyUnresolvedReferences
        return self is other or (super().__eq__(other) and
                                 type(self) == type(other) and
                                 self.replica == other.replica and
                                 self.fqid == other.fqid)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(replica={self.replica}, fqid={self.fqid}, {super().__repr__()})"

    @staticmethod
    def _msg(dryrun):
        """
        Returns a unary function that conditionally rewrites a given log message so it makes sense in the context of a
        dry run.

        The message should start with with a verb in -ing form, announcing an action to be taken.
        """
        def msg(s):
            assert s
            assert s[:1].isupper()
            assert s.split(maxsplit=1)[0].endswith('ing')
            return f"Skipped {s[:1].lower() + s[1:]}" if dryrun else s
        return msg


class BundleDocument(IndexDocument):
    """
    An instance of this class represents the Elasticsearch document for a given bundle.
    """

    # Note to implementors, only public methods should have a `dryrun` keyword argument. If they do, the argument
    # should have a default value of False. Protected and private methods may also have a dryrun argument but if they
    # do it must be positional in order to ensure that the argument isn't accidentally dropped along the call chain.

    @classmethod
    def from_replica(cls, replica: Replica, bundle_fqid: BundleFQID, logger):
        self = cls(replica, bundle_fqid, logger)
        blobstore, _, bucket_name = Config.get_cloud_specific_handles(replica)
        self['manifest'] = self._read_bundle_manifest(blobstore, bucket_name, bundle_fqid)
        self['files'] = self._read_file_infos(blobstore, bucket_name)
        self['state'] = 'new'
        return self

    @classmethod
    def from_index(cls, replica: Replica, bundle_fqid: BundleFQID, index_name, logger, version=None):
        es_client = ElasticsearchClient.get(logger)
        source = es_client.get(index_name, str(bundle_fqid), ESDocType.doc.name, version=version)['_source']
        return cls(replica, bundle_fqid, logger, source)

    @property
    def files(self):
        return self['files']

    @property
    def manifest(self):
        return self['manifest']

    @elasticsearch_retry
    def index(self, dryrun=False) -> (bool, str):
        elasticsearch_retry.add_context(bundle=self)
        index_name = self._prepare_index(dryrun)
        return self._index_into(index_name, dryrun)

    def _index_into(self, index_name: str, dryrun: bool):
        elasticsearch_retry.add_context(index=index_name)
        msg = self._msg(dryrun)
        versions = self._get_indexed_versions()
        old_version = versions.pop(index_name, None)
        if versions:
            self.logger.warning("%s", msg(f"Removing stale copies of the bundle document for {self.fqid} "
                                          f"from the following index(es): {json.dumps(versions)}."))
            if not dryrun:
                self._remove_versions(versions)
        if old_version:
            old_doc = self.from_index(self.replica, self.fqid, index_name, self.logger, version=old_version)
            if self == old_doc:
                self.logger.info("%s", f"Document for bundle {self.fqid} is already up-to-date "
                                       f"in index {index_name} at version {old_version}.")
                return False, index_name
            else:
                self.logger.warning("%s", msg(f"Updating an older copy of the document for bundle {self.fqid} "
                                              f"in index {index_name} at version {old_version}."))
        else:
            self.logger.info("%s", msg(f"Writing the document for bundle {self.fqid} "
                                       f"to index {index_name} for the first time."))
        if not dryrun:
            self._write_to_index(index_name, version=old_version or 0)
        return True, index_name

    @elasticsearch_retry
    def entomb(self, tombstone: 'BundleTombstoneDocument', dryrun=False):
        """
        Ensure that there is exactly one up-to-date instance of a tombstone for this document in exactly one
        ES index. The tombstone data overrides the document's data in the index.

        :param tombstone: The document with which to replace this document in the index.
        :param dryrun: see :py:meth:`~IndexDocument.index`
        :return: see :py:meth:`~IndexDocument.index`
        """
        elasticsearch_retry.add_context(bundle=self, tombstone=tombstone)
        self.logger.info("%s", f"Writing tombstone for {self.replica.name} bundle: {self.fqid}")
        # Preare the index using the original data such that the tombstone can be placed in the correct index.
        index_name = self._prepare_index(dryrun)
        # Override document with tombstone JSON …
        self.clear()
        self.update(tombstone)
        # … and place into proper index.
        modified, index_name = self._index_into(index_name, dryrun)
        self.logger.info("%s", f"Finished writing tombstone for {self.replica.name} bundle: {self.fqid}")
        return modified, index_name

    def _write_to_index(self, index_name: str, version: typing.Optional[int]=None):
        es_client = ElasticsearchClient.get(self.logger)
        initial_mappings = es_client.indices.get_mapping(index_name)[index_name]['mappings']
        super()._write_to_index(index_name, version=version)
        current_mappings = es_client.indices.get_mapping(index_name)[index_name]['mappings']
        if initial_mappings != current_mappings:
            self._refresh_percolate_queries(index_name)

    def _read_bundle_manifest(self, handle: BlobStore, bucket_name: str, bundle_fqid: BundleFQID) -> dict:
        manifest_string = handle.get(bucket_name, bundle_fqid.to_key()).decode("utf-8")
        self.logger.debug("%s", f"Read bundle manifest from bucket {bucket_name}"
                                f" with bundle key {bundle_fqid.to_key()}: {manifest_string}")
        manifest = json.loads(manifest_string, encoding="utf-8")
        return manifest

    def _read_file_infos(self, handle: BlobStore, bucket_name: str) -> dict:
        files_info = self.manifest[BundleMetadata.FILES]
        index_files = {}
        for file_info in files_info:
            if file_info[BundleFileMetadata.INDEXED] is True:
                if not file_info[BundleFileMetadata.CONTENT_TYPE].startswith('application/json'):
                    self.logger.warning("%s", f"In bundle {self.fqid} the file '{file_info[BundleFileMetadata.NAME]}'"
                                              f" is marked for indexing yet has content type"
                                              f" '{file_info[BundleFileMetadata.CONTENT_TYPE]}'"
                                              f" instead of the required content type 'application/json'."
                                              f" This file will not be indexed.")
                    continue
                file_blob_key = create_blob_key(file_info)
                try:
                    file_string = handle.get(bucket_name, file_blob_key).decode("utf-8")
                    file_json = json.loads(file_string)
                # TODO (mbaumann) Are there other JSON-related exceptions that should be checked below?
                except json.decoder.JSONDecodeError as ex:
                    self.logger.warning("%s", f"In bundle {self.fqid} the file '{file_info[BundleFileMetadata.NAME]}'"
                                              f" is marked for indexing yet could not be parsed."
                                              f" This file will not be indexed. Exception: {ex}")
                    continue
                except BlobStoreError as ex:
                    self.logger.warning("%s", f"In bundle {self.fqid} the file '{file_info[BundleFileMetadata.NAME]}'"
                                              f" is marked for indexing yet could not be accessed."
                                              f" This file will not be indexed."
                                              f" Exception: {type(ex).__name__}, file blob key: {file_blob_key}")
                    continue
                self.logger.debug("%s", f"Indexing file: {file_info[BundleFileMetadata.NAME]}")
                # There are two reasons in favor of not using dot in the name of the individual
                # files in the index document, and instead replacing it with an underscore.
                # 1. Ambiguity regarding interpretation/processing of dots in field names,
                #    which could potentially change between Elasticsearch versions. For example, see:
                #       https://github.com/elastic/elasticsearch/issues/15951
                # 2. The ES DSL queries are easier to read when there is no ambiguity regarding
                #    dot as a field separator.
                # Therefore, substitute dot for underscore in the key filename portion of the index.
                # As due diligence, additional investigation should be performed.
                index_filename = file_info[BundleFileMetadata.NAME].replace(".", "_")
                index_files[index_filename] = file_json
        scrub_index_data(index_files, str(self.fqid), self.logger)
        return index_files

    def _prepare_index(self, dryrun):
        shape_descriptor = self.get_shape_descriptor()
        index_name = Config.get_es_index_name(ESIndexType.docs, self.replica, shape_descriptor)
        es_client = ElasticsearchClient.get(self.logger)
        if not dryrun:
            IndexManager.create_index(es_client, self.replica, index_name)
        return index_name

    def get_shape_descriptor(self) -> typing.Optional[str]:
        """
        Return a string identifying the shape/structure/format of the data in this bundle
        document, so that it may be indexed appropriately.

        Currently, this returns a string identifying the metadata schema release major number.
        For example:

            v3 - Bundle contains metadata in the version 3 format
            v4 - Bundle contains metadata in the version 4 format
            ...

        This includes verification that schema major number is the same for all index metadata
        files in the bundle, consistent with the current HCA ingest service behavior. If no
        metadata version information is contained in the bundle, the empty string is returned.
        Currently this occurs in the case of the empty bundle used for deployment testing.

        If/when bundle schemas are available, this function should be updated to reflect the
        bundle schema type and major version number.

        Other projects (non-HCA) may manage their metadata schemas (if any) and schema versions.
        This should be an extension point that is customizable by other projects according to
        their metadata.
        """
        schema_version_map = defaultdict(set)  # type: typing.MutableMapping[str, typing.MutableSet[str]]
        for filename, file_content in self.files.items():
            core = file_content.get('core')
            if core is not None:
                schema_type = core['type']
                schema_version = core['schema_version']
                schema_version_major = schema_version.split(".")[0]
                schema_version_map[schema_version_major].add(schema_type)
            else:
                self.logger.info("%s", f"File {filename} does not contain the 'core' section necessary to identify "
                                       f"the schema and its version.")
        if schema_version_map:
            schema_versions = schema_version_map.keys()
            assert len(schema_versions) == 1, \
                "The bundle contains mixed schema major version numbers: {}".format(sorted(list(schema_versions)))
            return "v" + list(schema_versions)[0]
        else:
            return None  # No files with schema identifiers were found

    def _get_indexed_versions(self) -> typing.MutableMapping[str, str]:
        """
        Returns a dictionary mapping the name of each index containing this document to the
        version of this document in that index. Note that `version` denotes document version, not
        bundle version.
        """
        page_size = 64
        es_client = ElasticsearchClient.get(self.logger)
        alias_name = Config.get_es_alias_name(ESIndexType.docs, self.replica)
        response = es_client.search(index=alias_name, body={
            '_source': False,
            'stored_fields': [],
            'version': True,
            'from': 0,
            'size': page_size,
            'query': {
                'terms': {
                    '_id': [str(self.fqid)]
                }
            }
        })
        hits = response['hits']
        assert hits['total'] <= page_size, 'Document is in too many indices'
        indices = {hit['_index']: hit['_version'] for hit in hits['hits']}
        return indices

    def _remove_versions(self, versions: typing.MutableMapping[str, str]):
        """
        Remove this document from each given index provided that it contains the given version of this document.
        """
        es_client = ElasticsearchClient.get(self.logger)
        num_ok, errors = bulk(es_client, raise_on_error=False, actions=[{
            '_op_type': 'delete',
            '_index': index_name,
            '_type': ESDocType.doc.name,
            '_version': version,
            '_id': str(self.fqid),
        } for index_name, version in versions.items()])
        for item in errors:
            self.logger.warning("%s", f"Document deletion failed: {json.dumps(item)}")

    def _refresh_percolate_queries(self, index_name: str):
        # When dynamic templates are used and queries for percolation have been added
        # to an index before the index contains mappings of fields referenced by those queries,
        # the queries must be reloaded when the mappings are present for the queries to match.
        # See: https://github.com/elastic/elasticsearch/issues/5750
        subscription_index_name = Config.get_es_index_name(ESIndexType.subscriptions, self.replica)
        es_client = ElasticsearchClient.get(self.logger)
        if not es_client.indices.exists(subscription_index_name):
            return
        subscription_queries = [{'_index': index_name,
                                 '_type': ESDocType.query.name,
                                 '_id': hit['_id'],
                                 '_source': hit['_source']['es_query']
                                 }
                                for hit in scan(es_client,
                                                index=subscription_index_name,
                                                doc_type=ESDocType.subscription.name,
                                                query={'query': {'match_all': {}}})
                                ]

        if subscription_queries:
            try:
                bulk(es_client, iter(subscription_queries), refresh=True)
            except BulkIndexError as ex:
                self.logger.error("%s", f"Error occurred when adding subscription queries "
                                        f"to index {index_name} Errors: {ex.errors}")

    def notify(self, index_name):
        subscription_ids = self._find_matching_subscriptions(index_name)
        self._notify_subscribers(subscription_ids)

    def _find_matching_subscriptions(self, index_name: str) -> typing.MutableSet[str]:
        percolate_document = {
            'query': {
                'percolate': {
                    'field': "query",
                    'document_type': ESDocType.doc.name,
                    'document': self
                }
            }
        }
        subscription_ids = set()
        for hit in scan(ElasticsearchClient.get(self.logger),
                        index=index_name,
                        query=percolate_document):
            subscription_ids.add(hit["_id"])
        self.logger.debug("%s", f"Found {len(subscription_ids)} matching subscription(s).")
        return subscription_ids

    def _notify_subscribers(self, subscription_ids: typing.MutableSet[str]):
        for subscription_id in subscription_ids:
            try:
                # TODO Batch this request
                subscription = self._get_subscription(subscription_id)
                self._notify_subscriber(subscription)
            except Exception:
                self.logger.error("%s", f"Error occurred while processing subscription {subscription_id} "
                                        f"for bundle {self.fqid}.", exc_info=True)

    def _get_subscription(self, subscription_id: str) -> dict:
        subscription_query = {
            'query': {
                'ids': {
                    'type': ESDocType.subscription.name,
                    'values': [subscription_id]
                }
            }
        }
        response = ElasticsearchClient.get(self.logger).search(
            index=Config.get_es_index_name(ESIndexType.subscriptions, self.replica),
            body=subscription_query)
        hits = response['hits']['hits']
        assert len(hits) == 1
        hit = hits[0]
        assert hit['_id'] == subscription_id
        subscription = hit['_source']
        assert 'id' not in subscription
        subscription['id'] = subscription_id
        return subscription

    def _notify_subscriber(self, subscription: dict):
        subscription_id = subscription['id']
        transaction_id = str(uuid.uuid4())
        payload = {
            "transaction_id": transaction_id,
            "subscription_id": subscription_id,
            "es_query": subscription['es_query'],
            "match": {
                "bundle_uuid": self.fqid.uuid,
                "bundle_version": self.fqid.version,
            }
        }
        callback_url = subscription['callback_url']

        # FIXME wrap all errors in this block with an exception handler
        if DeploymentStage.IS_PROD():
            allowed_schemes = {'https'}
        else:
            allowed_schemes = {'https', 'http'}

        assert urlparse(callback_url).scheme in allowed_schemes, "Unexpected scheme for callback URL"

        if DeploymentStage.IS_PROD():
            hostname = urlparse(callback_url).hostname
            for family, socktype, proto, canonname, sockaddr in socket.getaddrinfo(hostname, port=None):
                msg = "Callback hostname resolves to forbidden network"
                assert ipaddress.ip_address(sockaddr[0]).is_global, msg  # type: ignore

        auth = None
        if "hmac_secret_key" in subscription:
            auth = HTTPSignatureAuth(key=subscription['hmac_secret_key'].encode(),
                                     key_id=subscription.get("hmac_key_id", "hca-dss:" + subscription_id))
        response = requests.post(callback_url, json=payload, auth=auth)

        # TODO (mbaumann) Add webhook retry logic
        if 200 <= response.status_code < 300:
            self.logger.info("%s", f"Successfully notified for subscription {subscription_id}"
                                   f" for bundle {self.fqid} with transaction id {transaction_id} "
                                   f"Code: {response.status_code}")
        else:
            self.logger.warning("%s", f"Failed notification for subscription {subscription_id}"
                                      f" for bundle {self.fqid} with transaction id {transaction_id} "
                                      f"Code: {response.status_code}")


class BundleTombstoneDocument(IndexDocument):

    @classmethod
    def from_replica(cls, replica: Replica, tombstone_id: TombstoneID, logger):
        blobstore, _, bucket_name = Config.get_cloud_specific_handles(replica)
        tombstone_data = json.loads(blobstore.get(bucket_name, tombstone_id.to_key()))
        return cls(replica, tombstone_id, logger, tombstone_data)

    def _list_dead_bundles(self) -> typing.Sequence[BundleDocument]:
        blobstore, _, bucket_name = Config.get_cloud_specific_handles(self.replica)

        if self.fqid.is_fully_qualified():
            # if a version is specified, delete just that version
            bundle_fqids = [self.fqid.to_bundle_fqid()]
        else:
            # if no version is specified, delete all bundle versions from the index
            prefix = self.fqid.to_key_prefix()
            fqids = [ObjectIdentifier.from_key(k) for k in set(blobstore.list(bucket_name, prefix))]
            bundle_fqids = filter(lambda fqid: type(fqid) == BundleFQID, fqids)

        docs = [BundleDocument.from_replica(self.replica, bundle_fqid, self.logger) for bundle_fqid in bundle_fqids]
        return docs

    @elasticsearch_retry
    def index(self, dryrun=False) -> (bool, str):
        elasticsearch_retry.add_context(tombstone=self)
        dead_docs = self._list_dead_bundles()
        for doc in dead_docs:
            doc.entomb(self, dryrun=dryrun)
