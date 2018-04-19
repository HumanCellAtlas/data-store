#!/usr/bin/env python
# coding: utf-8

from abc import ABCMeta, abstractmethod
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import io
import json
import logging
import os
import requests
from requests_http_signature import HTTPSignatureAuth
import sys
import threading
import time
import typing
import unittest
import unittest.mock
import uuid

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import BucketConfig, Config, DeploymentStage
from dss.config import Replica
from dss.index import DEFAULT_BACKENDS
from dss.index.backend import CompositeIndexBackend
from dss.index.es import ElasticsearchClient
from dss.index.es.document import BundleDocument
from dss.index.es.validator import scrub_index_data
from dss.index.indexer import Indexer
from dss.logging import configure_test_logging
from dss.storage.hcablobstore import BundleFileMetadata, BundleMetadata, FileMetadata
from dss.storage.identifiers import BundleFQID, ObjectIdentifier
from dss.util import UrlBuilder, create_blob_key, networking, RequirementError
from dss.util.version import datetime_to_version_format
from tests import eventually, get_auth_header, get_bundle_fqid, get_file_fqid, get_version
from tests.infra import DSSAssertMixin, DSSStorageMixin, DSSUploadMixin, TestBundle, testmode, MockLambdaContext
from tests.infra.elasticsearch_test_case import ElasticsearchTestCase
from tests.infra.server import ThreadedLocalServer
from tests.sample_search_queries import (smartseq2_paired_ends_v2_or_v3_query, smartseq2_paired_ends_v3_or_v4_query,
                                         smartseq2_paired_ends_v3_query, smartseq2_paired_ends_v4_query)

logger = logging.getLogger(__name__)

# The moto mock has two defects that show up when used by the dss core storage system.
# Use actual S3 until these defects are fixed in moto.
# TODO (mbaumann) When the defects in moto have been fixed, remove True from the line below.
USE_AWS_S3 = bool(os.environ.get("USE_AWS_S3", True))


#
# Basic test for DSS indexer:
#   1. Populate S3 bucket with data for a bundle as defined
#      in the HCA Storage System Disk Format specification
#   2. Inject a mock S3 event into function used by the indexing AWS Lambda
#   3. Read and process the bundle manifest to produce an index as
#      defined in HCA Storage System Index, Query, and Eventing Functional Spec & Use Cases
#      The index document is then added to Elasticsearch
#   4. Perform a search to verify the bundle index document is in Elasticsearch.
#   5. Verify the structure and content of the index document
#


class HTTPInfo:
    address = "127.0.0.1"
    port = None
    server = None
    thread = None


def setUpModule():
    configure_test_logging()
    HTTPInfo.port = networking.unused_tcp_port()
    HTTPInfo.server = HTTPServer((HTTPInfo.address, HTTPInfo.port), PostTestHandler)
    HTTPInfo.thread = threading.Thread(target=HTTPInfo.server.serve_forever)
    HTTPInfo.thread.start()


def tearDownModule():
    HTTPInfo.server.shutdown()


@testmode.standalone
class TestIndexerBase(ElasticsearchTestCase, DSSAssertMixin, DSSStorageMixin, DSSUploadMixin, metaclass=ABCMeta):
    bundle_key_by_replica = dict()  # type: typing.MutableMapping[str, str]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.app = ThreadedLocalServer()
        cls.app.start()
        Config.set_config(BucketConfig.TEST_FIXTURE)
        cls.blobstore = Config.get_blobstore_handle(cls.replica)
        cls.test_fixture_bucket = cls.replica.bucket
        Config.set_config(BucketConfig.TEST)
        cls.test_bucket = cls.replica.bucket
        cls.indexer_cls = Indexer.for_replica(cls.replica)
        cls.executor = ThreadPoolExecutor(len(DEFAULT_BACKENDS))

    @classmethod
    def tearDownClass(cls):
        cls.executor.shutdown(False)
        cls.app.shutdown()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        backend = CompositeIndexBackend(self.executor, DEFAULT_BACKENDS, context=MockLambdaContext())
        self.indexer = self.indexer_cls(backend)
        self.dss_alias_name = dss.Config.get_es_alias_name(dss.ESIndexType.docs, self.replica)
        self.subscription_index_name = dss.Config.get_es_index_name(dss.ESIndexType.subscriptions, self.replica)
        if self.replica not in self.bundle_key_by_replica:
            self.bundle_key_by_replica[self.replica] = self.load_test_data_bundle_for_path(
                "fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        self.bundle_key = self.bundle_key_by_replica[self.replica]
        self.smartseq2_paired_ends_query = smartseq2_paired_ends_v2_or_v3_query
        PostTestHandler.reset()

    def process_new_indexable_object(self, event):
        self.indexer.process_new_indexable_object(event)

    def test_create(self):
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(
            search_results[0],
            self.bundle_key,
            files=smartseq2_paried_ends_indexed_file_list,
        )

    def test_delete(self):
        self._test_delete(all_versions=False, zombie=False)

    def test_delete_all_versions(self):
        self._test_delete(all_versions=True, zombie=False)

    def test_delete_zombie(self):
        self._test_delete(all_versions=False, zombie=True)

    def test_delete_all_versions_zombie(self):
        self._test_delete(all_versions=True, zombie=True)

    def _test_delete(self, all_versions=False, zombie=False):
        bundle_fqid = BundleFQID.from_key(self.bundle_key)
        tombstone_id = bundle_fqid.to_tombstone_id(all_versions=all_versions)
        if zombie:
            tombstone_data = self._create_tombstone(tombstone_id)
            self._create_tombstoned_bundle(retry_method='get')
        else:
            self._create_tombstoned_bundle()
            tombstone_data = self._create_tombstone(tombstone_id)
        self._assert_tombstone(tombstone_id, tombstone_data)

    def _create_tombstoned_bundle(self, retry_method='index'):
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self._process_new_indexable_object_with_retry(sample_event, retry_method=retry_method)
        self.get_search_results(self.smartseq2_paired_ends_query, 1)

    def _create_tombstone(self, tombstone_id):
        blobstore = Config.get_blobstore_handle(self.replica)
        bucket = self.replica.bucket
        tombstone_data = {"status": "disappeared"}
        tombstone_data_bytes = io.BytesIO(json.dumps(tombstone_data).encode('utf-8'))
        # noinspection PyTypeChecker
        blobstore.upload_file_handle(bucket, tombstone_id.to_key(), tombstone_data_bytes)
        # Without this, the tombstone would break subsequent tests, due to the caching added in e12a5f7:
        self.addCleanup(self._delete_tombstone, tombstone_id)
        sample_event = self.create_bundle_deleted_event(tombstone_id.to_key())
        self._process_new_indexable_object_with_retry(sample_event)
        return tombstone_data

    def _process_new_indexable_object_with_retry(self, sample_event, retry_method='index'):
        # Remove cached ES client instances (patching the class wouldn't affect them)
        ElasticsearchClient._es_client = dict()
        # Patch client's index() method to raise exception once
        from elasticsearch.client import Elasticsearch
        real_method = getattr(Elasticsearch, retry_method)
        from elasticsearch.exceptions import ConflictError
        i = iter([ConflictError(409)])

        def mock_method(*args, **kwargs):
            try:
                e = next(i)
            except StopIteration:
                return real_method(*args, **kwargs)
            else:
                raise e

        with unittest.mock.patch.object(Elasticsearch, retry_method, mock_method):
            with unittest.mock.patch.object(dss.index.es.backend.logger, 'warning') as mock_warning:
                # call method under test with patches in place
                self.process_new_indexable_object(sample_event)

        mock_warning.assert_called_with('An exception occurred in %r.',
                                        unittest.mock.ANY, exc_info=True, stack_info=True)

    def _delete_tombstone(self, tombstone_id):
        blobstore = Config.get_blobstore_handle(self.replica)
        bucket = self.replica.bucket
        blobstore.delete(bucket, tombstone_id.to_key())

    @eventually(5.0, 0.5)
    def _assert_tombstone(self, tombstone_id, tombstone_data):
        blobstore = Config.get_blobstore_handle(self.replica)
        bucket = self.replica.bucket
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 0)
        self.assertEqual(0, len(search_results))
        bundle_fqids = [ObjectIdentifier.from_key(k) for k in blobstore.list(bucket, tombstone_id.to_key_prefix())]
        bundle_fqids = filter(lambda bundle_id: type(bundle_id) == BundleFQID, bundle_fqids)
        for bundle_fqid in bundle_fqids:
            exact_query = {
                "query": {
                    "terms": {
                        "_id": [str(bundle_fqid)]
                    }
                }
            }
            search_results = self.get_search_results(exact_query, 1)
            self.assertEqual(1, len(search_results))
            expected_search_result = dict(tombstone_data, uuid=tombstone_id.uuid)
            self.assertDictEqual(search_results[0], expected_search_result)

    def test_reindexing_with_changed_content(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)

        @eventually(timeout=5.0, interval=0.5)
        def _assert_reindexing_results(expect_extra_field, expected_version):
            query = {**self.smartseq2_paired_ends_query, 'version': True}
            hits = self.get_raw_search_results(query, 1)['hits']['hits']
            self.assertEqual(1, len(hits))
            self.assertEquals(expected_version, hits[0]['_version'])
            doc = hits[0]['_source']
            if expect_extra_field:
                self.assertEqual(42, doc['potato'])
            else:
                self.assertFalse('potato' in doc)

        to_json = BundleDocument.to_json

        def mock_to_json(doc: BundleDocument):
            # BundleDocument is a dict
            with unittest.mock.patch.dict(doc, potato=42):  # type: ignore
                return to_json(doc)

        # Index bundle, patching in an extra field just before document is written to index
        with unittest.mock.patch.object(BundleDocument, 'to_json', mock_to_json):
            self.process_new_indexable_object(sample_event)
        _assert_reindexing_results(expect_extra_field=True, expected_version=1)

        # Index again without the patch …
        with self.assertLogs(dss.logger, level="WARNING") as log:
            self.process_new_indexable_object(sample_event)
        self.assertTrue(any('Updating an older copy' in e for e in log.output))
        _assert_reindexing_results(expect_extra_field=False, expected_version=2)

        # … and again which should not write the document
        with self.assertLogs(dss.logger, level="INFO") as log:
            self.process_new_indexable_object(sample_event)
        self.assertTrue(any('is already up-to-date' in e for e in log.output))

    def test_reindexing_with_changed_shape(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        shape_descriptor = 'v99'

        @eventually(timeout=5.0, interval=0.5)
        def _assert_reindexing_results(expect_shape_descriptor):
            hits = self.get_raw_search_results(self.smartseq2_paired_ends_query, 1)['hits']['hits']
            self.assertEqual(1, len(hits))
            self.assertEqual(expect_shape_descriptor, shape_descriptor in hits[0]['_index'])

        # Index document into the "wrong" index by patching the shape descriptor
        with unittest.mock.patch.object(BundleDocument, 'get_shape_descriptor', return_value=shape_descriptor):
            self.process_new_indexable_object(sample_event)
        # There should only be one hit and it should be from the "wrong" index
        _assert_reindexing_results(expect_shape_descriptor=True)
        # Index again, this time into the correct index
        with self.assertLogs(dss.logger, level="WARNING") as log:
            self.process_new_indexable_object(sample_event)
        self.assertTrue(any('Removing stale copies' in e for e in log.output))
        # There should only be one hit and it should be from a different index, the "right" one
        _assert_reindexing_results(expect_shape_descriptor=False)

    def test_indexed_file_with_invalid_content_type(self):
        bundle = TestBundle(self.blobstore, "fixtures/indexing/bundles/v3/smartseq2/paired_ends",
                            self.test_fixture_bucket, self.replica)
        # Configure a file to be indexed that is not of context type 'application/json'
        for file in bundle.files:
            if file.name == "text_data_file1.txt":
                file.indexed = True
        bundle_key = self.load_test_data_bundle(bundle)
        sample_event = self.create_bundle_created_event(bundle_key)
        with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file 'text_data_file1.txt' is marked for indexing"
                         " yet has content type 'text/plain' instead of the required"
                         " content type 'application/json'. This file will not be indexed.")
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_key_is_not_indexed_when_processing_an_event_with_a_file_key(self):
        file_fqid = get_file_fqid()
        sample_event = self.create_bundle_created_event(file_fqid.to_key())
        with self.assertLogs(dss.logger, level="DEBUG") as log_monitor:
            self.process_new_indexable_object(sample_event)
        self.assertIn('Indexing of individual files is not supported.', log_monitor.output[0])
        self.assertIn(str(file_fqid), log_monitor.output[0])
        self.assertFalse(ElasticsearchClient.get().indices.exists_alias(name=self.dss_alias_name))

    def test_error_message_logged_when_invalid_bucket_in_event(self):
        bundle_key = "bundles/{}.{}".format(str(uuid.uuid4()), get_version())
        sample_event = self.create_bundle_created_event(bundle_key)
        with self.assertLogs(dss.logger, level="ERROR") as log_monitor:
            with self.assertRaises(Exception):
                self.process_new_indexable_object(sample_event)
        self.assertRegex(log_monitor.output[0], "ERROR:.*Exception occurred while processing .* event:.*")

    def test_indexed_file_unparsable(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/indexing/bundles/unparseable_indexed_file")
        sample_event = self.create_bundle_created_event(bundle_key)
        with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file 'unparseable_json.json' is marked for indexing"
                         " yet could not be parsed. This file will not be indexed. Exception:")
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_access_error(self):
        inaccesssible_filename = "inaccessible_file.json"
        bundle_key = self.load_test_data_bundle_with_inaccessible_file(
            "fixtures/indexing/bundles/v3/smartseq2/paired_ends", inaccesssible_filename, "application/json", True)
        sample_event = self.create_bundle_created_event(bundle_key)
        with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
            with self.assertRaises(RuntimeError):
                self.process_new_indexable_object(sample_event)
        self.assertRegex(
            log_monitor.output[0],
            f"WARNING:.*:In bundle .* the file '{inaccesssible_filename}' is marked for indexing yet could not be "
            f"accessed. Retrying.")
        self.assertRegex(
            log_monitor.output[-1],
            f".* This bundle will not be indexed. Bundle: .*, File Blob Key: .*, File Name: '{inaccesssible_filename}'")

    def test_not_enough_time_to_index(self):
        backend = CompositeIndexBackend(self.executor, DEFAULT_BACKENDS, context=MockLambdaContext(0))
        self.indexer = self.indexer_cls(backend)
        sample_event = self.create_bundle_created_event(self.bundle_key)
        with self.assertRaises(RuntimeError):
            self.process_new_indexable_object(sample_event)

    def test_notify(self):
        from dss.index import ElasticsearchIndexBackend
        backend = ElasticsearchIndexBackend(context=MockLambdaContext())

        def _notify(subscription, bundle_id=get_bundle_fqid()):
            document = BundleDocument(self.replica, bundle_id)
            backend._notify_subscriber(document, subscription=subscription)

        with self.assertRaisesRegex(requests.exceptions.InvalidURL, "Invalid URL 'http://': No host supplied"):
            _notify(subscription=dict(id="", es_query={}, callback_url="http://"))
        with self.assertRaisesRegex(RequirementError, "The scheme '' of URL '' is prohibited"):
            _notify(subscription=dict(id="", es_query={}, callback_url=""))
        with self.assertRaisesRegex(RequirementError, "The scheme 'wss' of URL 'wss://127.0.0.1' is prohibited"):
            _notify(subscription=dict(id="", es_query={}, callback_url="wss://127.0.0.1"))
        with unittest.mock.patch.dict(os.environ, DSS_DEPLOYMENT_STAGE=DeploymentStage.PROD.value):
            with self.assertRaisesRegex(RequirementError,
                                        "The scheme 'http' of URL 'http://example.com' is prohibited"):
                _notify(subscription=dict(id="", es_query={}, callback_url="http://example.com"))
            with self.assertRaisesRegex(RequirementError,
                                        "The hostname in URL 'https://127.0.0.1' resolves to a private IP"):
                _notify(subscription=dict(id="", es_query={}, callback_url="https://127.0.0.1"))

    def delete_subscription(self, subscription_id):
        self.assertDeleteResponse(
            str(UrlBuilder().set(path=f"/v1/subscriptions/{subscription_id}").add_query("replica", self.replica.name)),
            requests.codes.ok,
            headers=get_auth_header())

    def test_subscription_notification_successful(self):
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)
        for verify_payloads, subscribe_kwargs in ((True, dict(hmac_secret_key=PostTestHandler.hmac_secret_key)),
                                                  (False, dict())):
            PostTestHandler.verify_payloads = verify_payloads
            subscription_id = self.subscribe_for_notification(self.smartseq2_paired_ends_query,
                                                              f"http://{HTTPInfo.address}:{HTTPInfo.port}",
                                                              **subscribe_kwargs)

            sample_event = self.create_bundle_created_event(self.bundle_key)
            self.process_new_indexable_object(sample_event)
            prefix, _, bundle_fqid = self.bundle_key.partition("/")
            self.verify_notification(subscription_id, self.smartseq2_paired_ends_query, bundle_fqid)
            self.delete_subscription(subscription_id)
            PostTestHandler.reset()

    def test_subscription_notification_unsuccessful(self):
        PostTestHandler.verify_payloads = True
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)

        url = f"http://{HTTPInfo.address}:{HTTPInfo.port}/"
        subscription_id = self.subscribe_for_notification(self.smartseq2_paired_ends_query,
                                                          callback_url=url,
                                                          hmac_secret_key=PostTestHandler.hmac_secret_key,
                                                          hmac_key_id="test")

        bundle_key = self.load_test_data_bundle_for_path("fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        PostTestHandler.set_response_code(500)
        with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event)
        prefix, _, bundle_fqid = bundle_key.partition("/")
        self.assertRegex(log_monitor.output[0],
                         f"(?s)"  # dot matches newline
                         f"ERROR:.*:"
                         f"Error occurred while processing subscription {subscription_id} for bundle {bundle_fqid}.*"
                         f"requests.exceptions.HTTPError: 500 Server Error: Internal Server Error for url: {url}")

    def test_subscription_registration_before_indexing(self):
        subscription_id = self.subscribe_for_notification(self.smartseq2_paired_ends_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")
        sample_event = self.create_bundle_created_event(self.bundle_key)
        PostTestHandler.verify_payloads = False
        self.process_new_indexable_object(sample_event)
        prefix, _, bundle_fqid = self.bundle_key.partition("/")
        self.verify_notification(subscription_id, self.smartseq2_paired_ends_query, bundle_fqid)
        self.delete_subscription(subscription_id)

    def test_subscription_query_with_multiple_data_types_indexing_and_notification(self):
        # Verify that a subscription query using numeric, date and string types
        # that is registered before indexing (via the ES setting
        # index.percolator.map_unmapped_fields_as_string=True) works correctly
        # when a document is subsequently indexed.
        subscription_query = \
            {
                'query': {
                    'bool': {
                        'must': [{
                            'match': {
                                "files.sample_json.donor.age": 12
                            }
                        }, {
                            'range': {
                                "files.sample_json.submit_date": {
                                    "gte": "2015-11-30",
                                    "lte": "2015-11-30"
                                }
                            }
                        }, {
                            'match': {
                                "files.sample_json.ncbi_biosample": "SAMN04303778"
                            }
                        }]
                    }
                }
            }

        subscription_id = self.subscribe_for_notification(subscription_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")
        sample_event = self.create_bundle_created_event(self.bundle_key)
        PostTestHandler.verify_payloads = False
        self.process_new_indexable_object(sample_event)

        # Verify the mapping types are as expected for a valid test
        doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica, "v3")
        mappings = ElasticsearchClient.get().indices.get_mapping(doc_index_name)[doc_index_name]['mappings']
        sample_json_mappings = mappings['doc']['properties']['files']['properties']['sample_json']
        self.assertEquals(sample_json_mappings['properties']['donor']['properties']['age']['type'], "long")
        self.assertEquals(sample_json_mappings['properties']['submit_date']['type'], "date")
        self.assertEquals(sample_json_mappings['properties']['ncbi_biosample']['type'], "keyword")

        # Verify the query works correctly as a search
        search_results = self.get_search_results(subscription_query, 1)
        self.assertEqual(1, len(search_results))

        # Verify the query works correctly as a subscription, resulting in notification
        prefix, _, bundle_fqid = self.bundle_key.partition("/")
        self.verify_notification(subscription_id, subscription_query, bundle_fqid)
        self.delete_subscription(subscription_id)

    def test_get_shape_descriptor(self):

        def describedBy(schema_type, schema_version):
            return {
                'describedBy': f"http://schema.humancellatlas.org/module/{schema_version}.0.0/{schema_type}.json"
            }

        def core(schema_type, schema_version):
            return {
                'core': {
                    'schema_url': f"http://hgwdev.soe.ucsc.edu/~kent/hca/schema/{schema_type}.json",
                    'schema_version': schema_version + ".0.0",
                    'type': schema_type
                }
            }

        for (version, other_version, content) in (('5', '4', describedBy), ('4', '3', core)):
            with self.subTest(version=version, previous_version=other_version, content=content.__name__):
                index_document = BundleDocument(self.replica, get_bundle_fqid())

                with self.subTest(f"Consistent schema version {version}"):
                    index_document['files'] = {
                        'assay_json': content('assay', version),
                        'sample_json': content('sample', version)
                    }
                    self.assertEqual(index_document.get_shape_descriptor(), "v" + version)

                with self.subTest("Mixed metadata schema versions"):
                    index_document['files'] = {
                        'assay_json': content('assay', other_version),
                        'sample_json': content('sample', version)
                    }
                    self.assertEqual(index_document.get_shape_descriptor(), f"v.assay.{other_version}.sample.{version}")

                with self.subTest(f"Consistent schema version {other_version}"):
                    index_document['files'] = {
                        'assay_json': content('assay', other_version),
                        'sample_json': content('sample', other_version)
                    }
                    self.assertEqual(index_document.get_shape_descriptor(), "v" + other_version)

                with self.subTest("One version present, one absent"):
                    index_document['files'] = {
                        'assay_json': {},
                        'sample_json': content('sample', other_version)
                    }
                    with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
                        index_document.get_shape_descriptor()
                    self.assertRegexIn(r"WARNING:.*Unable to obtain JSON schema info from file 'assay_json'. The file "
                                       r"will be indexed as is, without sanitization. This may prevent subsequent, "
                                       r"valid files from being indexed correctly.", log_monitor.output)
                    self.assertEqual(index_document.get_shape_descriptor(), "v" + other_version)

                with self.subTest("No versions"):
                    index_document['files'] = {
                        'assay_json': {},
                        'sample_json': {}
                    }
                    self.assertEqual(index_document.get_shape_descriptor(), None)

                with self.subTest("Mixed versions for one type"):
                    index_document['files'] = {
                        'sample1_json': content('sample', version),
                        'sample2_json': content('sample', other_version)
                    }
                    self.assertEqual(index_document.get_shape_descriptor(),
                                     f"v.sample1.sample.{version}.sample2.sample.{other_version}")

                def test_requirements(msg, file_name, schema, expected_error):
                    with self.subTest(msg):
                        index_document['files'] = {
                            file_name: content(schema, version)
                        }
                        with self.assertRaises(RequirementError) as context:
                            index_document.get_shape_descriptor()
                        self.assertEqual(context.exception.args[0], expected_error)

                test_requirements('Dot in file name', 'sample.json', 'sample',
                                  "A metadata file name must not contain '.' characters: sample.json")
                test_requirements('Dot in type', 'sample_json', 'sam.ple',
                                  "A schema name must not contain '.' characters: sam.ple")
                test_requirements('Type is number', 'sample_json', '123',
                                  "A schema name must contain at least one non-digit: 123")
                test_requirements('File name is number', '7_json', 'sample',
                                  "A metadata file name must contain at least one non-digit: 7")

    def test_alias_and_versioned_index_exists(self):
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)
        es_client = ElasticsearchClient.get()
        self.assertTrue(es_client.indices.exists_alias(name=[self.dss_alias_name]))
        alias = es_client.indices.get_alias(name=[self.dss_alias_name])
        doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica, "v3")
        self.assertIn(doc_index_name, alias)
        self.assertTrue(es_client.indices.exists(index=doc_index_name))

    def test_alias_and_multiple_schema_version_index_exists(self):
        # Load and test an unversioned bundle
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/unversioned/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event)
        es_client = ElasticsearchClient.get()
        alias = es_client.indices.get_alias(name=[self.dss_alias_name])
        unversioned_doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica, None)
        self.assertIn(unversioned_doc_index_name, alias)
        self.assertTrue(es_client.indices.exists(index=unversioned_doc_index_name))

        # Load and test a v3 bundle
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)
        self.assertTrue(es_client.indices.exists_alias(name=[self.dss_alias_name]))
        alias = es_client.indices.get_alias(name=[self.dss_alias_name])
        doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, self.replica, "v3")
        # Ensure the alias references both indices
        self.assertIn(unversioned_doc_index_name, alias)
        self.assertIn(doc_index_name, alias)
        self.assertTrue(es_client.indices.exists(index=doc_index_name))

    def test_multiple_schema_version_indexing_and_search(self):
        # Load a schema version 4 bundle
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/v4/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event)

        # Search using a v4-specific query - should match
        search_results = self.get_search_results(smartseq2_paired_ends_v4_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)
        # Search using a query that works for v2 or v3 - should match
        search_results = self.get_search_results(smartseq2_paired_ends_v3_or_v4_query, 1)
        self.assertEqual(1, len(search_results))

        # Search using a v3-specific query - should not match
        search_results = self.get_search_results(smartseq2_paired_ends_v3_query, 0)
        self.assertEqual(0, len(search_results))

        # Load a v3 bundle
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)

        # Search using a v3-specific query - should match
        search_results = self.get_search_results(smartseq2_paired_ends_v3_query, 1)
        self.assertEqual(1, len(search_results))

        # Search using a query that works for v3 or v4 - should match both v3 and v4 bundles
        search_results = self.get_search_results(smartseq2_paired_ends_v3_or_v4_query, 2)
        self.assertEqual(2, len(search_results))

    def test_multiple_schema_version_subscription_indexing_and_notification(self):
        PostTestHandler.verify_payloads = False

        # Load a schema version 4 bundle
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/v4/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event)

        # Load a v3 bundle
        sample_event = self.create_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event)

        subscription_id = self.subscribe_for_notification(smartseq2_paired_ends_v3_or_v4_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")

        # Load another schema version 4 bundle and verify notification
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/v4/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event)
        prefix, _, bundle_fqid = bundle_key.partition("/")
        self.verify_notification(subscription_id, smartseq2_paired_ends_v3_or_v4_query, bundle_fqid)

        PostTestHandler.reset()
        PostTestHandler.verify_payloads = False

        # Load another schema version 3 bundle and verify notification
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        sample_event = self.create_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event)
        prefix, _, bundle_fqid = bundle_key.partition("/")
        self.verify_notification(subscription_id, smartseq2_paired_ends_v3_or_v4_query, bundle_fqid)

        self.delete_subscription(subscription_id)

    # TODO: Remove this test once we stop supporting the `core` property (see issue #1015).
    def test_scrub_index_data_with_core(self):
        manifest = read_bundle_manifest(self.blobstore, self.test_bucket, self.bundle_key)
        doc = 'assay_json'
        with self.subTest("removes extra fields when fields not specified in schema."):
            index_data = create_index_data(self.blobstore, self.test_bucket, self.bundle_key, manifest)
            index_data['files']['assay_json'].update({'extra_top': 123,
                                                      'extra_obj': {"something": "here", "another": 123},
                                                      'extra_lst': ["a", "b"]})
            index_data['files']['assay_json']['core']['extra_internal'] = 123
            index_data['files']['sample_json']['extra_0'] = "tests patterned properties."
            index_data['files']['project_json']['extra_1'] = "Another extra field in a different file."
            index_data['files']['project_json']['extra_2'] = "Another extra field in a different file."
            index_data['files']['project_json']['core']['characteristics_3'] = "patternProperties only apply to root."
            bundle_fqid = self.bundle_key.split('/')[1]

            with self.assertLogs(dss.logger, level="INFO") as log_monitor:
                scrub_index_data(index_data['files'], bundle_fqid)
            self.assertRegexIn(r"INFO:[^:]+:In [\w\-\.]+, unexpected additional fields have been removed from the "
                               r"data to be indexed. Removed \[[^\]]*].", log_monitor.output)
            self.verify_index_document_structure_and_content(
                index_data,
                self.bundle_key,
                files=smartseq2_paried_ends_indexed_file_list,
            )

        with self.subTest("document is removed from meta data when an invalid url is in core.schema_url."):
            invalid_url = "://invalid_url"
            index_data = create_index_data(self.blobstore, self.test_bucket, self.bundle_key, manifest)
            index_data['files'][doc]['core']['schema_url'] = invalid_url
            with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
                scrub_index_data(index_data['files'], bundle_fqid)
            self.assertRegexIn(f"WARNING:[^:]+:Unable to retrieve JSON schema information from {doc} in bundle "
                               f"{bundle_fqid}.*",
                               log_monitor.output)
            self.verify_index_document_structure_and_content(
                index_data,
                self.bundle_key,
                files=smartseq2_paried_ends_indexed_file_list,
                excluded_files=[doc]
            )

        with self.subTest("exception is raised when document is missing core.schema_url field."):
            index_data = create_index_data(self.blobstore, self.test_bucket, self.bundle_key, manifest)
            index_data['files'][doc]['core'].pop('schema_url')
            with self.assertRaises(KeyError):
                scrub_index_data(index_data['files'], bundle_fqid)

        with self.subTest("document is removed from meta data when document is missing core field."):
            'Only the manifest should exist.'
            bundle_key = self.load_test_data_bundle_for_path(
                "fixtures/indexing/bundles/unversioned/smartseq2/paired_ends_extras")
            bundle_fqid = bundle_key.split('/')[1]
            manifest = read_bundle_manifest(self.blobstore, self.test_bucket, bundle_key)
            index_data = create_index_data(self.blobstore, self.test_bucket, bundle_key, manifest)
            for file in index_data['files']:
                file.pop('core', None)
            scrub_index_data(index_data['files'], bundle_fqid)

            self.assertEqual(4, len(index_data.keys()))
            self.assertEqual("new", index_data['state'])
            self.assertIsNotNone(index_data['manifest'])
            self.assertEqual(index_data['files'], {})

            expected_index_data = generate_expected_index_document(self.blobstore,
                                                                   self.test_bucket,
                                                                   bundle_key,
                                                                   smartseq2_paried_ends_indexed_file_list)
            self.assertDictEqual(expected_index_data, index_data, msg=f"Expected index document: "
                                                                      f"{json.dumps(expected_index_data, indent=4)}"
                                                                      f"Actual index document: "
                                                                      f"{json.dumps(index_data, indent=4)}")

    def test_scrub_index_data_with_describedBy(self):
        index_data_master = {
            "files": {
                "biomaterial_bundle_json": {
                    "describedBy": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/"
                                   "5.0.0/json_schema/bundle/biomaterial.json",
                    "schema_version": "5.0.0",
                    "schema_type": "biomaterial_bundle",
                    "biomaterials": [
                        {
                            "hca_ingest": {
                                "describedBy": "https://schema.humancellatlas.org/bundle/5.0.0/ingest_audit",
                                "document_id": "a37dbd93-b93a-4838-a7bb-cd0796b3d9d0",
                                "submissionDate": "2017-10-13T09:18:49.422Z",
                                "updateDate": "2017-10-13T09:19:40.069Z",
                                "accession": "SAM0888697"
                            },
                            "content": {
                                "describedBy": "https://schema.humancellatlas.org/type/biomaterial/"
                                               "5.0.0/donor_organism",
                                "schema_version": "5.0.0",
                                "schema_type": "biomaterial",
                                "biomaterial_core": {
                                    "describedBy": "https://schema.humancellatlas.org/core/biomaterial/"
                                                   "5.0.0/biomaterial_core",
                                    "biomaterial_id": "d1",
                                    "biomaterial_name": "donor1",
                                    "ncbi_taxon_id": [
                                        9606
                                    ]
                                },
                                "is_living": True,
                                "biological_sex": "male",
                                "development_stage": {
                                    "describedBy":
                                        "https://schema.humancellatlas.org/module/ontology/"
                                        "5.0.0/development_stage_ontology",
                                    "text": "adult",
                                    "ontology": "EFO_0001272"
                                },
                                "genus_species": [
                                    {
                                        "describedBy": "https://schema.humancellatlas.org/module/ontology/"
                                                       "5.0.0/species_ontology",
                                        "text": "Homo sapiens",
                                        "ontology": "NCBITAXON_9606"
                                    }
                                ]
                            }
                        },
                        {
                            "hca_ingest": {
                                "describedBy": "https://schema.humancellatlas.org/bundle/5.0.0/ingest_audit",
                                "document_id": "a37dbd93-b93a-4838-a7bb-cd0796b3d9d1",
                                "submissionDate": "2017-10-13T09:18:49.422Z",
                                "updateDate": "2017-10-13T09:19:40.069Z",
                                "accession": "SAM0999697"
                            },
                            "content": {
                                "describedBy": "https://schema.humancellatlas.org/type/biomaterial/"
                                               "5.0.0/specimen_from_organism",
                                "schema_version": "5.0.0",
                                "schema_type": "biomaterial",
                                "biomaterial_core": {
                                    "describedBy": "https://schema.humancellatlas.org/core/biomaterial/"
                                                   "5.0.0/biomaterial_core",
                                    "biomaterial_id": "s1",
                                    "biomaterial_name": "PBMCs",
                                    "biomaterial_description": "peripheral blood mononuclear cells (PBMCs)",
                                    "ncbi_taxon_id": [
                                        9606
                                    ],
                                    "has_input_biomaterial": "d1"
                                },
                                "organ": {
                                    "describedBy": "https://schema.humancellatlas.org/module/ontology/"
                                                   "5.0.0/organ_ontology",
                                    "text": "blood",
                                    "ontology": "UBERON_0000178"
                                },
                                "organ_part": {
                                    "describedBy": "https://schema.humancellatlas.org/module/ontology/"
                                                   "5.0.0/organ_part_ontology",
                                    "text": "peripheral blood mononuclear cells (PBMCs)"
                                }
                            },
                            "derived_from": ["a37dbd93-b93a-4838-a7bb-cd0796b3d9d1"],
                            "derivation_processes": []
                        }
                    ]
                },
                "ingest_audit_json": {
                    "describedBy": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/"
                                   "5.0.0/json_schema/bundle/ingest_audit.json",
                    "document_id": ".{8}-.{4}-.{4}-.{4}-.{12}",
                    "submissionDate": "08-08-2018"
                }
            }
        }
        doc = 'biomaterial_bundle_json'
        bundle_fqid = self.bundle_key.split('/')[1]
        index_data_expected = copy.deepcopy(index_data_master)
        index_data_expected['files'].pop(doc)
        with self.subTest("Exception is raised when describedBy contains an invalid URL"):
            invalid_url = "://invalid_url"
            index_data = copy.deepcopy(index_data_master)
            index_data['files'][doc]['describedBy'] = invalid_url
            with self.assertRaises(RuntimeError) as context:
                scrub_index_data(index_data['files'], bundle_fqid)
            self.assertRegex(context.exception.args[0], "^No version designator in schema URL")

        with self.subTest("document is removed from meta data when document is missing describedBy field."):
            index_data = copy.deepcopy(index_data_master)
            index_data['files'][doc].pop('describedBy')
            with self.assertLogs(dss.logger, level="WARNING") as log_monitor:
                scrub_index_data(index_data['files'], bundle_fqid)
            self.assertRegexIn(f"WARNING:[^:]+:Unable to retrieve JSON schema information from {doc} in bundle "
                               f"{bundle_fqid}.*", log_monitor.output)
            self.assertDictEqual(index_data_expected, index_data)

        with self.subTest("removes extra fields when fields not specified in schema."):
            index_data = copy.deepcopy(index_data_master)
            index_data['files']['biomaterial_bundle_json'].update({'extra_top': 123,
                                                                   'extra_obj': {"something": "here", "another": 123},
                                                                   'extra_lst': ["a", "b"]})
            index_data['files']['ingest_audit_json']['extra_1'] = "Another extra field in a different file."

        with self.assertLogs(dss.logger, level="INFO") as log_monitor:
            scrub_index_data(index_data['files'], bundle_fqid)
        self.assertDictEqual(index_data_master, index_data)
        self.assertRegexIn(r"INFO:[^:]+:In [\w\-\.]+, unexpected additional fields have been removed from the "
                           r"data to be indexed. Removed \[[^\]]*].", log_monitor.output)

    def verify_notification(self, subscription_id, es_query, bundle_fqid):
        posted_payload_string = self.get_notification_payload()
        self.assertIsNotNone(posted_payload_string)
        posted_json = json.loads(posted_payload_string)
        self.assertIn('transaction_id', posted_json)
        self.assertIn('subscription_id', posted_json)
        self.assertEqual(subscription_id, posted_json['subscription_id'])
        self.assertIn('es_query', posted_json)
        self.assertEqual(es_query, posted_json['es_query'])
        self.assertIn('match', posted_json)
        bundle_uuid, _, bundle_version = bundle_fqid.partition(".")
        self.assertEqual(bundle_uuid, posted_json['match']['bundle_uuid'])
        self.assertEqual(bundle_version, posted_json['match']['bundle_version'])

    def test_indexer_lookup(self):
        for replica in Replica:
            indexer = Indexer.for_replica(replica)
            self.assertIs(replica, indexer.replica)

    @staticmethod
    def get_notification_payload():
        timeout = 5
        timeout_time = time.time() + timeout
        while True:
            posted_payload_string = PostTestHandler.get_payload()
            if posted_payload_string:
                return posted_payload_string
            if time.time() >= timeout_time:
                return None
            else:
                time.sleep(0.5)

    def load_test_data_bundle_for_path(self, fixture_path: str):
        """Loads files into test bucket and returns bundle id"""
        bundle = TestBundle(self.blobstore, fixture_path, self.test_fixture_bucket, self.replica)
        return self.load_test_data_bundle(bundle)

    def load_test_data_bundle_with_inaccessible_file(self, fixture_path: str,
                                                     inaccessible_filename: str,
                                                     inaccessible_file_content_type: str,
                                                     inaccessible_file_indexed: bool):
        bundle = TestBundle(self.blobstore, fixture_path, self.test_fixture_bucket, self.replica)
        self.load_test_data_bundle(bundle)
        bundle_builder = BundleBuilder(self.replica)
        for file in bundle.files:
            bundle_builder.add_file(self.test_bucket, file.name, file.indexed, f'{file.uuid}.{file.version}')
        bundle_builder.add_invalid_file(inaccessible_filename,
                                        inaccessible_file_content_type,
                                        inaccessible_file_indexed)
        bundle_builder.store(self.test_bucket)
        return 'bundles/' + bundle_builder.get_bundle_fqid()

    def load_test_data_bundle(self, bundle: TestBundle):
        self.upload_files_and_create_bundle(bundle, self.replica)
        return f"bundles/{bundle.uuid}.{bundle.version}"

    def subscribe_for_notification(self, es_query, callback_url, **kwargs):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica.name))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(es_query=es_query, callback_url=callback_url, **kwargs),
            headers=get_auth_header()
        )
        uuid_ = resp_obj.json['uuid']
        return uuid_

    def verify_index_document_structure_and_content(self, actual_index_document,
                                                    bundle_key, files, excluded_files=None):
        if excluded_files is None:
            excluded_files = []
        self.verify_index_document_structure(actual_index_document, files, excluded_files)
        expected_index_document = generate_expected_index_document(self.blobstore, self.test_bucket, bundle_key,
                                                                   excluded_files=excluded_files)
        if expected_index_document != actual_index_document:
            logger.error("Expected index document: %s", json.dumps(expected_index_document, indent=4))
            logger.error("Actual index document: %s", json.dumps(actual_index_document, indent=4))
            self.assertDictEqual(expected_index_document, actual_index_document)

    def verify_index_document_structure(self, index_document, files, excluded_files):
        self.assertEqual(4, len(index_document.keys()))
        self.assertEqual("new", index_document['state'])
        self.assertIsNotNone(index_document['uuid'])
        self.assertIsNotNone(index_document['manifest'])
        self.assertIsNotNone(index_document['files'])
        self.assertEqual((len(files) - len(excluded_files)),
                         len(index_document['files'].keys()))
        for filename in files:
            if filename not in excluded_files:
                self.assertIsNotNone(index_document['files'][filename])

    def get_search_results(self, query, min_expected_hit_count):
        response = self.get_raw_search_results(query, min_expected_hit_count)
        return [hit['_source'] for hit in response['hits']['hits']]

    def get_raw_search_results(self, query, min_expected_hit_count):
        # Elasticsearch periodically refreshes the searchable data with newly added data.
        # By default, the refresh interval is one second.
        # https://www.elastic.co/guide/en/elasticsearch/reference/5.5/_modifying_your_data.html
        # Yet, sometimes one second is not quite enough given the churn in the local Elasticsearch instance.
        timeout = 5
        timeout_time = time.time() + timeout
        while True:
            response = ElasticsearchClient.get().search(
                index=self.dss_alias_name,
                doc_type=dss.ESDocType.doc.name,
                body=json.dumps(query))
            if (len(response['hits']['hits']) >= min_expected_hit_count) or (time.time() >= timeout_time):
                return response
            else:
                time.sleep(0.5)

    @abstractmethod
    def create_bundle_created_event(self, bundle_key):
        raise NotImplementedError()

    @abstractmethod
    def create_bundle_deleted_event(self, bundle_key):
        raise NotImplementedError()


class TestAWSIndexer(TestIndexerBase):

    replica = Replica.aws

    def create_bundle_created_event(self, key) -> typing.Dict:
        return self._create_event("sample_s3_bundle_created_event.json", key)

    def create_bundle_deleted_event(self, key) -> typing.Dict:
        return self._create_event("sample_s3_bundle_deleted_event.json", key)

    def _create_event(self, event_template_path, key) -> typing.Dict:
        with open(os.path.join(os.path.dirname(__file__), event_template_path)) as fh:
            sample_event = json.load(fh)
        sample_event['Records'][0]["s3"]['bucket']['name'] = self.test_bucket
        sample_event['Records'][0]["s3"]['object']['key'] = key
        return sample_event


class TestGCPIndexer(TestIndexerBase):

    replica = Replica.gcp

    def create_bundle_created_event(self, key) -> typing.Dict:
        return self._create_event("sample_gs_bundle_created_event.json", key)

    def create_bundle_deleted_event(self, key) -> typing.Dict:
        return self._create_event("sample_s3_bundle_deleted_event.json", key)

    def _create_event(self, event_template_path, key):
        with open(os.path.join(os.path.dirname(__file__), event_template_path)) as fh:
            sample_event = json.load(fh)
        sample_event["bucket"] = self.test_bucket
        sample_event['name'] = key
        return sample_event


class BundleBuilder:
    def __init__(self, replica, bundle_fqid=None, bundle_version=None):
        self.blobstore = Config.get_blobstore_handle(replica)
        self.bundle_fqid = bundle_fqid if bundle_fqid else str(uuid.uuid4())
        self.bundle_version = bundle_version if bundle_version else self._get_version()
        self.bundle_manifest = {
            BundleMetadata.FORMAT: BundleMetadata.FILE_FORMAT_VERSION,
            BundleMetadata.VERSION: self.bundle_version,
            BundleMetadata.FILES: [],
            BundleMetadata.CREATOR_UID: "0"
        }

    def get_bundle_fqid(self):
        return f'{self.bundle_fqid}.{self.bundle_version}'

    def add_file(self, bucket_name, name, indexed, file_id):
        # Add the existing file to the bundle manifest
        file_manifest_string = self.blobstore.get(bucket_name, f"files/{file_id}").decode("utf-8")
        file_manifest = json.loads(file_manifest_string, encoding="utf-8")
        file_uuid, file_version = file_id.split(".", 1)
        bundle_file_manifest = {
            BundleFileMetadata.NAME: name,
            BundleFileMetadata.UUID: file_uuid,
            BundleFileMetadata.VERSION: file_version,
            BundleFileMetadata.CONTENT_TYPE: file_manifest[FileMetadata.CONTENT_TYPE],
            BundleFileMetadata.INDEXED: indexed,
            BundleFileMetadata.CRC32C: file_manifest[FileMetadata.CRC32C],
            BundleFileMetadata.S3_ETAG: file_manifest[FileMetadata.S3_ETAG],
            BundleFileMetadata.SHA1: file_manifest[FileMetadata.SHA1],
            BundleFileMetadata.SHA256: file_manifest[FileMetadata.SHA256],
        }
        self.bundle_manifest[BundleMetadata.FILES].append(bundle_file_manifest)

    def add_invalid_file(self, name, content_type, indexed):
        bundle_file_manifest = {
            BundleFileMetadata.NAME: name,
            BundleFileMetadata.UUID: str(uuid.uuid4()),
            BundleFileMetadata.VERSION: self._get_version(),
            BundleFileMetadata.CONTENT_TYPE: content_type,
            BundleFileMetadata.INDEXED: indexed,
            BundleFileMetadata.CRC32C: "0",
            BundleFileMetadata.S3_ETAG: "0",
            BundleFileMetadata.SHA1: "0",
            BundleFileMetadata.SHA256: "0",
        }
        self.bundle_manifest[BundleMetadata.FILES].append(bundle_file_manifest)

    def store(self, bucket_name):
        # noinspection PyTypeChecker
        self.blobstore.upload_file_handle(bucket_name,
                                          'bundles/' + self.get_bundle_fqid(),
                                          io.BytesIO(json.dumps(self.bundle_manifest).encode("utf-8")))

    def _get_version(self):
        return datetime_to_version_format(datetime.datetime.utcnow())


class PostTestHandler(BaseHTTPRequestHandler):
    _response_code = 200
    _payload = None
    hmac_secret_key = "ribos0me"
    verify_payloads = True

    def do_POST(self):
        if self.verify_payloads:
            HTTPSignatureAuth.verify(requests.Request("POST", self.path, self.headers),
                                     key_resolver=lambda key_id, algorithm: self.hmac_secret_key.encode())
            try:
                HTTPSignatureAuth.verify(requests.Request("POST", self.path, self.headers),
                                         key_resolver=lambda key_id, algorithm: self.hmac_secret_key[::-1].encode())
                raise Exception("Expected AssertionError")
            except AssertionError:
                pass
        self.send_response(self._response_code)
        self.send_header("Content-length", "0")
        self.end_headers()
        length = int(self.headers['content-length'])
        if length:
            PostTestHandler._payload = self.rfile.read(length).decode("utf-8")

    @classmethod
    def reset(cls):
        cls._response_code = 200
        cls._payload = None
        cls.verify_payloads = True

    @classmethod
    def set_response_code(cls, code: int):
        cls._response_code = code

    @classmethod
    def get_payload(cls):
        return cls._payload

    def log_request(self, code='-', size='-'):
        if Config.debug_level():
            super().log_request(code, size)


smartseq2_paried_ends_indexed_file_list = ["assay_json", "project_json", "sample_json"]
smartseq2_paried_ends_indexed_excluded_list = ["manifest_json", "cell_json"]


def create_s3_bucket(bucket_name) -> None:
    import boto3
    from botocore.exceptions import ClientError
    conn = boto3.resource("s3")
    try:
        conn.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if ex.response['Error']['Code'] != "BucketAlreadyOwnedByYou":
            logger.error(f"An unexpected error occured when creating test bucket: {bucket_name}")


def generate_expected_index_document(blobstore, bucket_name, bundle_key, excluded_files=None):
    if excluded_files is None:
        excluded_files = []
    manifest = read_bundle_manifest(blobstore, bucket_name, bundle_key)
    index_data = create_index_data(blobstore, bucket_name, bundle_key, manifest, excluded_files)
    return index_data


def read_bundle_manifest(blobstore, bucket_name, bundle_key):
    manifest_string = blobstore.get(bucket_name, bundle_key).decode("utf-8")
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(blobstore, bucket_name, bundle_key, manifest,
                      excluded_files=None) -> typing.MutableMapping[str, typing.Any]:
    if excluded_files is None:
        excluded_files = []
    index = dict(state="new", manifest=manifest, uuid=BundleFQID.from_key(bundle_key).uuid)
    files_info = manifest['files']
    excluded_file = [file.replace('_', '.') for file in excluded_files]
    index_files = {}
    for file_info in files_info:
        if file_info['indexed'] is True and file_info["name"] not in excluded_file:
            try:
                file_key = create_blob_key(file_info)
                content_type = file_info[BundleFileMetadata.CONTENT_TYPE]
                if content_type != "application/json":
                    continue
                file_string = blobstore.get(bucket_name, file_key).decode("utf-8")
                file_json = json.loads(file_string)
            except Exception:
                continue
            index_filename = file_info["name"].replace(".", "_")
            index_files[index_filename] = file_json
    index['files'] = index_files
    return index


# Prevent unittest's discovery from attempting to discover the base test class. The alterative, not inheriting
# TestCase in the base class, is too inconvenient because it interferes with auto-complete and generates PEP-8
# warnings about the camel case methods.
#
del TestIndexerBase


if __name__ == "__main__":
    unittest.main()
