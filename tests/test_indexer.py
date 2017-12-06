#!/usr/bin/env python
# coding: utf-8

import datetime
import io
import json
import logging
import os
import sys
import threading
import time
import typing
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import google.auth
import google.auth.transport.requests
import requests
from requests_http_signature import HTTPSignatureAuth

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, BucketConfig, DeploymentStage
from dss.config import IndexSuffix, ESDocType, Replica
from dss.events.handlers.index import AWSIndexHandler, GCPIndexHandler, BundleDocument, create_elasticsearch_index
from dss.hcablobstore import BundleMetadata, BundleFileMetadata, FileMetadata
from dss.util import create_blob_key, networking, UrlBuilder
from dss.util.bundles import bundle_key_to_bundle_fqid
from dss.util.es import ElasticsearchClient, ElasticsearchServer
from dss.util.version import datetime_to_version_format
from dss.events.handlers.index import DSS_OBJECT_NAME_REGEX, DSS_BUNDLE_KEY_REGEX
from tests import get_version
from tests.es import elasticsearch_delete_index, clear_indexes
from tests.infra import DSSAssertMixin, DSSUploadMixin, DSSStorageMixin, TestBundle, start_verbose_logging
from tests.infra.server import ThreadedLocalServer
from tests.sample_search_queries import (smartseq2_paired_ends_v2_query, smartseq2_paired_ends_v3_query,
                                         smartseq2_paired_ends_v2_or_v3_query)

from tests import eventually

# The moto mock has two defects that show up when used by the dss core storage system.
# Use actual S3 until these defects are fixed in moto.
# TODO (mbaumann) When the defects in moto have been fixed, remove True from the line below.
USE_AWS_S3 = bool(os.environ.get("USE_AWS_S3", True))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

start_verbose_logging()


# TODO: (tsmith) test with multiple doc indexes once indexing by major version is compeleted

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


class ESInfo:
    server = None


def setUpModule():
    IndexSuffix.name = __name__.rsplit('.', 1)[-1]
    HTTPInfo.port = networking.unused_tcp_port()
    HTTPInfo.server = HTTPServer((HTTPInfo.address, HTTPInfo.port), PostTestHandler)
    HTTPInfo.thread = threading.Thread(target=HTTPInfo.server.serve_forever)
    HTTPInfo.thread.start()

    ESInfo.server = ElasticsearchServer()
    os.environ['DSS_ES_PORT'] = str(ESInfo.server.port)


def tearDownModule():
    ESInfo.server.shutdown()
    HTTPInfo.server.shutdown()
    IndexSuffix.reset()
    os.unsetenv('DSS_ES_PORT')


class TestIndexerBase(DSSAssertMixin, DSSStorageMixin, DSSUploadMixin):
    bundle_key_by_replica = dict()  # type: typing.MutableMapping[str, str]

    @classmethod
    def indexer_setup(cls, replica):
        cls.app = ThreadedLocalServer()
        cls.app.start()
        cls.replica = replica
        Config.set_config(BucketConfig.TEST_FIXTURE)
        cls.blobstore, _, cls.test_fixture_bucket = Config.get_cloud_specific_handles(cls.replica)
        Config.set_config(BucketConfig.TEST)
        _, _, cls.test_bucket = Config.get_cloud_specific_handles(cls.replica)
        cls.dss_alias_name = dss.Config.get_es_alias_name(dss.ESIndexType.docs, dss.Replica[cls.replica])
        cls.subscription_index_name = dss.Config.get_es_index_name(dss.ESIndexType.subscriptions,
                                                                   dss.Replica[cls.replica])

    @classmethod
    def tearDownClass(cls):
        elasticsearch_delete_index(f"*{IndexSuffix.name}")
        cls.app.shutdown()

    def setUp(self):
        if self.replica not in TestIndexerBase.bundle_key_by_replica:
            TestIndexerBase.bundle_key_by_replica[self.replica] = self.load_test_data_bundle_for_path(
                "fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        self.bundle_key = TestIndexerBase.bundle_key_by_replica[self.replica]
        self.smartseq2_paired_ends_query = smartseq2_paired_ends_v2_or_v3_query
        PostTestHandler.reset()

    def tearDown(self):
        clear_indexes([self.dss_alias_name],
                      [ESDocType.doc.name, ESDocType.query.name, ESDocType.subscription.name])
        clear_indexes([self.subscription_index_name],
                      [ESDocType.doc.name, ESDocType.query.name, ESDocType.subscription.name])
        self.storageHelper = None

    def test_process_new_indexable_object_create(self):
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(
            search_results[0],
            self.bundle_key,
            files=smartseq2_paried_ends_indexed_file_list,
        )

    def test_process_new_indexable_object_delete(self):
        bundle_uuid, _ = DSS_OBJECT_NAME_REGEX.match(self.bundle_key).groups()
        # delete the whole bundle
        self._test_process_new_indexable_object_delete(self.bundle_key + ".dead")
        # delete a specific bundle version
        self._test_process_new_indexable_object_delete(f"bundles/{bundle_uuid}.dead")

    def _test_process_new_indexable_object_delete(self, deletion_object_name):
        bundle_uuid, version = DSS_OBJECT_NAME_REGEX.search(deletion_object_name).groups()
        # set the tombstone
        blobstore, _, bucket = Config.get_cloud_specific_handles(self.replica)
        blobstore.upload_file_handle(bucket, deletion_object_name, io.BytesIO(b"{}"))

        # send the
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        self.get_search_results(self.smartseq2_paired_ends_query, 1)

        sample_event = self.create_sample_bundle_deleted_event(deletion_object_name)
        self.process_new_indexable_object(sample_event, logger)

        @eventually(5.0, 0.5)
        def _deletion_results_test():
            search_results = self.get_search_results(self.smartseq2_paired_ends_query, 0)
            self.assertEqual(0, len(search_results))
            bundle_fqids = [
                bundle_key_to_bundle_fqid(k) for k in blobstore.list(bucket, f"bundles/{bundle_uuid}.")
                if DSS_BUNDLE_KEY_REGEX.match(k)
            ]
            for bundle_fqid in bundle_fqids:
                exact_query = {
                    "query": {
                        "terms": {
                            "_id": [bundle_fqid]
                        }
                    }
                }
                search_results = self.get_search_results(exact_query, 1)
                self.assertEqual(1, len(search_results))
                self.assertEqual(search_results[0], {})

        _deletion_results_test()

    def test_indexed_file_with_invalid_content_type(self):
        bundle = TestBundle(self.blobstore, "fixtures/indexing/bundles/v3/smartseq2/paired_ends",
                            self.test_fixture_bucket, self.replica)
        # Configure a file to be indexed that is not of context type 'application/json'
        for file in bundle.files:
            if file.name == "text_data_file1.txt":
                file.indexed = True
        bundle_key = self.load_test_data_bundle(bundle)
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file 'text_data_file1.txt' is marked for indexing"
                         " yet has content type 'text/plain' instead of the required"
                         " content type 'application/json'. This file will not be indexed.")
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_key_is_not_indexed_when_processing_an_event_with_a_nonbundle_key(self):
        elasticsearch_delete_index(f'*{IndexSuffix.name}')
        bundle_uuid = "{}.{}".format(str(uuid.uuid4()), get_version())
        bundle_key = "files/" + bundle_uuid
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        log_last = logger.getEffectiveLevel()
        logger.setLevel(logging.DEBUG)
        try:
            with self.assertLogs(logger, level="DEBUG") as log_monitor:
                self.process_new_indexable_object(sample_event, logger)
            self.assertRegex(log_monitor.output[0], "DEBUG:.*Not indexing .* creation event for key: .*")
            self.assertFalse(ElasticsearchClient.get(logger).indices.exists_alias(self.dss_alias_name))
        finally:
            logger.setLevel(log_last)

    def test_error_message_logged_when_invalid_bucket_in_event(self):
        bundle_key = "bundles/{}.{}".format(str(uuid.uuid4()), get_version())
        sample_event = self.create_bundle_created_event(bundle_key, "fake")
        with self.assertLogs(logger, level="ERROR") as log_monitor:
            with self.assertRaises(Exception):
                self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0], "ERROR:.*Exception occurred while processing .* event:.*")

    def test_indexed_file_unparsable(self):
        bundle_key = self.load_test_data_bundle_for_path("fixtures/indexing/bundles/unparseable_indexed_file")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0],
                         "WARNING:.*:In bundle .* the file 'unparseable_json.json' is marked for indexing"
                         " yet could not be parsed. This file will not be indexed. Exception:")
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)

    def test_indexed_file_access_error(self):
        inaccesssible_filename = "inaccessible_file.json"
        elasticsearch_delete_index(f'*{IndexSuffix.name}')
        bundle_key = self.load_test_data_bundle_with_inaccessible_file(
            "fixtures/indexing/bundles/v3/smartseq2/paired_ends", inaccesssible_filename, "application/json", True)
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        self.assertRegex(log_monitor.output[0],
                         f"WARNING:.*:In bundle .* the file '{inaccesssible_filename}' is marked for indexing"
                         " yet could not be accessed. This file will not be indexed. Exception: .*, File blob key:")
        search_results = self.get_search_results(self.smartseq2_paired_ends_query, 1)
        self.assertEqual(1, len(search_results))
        files = list(smartseq2_paried_ends_indexed_file_list)
        files.append(inaccesssible_filename.replace(".", "_"))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=files,
                                                         excluded_files=[inaccesssible_filename.replace(".", "_")])

    def test_notify(self):
        def _notify(subscription, bundle_id=get_bundle_fqid()):
            document = BundleDocument.from_json(Replica[self.replica], bundle_id, {}, logger)
            document.notify_subscriber(subscription=subscription)
        with self.assertRaisesRegex(requests.exceptions.InvalidURL, "Invalid URL 'http://': No host supplied"):
            _notify(subscription=dict(id="", es_query={}, callback_url="http://"))
        with self.assertRaisesRegex(AssertionError, "Unexpected scheme for callback URL"):
            _notify(subscription=dict(id="", es_query={}, callback_url=""))
        with self.assertRaisesRegex(AssertionError, "Unexpected scheme for callback URL"):
            _notify(subscription=dict(id="", es_query={}, callback_url="wss://127.0.0.1"))
        try:
            environ_backup = os.environ
            os.environ = dict(DSS_DEPLOYMENT_STAGE=DeploymentStage.PROD.value)
            with self.assertRaisesRegex(AssertionError, "Unexpected scheme for callback URL"):
                _notify(subscription=dict(id="", es_query={}, callback_url="http://example.com"))
            with self.assertRaisesRegex(AssertionError, "Callback hostname resolves to forbidden network"):
                _notify(subscription=dict(id="", es_query={}, callback_url="https://127.0.0.1"))
        finally:
            os.environ = environ_backup

    def delete_subscription(self, subscription_id):
        self.assertDeleteResponse(
            str(UrlBuilder().set(path=f"/v1/subscriptions/{subscription_id}").add_query("replica", self.replica)),
            requests.codes.ok,
            headers=self.get_auth_header()
        )

    def test_subscription_notification_successful(self):
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        for verify_payloads, subscribe_kwargs in ((True, dict(hmac_secret_key=PostTestHandler.hmac_secret_key)),
                                                  (False, dict())):
            PostTestHandler.verify_payloads = verify_payloads
            subscription_id = self.subscribe_for_notification(self.smartseq2_paired_ends_query,
                                                              f"http://{HTTPInfo.address}:{HTTPInfo.port}",
                                                              **subscribe_kwargs)

            sample_event = self.create_sample_bundle_created_event(self.bundle_key)
            self.process_new_indexable_object(sample_event, logger)
            prefix, _, bundle_id = self.bundle_key.partition("/")
            self.verify_notification(subscription_id, self.smartseq2_paired_ends_query, bundle_id)
            self.delete_subscription(subscription_id)
            PostTestHandler.reset()

    def test_subscription_notification_unsuccessful(self):
        PostTestHandler.verify_payloads = True
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        subscription_id = self.subscribe_for_notification(self.smartseq2_paired_ends_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}",
                                                          hmac_secret_key=PostTestHandler.hmac_secret_key,
                                                          hmac_key_id="test")

        bundle_key = self.load_test_data_bundle_for_path("fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        error_response_code = 500
        PostTestHandler.set_response_code(error_response_code)
        with self.assertLogs(logger, level="WARNING") as log_monitor:
            self.process_new_indexable_object(sample_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.assertRegex(log_monitor.output[0],
                         f"WARNING:.*:Failed notification for subscription {subscription_id}"
                         f" for bundle {bundle_id} with transaction id .+ Code: {error_response_code}")

    def test_subscription_registration_before_indexing(self):
        elasticsearch_delete_index(f'*{IndexSuffix.name}')
        subscription_id = self.subscribe_for_notification(self.smartseq2_paired_ends_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        PostTestHandler.verify_payloads = False
        self.process_new_indexable_object(sample_event, logger)
        prefix, _, bundle_id = self.bundle_key.partition("/")
        self.verify_notification(subscription_id, self.smartseq2_paired_ends_query, bundle_id)
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

        elasticsearch_delete_index(f"*{IndexSuffix.name}")
        subscription_id = self.subscribe_for_notification(subscription_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        PostTestHandler.verify_payloads = False
        self.process_new_indexable_object(sample_event, logger)

        # Verify the mapping types are as expected for a valid test
        doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, dss.Replica[self.replica], "v3")
        mappings = ElasticsearchClient.get(logger).indices.get_mapping(doc_index_name)[doc_index_name]['mappings']
        sample_json_mappings = mappings['doc']['properties']['files']['properties']['sample_json']
        self.assertEquals(sample_json_mappings['properties']['donor']['properties']['age']['type'], "long")
        self.assertEquals(sample_json_mappings['properties']['submit_date']['type'], "date")
        self.assertEquals(sample_json_mappings['properties']['ncbi_biosample']['type'], "keyword")

        # Verify the query works correctly as a search
        search_results = self.get_search_results(subscription_query, 1)
        self.assertEqual(1, len(search_results))

        # Verify the query works correctly as a subscription, resulting in notification
        prefix, _, bundle_id = self.bundle_key.partition("/")
        self.verify_notification(subscription_id, subscription_query, bundle_id)
        self.delete_subscription(subscription_id)

    def test_get_shape_descriptor(self):
        index_document = BundleDocument.from_json(self.replica, 'uuid.version', {
            'files': {
                'assay_json': {
                    'core': {
                        'schema_url': "http://hgwdev.soe.ucsc.edu/~kent/hca/schema/assay.json",
                        'schema_version': "3.0.0",
                        'type': "assay"
                    }
                },
                'sample_json': {
                    'core': {
                        'schema_url': "http://hgwdev.soe.ucsc.edu/~kent/hca/schema/sample.json",
                        'schema_version': "3.0.0",
                        'type': "sample"
                    }
                }
            }
        }, logger)
        with self.subTest("Same major version."):
            self.assertEqual(index_document.get_shape_descriptor(), "v3")

        index_document['files']['assay_json']['core']['schema_version'] = "4.0.0"
        with self.subTest("Mixed/inconsistent metadata schema release versions in the same bundle"):
            with self.assertRaisesRegex(AssertionError,
                                        "The bundle contains mixed schema major version numbers: \['3', '4'\]"):
                index_document.get_shape_descriptor()

        index_document['files']['sample_json']['core']['schema_version'] = "4.0.0"
        with self.subTest("Consistent versions, with a different version value"):
            self.assertEqual(index_document.get_shape_descriptor(), "v4")

        index_document['files']['assay_json'].pop('core')
        with self.subTest("An version file and unversioned file"):
            with self.assertLogs(logger, level="INFO") as log_monitor:
                index_document.get_shape_descriptor()
            self.assertRegex(log_monitor.output[0], ("INFO:.*File assay_json does not contain a 'core' section "
                                                     "to identify the schema and schema version."))
            self.assertEqual(index_document.get_shape_descriptor(), "v4")

        index_document['files']['sample_json'].pop('core')
        with self.subTest("no versioned file"):
            self.assertEqual(index_document.get_shape_descriptor(), None)

    def test_alias_and_versioned_index_exists(self):
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        es_client = ElasticsearchClient.get(logger)
        self.assertTrue(es_client.indices.exists_alias(name=[self.dss_alias_name]))
        alias = es_client.indices.get_alias(name=[self.dss_alias_name])
        doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, dss.Replica[self.replica], "v3")
        self.assertIn(doc_index_name, alias)
        self.assertTrue(es_client.indices.exists(index=doc_index_name))

    def test_alias_and_multiple_schema_version_index_exists(self):
        # Load and test an unversioned bundle
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/unversioned/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        es_client = ElasticsearchClient.get(logger)
        alias = es_client.indices.get_alias(name=[self.dss_alias_name])
        unversioned_doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, dss.Replica[self.replica], None)
        self.assertIn(unversioned_doc_index_name, alias)
        self.assertTrue(es_client.indices.exists(index=unversioned_doc_index_name))

        # Load and test a v3 bundle
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        self.assertTrue(es_client.indices.exists_alias(name=[self.dss_alias_name]))
        alias = es_client.indices.get_alias(name=[self.dss_alias_name])
        doc_index_name = dss.Config.get_es_index_name(dss.ESIndexType.docs, dss.Replica[self.replica], "v3")
        # Ensure the alias references both indices
        self.assertIn(unversioned_doc_index_name, alias)
        self.assertIn(doc_index_name, alias)
        self.assertTrue(es_client.indices.exists(index=doc_index_name))

    def test_multiple_schema_version_indexing_and_search(self):
        # Load a schema version 2 (unversioned) bundle
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/unversioned/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        # Search using a v2-specific query - should match
        search_results = self.get_search_results(smartseq2_paired_ends_v2_query, 1)
        self.assertEqual(1, len(search_results))
        self.verify_index_document_structure_and_content(search_results[0], bundle_key,
                                                         files=smartseq2_paried_ends_indexed_file_list)
        # Search using a query that works for v2 or v3 - should match
        search_results = self.get_search_results(smartseq2_paired_ends_v2_or_v3_query, 1)
        self.assertEqual(1, len(search_results))

        # Search using a v3-specific query - should not match
        search_results = self.get_search_results(smartseq2_paired_ends_v3_query, 0)
        self.assertEqual(0, len(search_results))

        # Load a v3 bundle
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        # Search using a v3-specific query - should match
        search_results = self.get_search_results(smartseq2_paired_ends_v3_query, 1)
        self.assertEqual(1, len(search_results))

        # Search using a query that works for v2 or v3 - should match both v2 and v3 bundles
        search_results = self.get_search_results(smartseq2_paired_ends_v2_or_v3_query, 2)
        self.assertEqual(2, len(search_results))

    def test_multiple_schema_version_subscription_indexing_and_notification(self):
        PostTestHandler.verify_payloads = False

        # Load a schema version 2 (unversioned) bundle
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/unversioned/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        # Load a v3 bundle
        sample_event = self.create_sample_bundle_created_event(self.bundle_key)
        self.process_new_indexable_object(sample_event, logger)

        subscription_id = self.subscribe_for_notification(smartseq2_paired_ends_v2_or_v3_query,
                                                          f"http://{HTTPInfo.address}:{HTTPInfo.port}")

        # Load another schema version 2 (unversioned) bundle and verify notification
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/unversioned/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.verify_notification(subscription_id, smartseq2_paired_ends_v2_or_v3_query, bundle_id)

        PostTestHandler.reset()
        PostTestHandler.verify_payloads = False

        # Load another schema version 3 bundle and verify notification
        bundle_key = self.load_test_data_bundle_for_path(
            "fixtures/indexing/bundles/v3/smartseq2/paired_ends")
        sample_event = self.create_sample_bundle_created_event(bundle_key)
        self.process_new_indexable_object(sample_event, logger)
        prefix, _, bundle_id = bundle_key.partition("/")
        self.verify_notification(subscription_id, smartseq2_paired_ends_v2_or_v3_query, bundle_id)

        self.delete_subscription(subscription_id)

    def verify_notification(self, subscription_id, es_query, bundle_id):
        posted_payload_string = self.get_notification_payload()
        self.assertIsNotNone(posted_payload_string)
        posted_json = json.loads(posted_payload_string)
        self.assertIn('transaction_id', posted_json)
        self.assertIn('subscription_id', posted_json)
        self.assertEqual(subscription_id, posted_json['subscription_id'])
        self.assertIn('es_query', posted_json)
        self.assertEqual(es_query, posted_json['es_query'])
        self.assertIn('match', posted_json)
        bundle_uuid, _, bundle_version = bundle_id.partition(".")
        self.assertEqual(bundle_uuid, posted_json['match']['bundle_uuid'])
        self.assertEqual(bundle_version, posted_json['match']['bundle_version'])

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
        return 'bundles/' + bundle_builder.get_bundle_id()

    def load_test_data_bundle(self, bundle: TestBundle):
        self.upload_files_and_create_bundle(bundle, self.replica)
        return f"bundles/{bundle.uuid}.{bundle.version}"

    def subscribe_for_notification(self, es_query, callback_url, **kwargs):
        url = str(UrlBuilder()
                  .set(path="/v1/subscriptions")
                  .add_query("replica", self.replica))
        resp_obj = self.assertPutResponse(
            url,
            requests.codes.created,
            json_request_body=dict(es_query=es_query, callback_url=callback_url, **kwargs),
            headers=self.get_auth_header()
        )
        uuid_ = resp_obj.json['uuid']
        return uuid_

    def get_auth_header(self, token=None):
        credentials, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/userinfo.email"])

        if not token:
            r = google.auth.transport.requests.Request()
            credentials.refresh(r)
            r.session.close()
            token = credentials.token

        return {'Authorization': f"Bearer {token}"}

    def verify_index_document_structure_and_content(self, actual_index_document,
                                                    bundle_key, files, excluded_files=[]):
        self.verify_index_document_structure(actual_index_document, files, excluded_files)
        expected_index_document = generate_expected_index_document(self.blobstore, self.test_bucket, bundle_key)
        if expected_index_document != actual_index_document:
            logger.error(f"Expected index document: {json.dumps(expected_index_document, indent=4)}")
            logger.error(f"Actual index document: {json.dumps(actual_index_document, indent=4)}")
            self.assertDictEqual(expected_index_document, actual_index_document)

    def verify_index_document_structure(self, index_document, files, excluded_files):
        self.assertEqual(3, len(index_document.keys()))
        self.assertEqual("new", index_document['state'])
        self.assertIsNotNone(index_document['manifest'])
        self.assertIsNotNone(index_document['files'])
        self.assertEqual((len(files) - len(excluded_files)),
                         len(index_document['files'].keys()))
        for filename in files:
            if filename not in excluded_files:
                self.assertIsNotNone(index_document['files'][filename])

    @classmethod
    def get_search_results(cls, query, expected_hit_count):
        # Elasticsearch periodically refreshes the searchable data with newly added data.
        # By default, the refresh interval is one second.
        # https://www.elastic.co/guide/en/elasticsearch/reference/5.5/_modifying_your_data.html
        # Yet, sometimes one second is not quite enough given the churn in the local Elasticsearch instance.
        timeout = 5
        timeout_time = time.time() + timeout
        while True:
            response = ElasticsearchClient.get(logger).search(
                index=cls.dss_alias_name,
                doc_type=dss.ESDocType.doc.name,
                body=json.dumps(query))
            if (len(response['hits']['hits']) >= expected_hit_count) or (time.time() >= timeout_time):
                return [hit['_source'] for hit in response['hits']['hits']]
            else:
                time.sleep(0.5)

    def create_sample_bundle_created_event(self, bundle_key):
        return self.create_bundle_created_event(bundle_key, self.test_bucket)

    def create_sample_bundle_deleted_event(self, bundle_key):
        return self.create_bundle_deleted_event(bundle_key, self.test_bucket)

    def create_bundle_created_event(self, bundle_key, bucket_name):
        raise NotImplemented()

    def process_new_indexable_object(self, event, logger):
        raise NotImplemented()


class TestAWSIndexer(AWSIndexHandler, TestIndexerBase, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().indexer_setup("aws")

    def create_bundle_created_event(self, bundle_key, bucket_name) -> typing.Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_created_event.json")) as fh:
            sample_event = json.load(fh)
        sample_event['Records'][0]["s3"]['bucket']['name'] = bucket_name
        sample_event['Records'][0]["s3"]['object']['key'] = bundle_key
        return sample_event

    def create_bundle_deleted_event(self, bundle_key, bucket_name) -> typing.Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_deleted_event.json")) as fh:
            sample_event = json.load(fh)
        sample_event['Records'][0]["s3"]['bucket']['name'] = bucket_name
        sample_event['Records'][0]["s3"]['object']['key'] = bundle_key
        return sample_event


class TestGCPIndexer(GCPIndexHandler, TestIndexerBase, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().indexer_setup("gcp")

    def create_bundle_created_event(self, bundle_key, bucket_name) -> typing.Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_gs_bundle_created_event.json")) as fh:
            sample_event = json.load(fh)
        sample_event["bucket"] = bucket_name
        sample_event["name"] = bundle_key
        return sample_event

    def create_bundle_deleted_event(self, key, bucket_name) -> typing.Dict:
        with open(os.path.join(os.path.dirname(__file__), "sample_s3_bundle_deleted_event.json")) as fh:
            sample_event = json.load(fh)
        sample_event['bucket'] = bucket_name
        sample_event['name'] = key
        return sample_event


class BundleBuilder:
    def __init__(self, replica, bundle_id=None, bundle_version=None):
        self.blobstore, _, _ = Config.get_cloud_specific_handles(replica)
        self.bundle_id = bundle_id if bundle_id else str(uuid.uuid4())
        self.bundle_version = bundle_version if bundle_version else self._get_version()
        self.bundle_manifest = {
            BundleMetadata.FORMAT: BundleMetadata.FILE_FORMAT_VERSION,
            BundleMetadata.VERSION: self.bundle_version,
            BundleMetadata.FILES: [],
            BundleMetadata.CREATOR_UID: "0"
        }

    def get_bundle_id(self):
        return f'{self.bundle_id}.{self.bundle_version}'

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
        self.blobstore.upload_file_handle(bucket_name,
                                          'bundles/' + self.get_bundle_id(),
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


smartseq2_paried_ends_indexed_file_list = ["assay_json", "cell_json", "manifest_json", "project_json", "sample_json"]


def create_s3_bucket(bucket_name) -> None:
    import boto3
    from botocore.exceptions import ClientError
    conn = boto3.resource("s3")
    try:
        conn.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        if ex.response['Error']['Code'] != "BucketAlreadyOwnedByYou":
            logger.error(f"An unexpected error occured when creating test bucket: {bucket_name}")


def generate_expected_index_document(blobstore, bucket_name, bundle_key):
    manifest = read_bundle_manifest(blobstore, bucket_name, bundle_key)
    index_data = create_index_data(blobstore, bucket_name, manifest)
    return index_data


def read_bundle_manifest(blobstore, bucket_name, bundle_key):
    manifest_string = blobstore.get(bucket_name, bundle_key).decode("utf-8")
    manifest = json.loads(manifest_string, encoding="utf-8")
    return manifest


def create_index_data(blobstore, bucket_name, manifest):
    index = dict(state="new", manifest=manifest)
    files_info = manifest['files']
    index_files = {}
    for file_info in files_info:
        if file_info['indexed'] is True:
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


if __name__ == "__main__":
    unittest.main()
