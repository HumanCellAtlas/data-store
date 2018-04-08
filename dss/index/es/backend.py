import json
import logging
from typing import MutableSet
import uuid

from elasticsearch.helpers import scan

from dss import ESDocType, ESIndexType, Config
from dss.index.backend import IndexBackend
from dss.index.bundle import Bundle, Tombstone
from dss.notify.notification import Notification, Endpoint

from . import elasticsearch_retry, ElasticsearchClient, TIME_NEEDED
from .document import BundleDocument, BundleTombstoneDocument

logger = logging.getLogger(__name__)


class ElasticsearchIndexBackend(IndexBackend):

    @elasticsearch_retry(logger)
    def index_bundle(self, bundle: Bundle):
        self._is_enough_time()
        elasticsearch_retry.add_context(bundle=bundle)
        doc = BundleDocument.from_bundle(bundle)
        modified, index_name = doc.index(dryrun=self.dryrun)
        if self.notify or modified and self.notify is None:
            self._notify(doc, index_name)

    @elasticsearch_retry(logger)
    def remove_bundle(self, bundle: Bundle, tombstone: Tombstone):
        self._is_enough_time()
        elasticsearch_retry.add_context(tombstone=tombstone, bundle=bundle)
        doc = BundleDocument.from_bundle(bundle)
        tombstone_doc = BundleTombstoneDocument.from_tombstone(tombstone)
        modified, index_name = doc.entomb(tombstone_doc, dryrun=self.dryrun)
        if self.notify or modified and self.notify is None:
            self._notify(doc, index_name)

    def _notify(self, bundle, index_name):
        subscription_ids = self._find_matching_subscriptions(bundle, index_name)
        self._notify_subscribers(bundle, subscription_ids)

    def _find_matching_subscriptions(self, doc: BundleDocument, index_name: str) -> MutableSet[str]:
        percolate_document = {
            'query': {
                'percolate': {
                    'field': "query",
                    'document_type': ESDocType.doc.name,
                    'document': doc
                }
            }
        }
        subscription_ids = set()
        for hit in scan(ElasticsearchClient.get(),
                        index=index_name,
                        query=percolate_document):
            subscription_ids.add(hit["_id"])
        logger.debug(f"Found {len(subscription_ids)} matching subscription(s).")
        return subscription_ids

    def _notify_subscribers(self, bundle: BundleDocument, subscription_ids: MutableSet[str]):
        for subscription_id in subscription_ids:
            try:
                # TODO Batch this request
                subscription = self._get_subscription(bundle, subscription_id)
                self._notify_subscriber(bundle, subscription)
            except Exception:
                logger.error(f"Error occurred while processing subscription {subscription_id} "
                             f"for bundle {bundle.fqid}.", exc_info=True)

    def _get_subscription(self, doc: BundleDocument, subscription_id: str) -> dict:
        subscription_query = {
            'query': {
                'ids': {
                    'type': ESDocType.subscription.name,
                    'values': [subscription_id]
                }
            }
        }
        response = ElasticsearchClient.get().search(
            index=Config.get_es_index_name(ESIndexType.subscriptions, doc.replica),
            body=subscription_query)
        hits = response['hits']['hits']
        assert len(hits) == 1
        hit = hits[0]
        assert hit['_id'] == subscription_id
        subscription = hit['_source']
        assert 'id' not in subscription
        subscription['id'] = subscription_id
        return subscription

    def _notify_subscriber(self, doc: BundleDocument, subscription: dict):
        transaction_id = str(uuid.uuid4())
        subscription_id = subscription['id']
        endpoint = Endpoint.from_subscription(subscription)

        payload = dict(transaction_id=transaction_id,
                       subscription_id=subscription_id,
                       es_query=subscription['es_query'],
                       match=dict(bundle_uuid=doc.fqid.uuid,
                                  bundle_version=doc.fqid.version))

        if endpoint.encoding == 'application/json':
            body = payload
        elif endpoint.encoding == 'multipart/form-data':
            body = endpoint.form_fields.copy()
            body[endpoint.payload_form_field] = json.dumps(payload)
        else:
            raise ValueError(f"Encoding {endpoint.encoding} is not supported")

        try:
            hmac_key = subscription['hmac_secret_key']
        except KeyError:
            hmac_key = None
            hmac_key_id = None
        else:
            hmac_key = hmac_key.encode()
            hmac_key_id = subscription.get('hmac_key_id', "hca-dss:" + subscription_id)

        notification = Notification.create(notification_id=transaction_id,
                                           subscription_id=subscription_id,
                                           url=endpoint.callback_url,
                                           method=endpoint.method,
                                           encoding=endpoint.encoding,
                                           body=body,
                                           hmac_key=hmac_key,
                                           hmac_key_id=hmac_key_id)
        logger.info(f"Sending notification {notification} about bundle {doc.fqid}")
        notification.deliver_or_raise()

    def _is_enough_time(self):
        if self.context.get_remaining_time_in_millis() / 1000 <= TIME_NEEDED:
            raise RuntimeError("Not enough time to complete indexing operation.")
