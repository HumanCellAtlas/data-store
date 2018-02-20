import logging

from dss.index.backend import IndexBackend, CompositeIndexBackend
from dss.index.bundle import Bundle, Tombstone
from . import elasticsearch_retry
from .document import BundleDocument, BundleTombstoneDocument

logger = logging.getLogger(__name__)


class ElasticsearchIndexBackend(IndexBackend):

    @elasticsearch_retry(logger)
    def index_bundle(self, bundle: Bundle):
        elasticsearch_retry.add_context(bundle=bundle)
        doc = BundleDocument.from_bundle(bundle)
        modified, index_name = doc.index(dryrun=self.dryrun)
        if self.notify or modified and self.notify is None:
            doc.notify(index_name)

    @elasticsearch_retry(logger)
    def remove_bundle(self, bundle: Bundle, tombstone: Tombstone):
        elasticsearch_retry.add_context(tombstone=tombstone, bundle=bundle)
        doc = BundleDocument.from_bundle(bundle)
        tombstone_doc = BundleTombstoneDocument.from_tombstone(tombstone)
        modified, index_name = doc.entomb(tombstone_doc, dryrun=self.dryrun)
        if self.notify or modified and self.notify is None:
            doc.notify(index_name)
