import json
import logging
import os

from elasticsearch import Elasticsearch

from dss import Config, ESDocType, ESIndexType, Replica


logger = logging.getLogger(__name__)


class IndexManager:

    @classmethod
    def create_index(cls, es_client: Elasticsearch, replica: Replica, index_name: str):
        if not es_client.indices.exists(index_name):
            with open(os.path.join(os.path.dirname(__file__), "mapping.json"), "r") as fh:
                index_mapping = json.load(fh)
            index_mapping["mappings"][ESDocType.doc.name] = index_mapping["mappings"].pop("doc")
            index_mapping["mappings"][ESDocType.query.name] = index_mapping["mappings"].pop("query")
            alias_name = Config.get_es_alias_name(ESIndexType.docs, replica)
            cls.create_doc_index(es_client, index_name, alias_name, index_mapping)
        else:
            logger.debug(f"Using existing Elasticsearch index: {index_name}")

    @staticmethod
    def create_doc_index(es_client: Elasticsearch, index_name: str, alias_name: str, index_mapping=None):
        try:
            logger.debug(f"Creating new Elasticsearch index: {index_name}")
            response = es_client.indices.create(index_name, body=index_mapping)
            logger.debug("Index creation response: %s", json.dumps(response, indent=4))
        except Exception as ex:
            logger.error(f"Unable to create index: {index_name} Exception: {ex}")
            raise ex
        try:
            logger.debug(f"Aliasing {index_name} as {alias_name}")
            response = es_client.indices.update_aliases({
                "actions": [
                    {"add": {"index": index_name, "alias": alias_name}}
                ]
            })
            logger.debug("Index add alias response: %s", json.dumps(response, indent=4))
        except Exception as ex:
            logger.error(f"Unable to alias index: {index_name} as {alias_name} Exception: {ex}")
            es_client.indices.update_aliases({
                "actions": [
                    {"remove": {"index": index_name, "alias": alias_name}}
                ]
            })
            es_client.indices.delete(index_name)
            raise ex

    @staticmethod
    def get_subscription_index(es_client: Elasticsearch, index_name: str, index_mapping=None):
        try:
            response = es_client.indices.exists(index_name)
            if response:
                logger.debug(f"Using existing Elasticsearch index: {index_name}")
            else:
                logger.debug(f"Creating new Elasticsearch index: {index_name}")
                response = es_client.indices.create(index_name, body=index_mapping)
                logger.debug("Index creation response: %s", json.dumps(response, indent=4))
        except Exception as ex:
            logger.error(f"Unable to create index: {index_name} Exception: {ex}")
            raise ex
