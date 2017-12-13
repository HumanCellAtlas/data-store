import json
import logging

import os

from dss import Config, ESIndexType, ESDocType, Replica
from dss.util.es import ElasticsearchClient, create_elasticsearch_doc_index


class Index:
    @staticmethod
    def create_elasticsearch_index(index_name: str, replica: Replica, logger: logging.Logger):
        es_client = ElasticsearchClient.get(logger)
        if not es_client.indices.exists(index_name):
            with open(os.path.join(os.path.dirname(__file__), "mapping.json"), "r") as fh:
                index_mapping = json.load(fh)
            index_mapping["mappings"][ESDocType.doc.name] = index_mapping["mappings"].pop("doc")
            index_mapping["mappings"][ESDocType.query.name] = index_mapping["mappings"].pop("query")
            alias_name = Config.get_es_alias_name(ESIndexType.docs, replica)
            create_elasticsearch_doc_index(es_client, index_name, alias_name, logger, index_mapping)
        else:
            logger.debug(f"Using existing Elasticsearch index: {index_name}")
