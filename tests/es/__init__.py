import logging
from typing import List
from dss.config import IndexSuffix, Config, BucketConfig
from dss.util.es import ElasticsearchClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elasticsearch_delete_index(index_name: str):
    # ensure the indexes are test index.
    assert Config._CURRENT_CONFIG == BucketConfig.TEST
    assert IndexSuffix.name
    assert index_name.endswith(IndexSuffix.name)

    try:
        es_client = ElasticsearchClient.get(logger)
        es_client.indices.delete(index=index_name, ignore=[404])
    except Exception as e:
        logger.warning("Error occurred while removing Elasticsearch index:%s Exception: %s", index_name, e)


# This prevents open socket errors after the test is over.
def close_elasticsearch_connections(es_client):
    for conn in es_client.transport.connection_pool.connections:
        conn.pool.close()


def clear_indexes(index_names: List[str], doctypes: List[str]):
    '''erases all of the documents in indexes with any of the doctypes provided. This can only be used in TEST
    configuration with IndexSuffix.name set. Only indexes with the same IndexSuffix.name can be erased.'''

    # ensure the indexes are test index.
    assert Config._CURRENT_CONFIG == BucketConfig.TEST
    assert IndexSuffix.name
    for index_name in index_names:
        assert index_name.endswith(IndexSuffix.name)

    es_client = ElasticsearchClient.get(logger)
    if es_client.indices.exists(index_names):
        es_client.delete_by_query(index=index_names,
                                  body={'query': {'match_all': {}}},
                                  doc_type=doctypes,
                                  refresh=True,
                                  conflicts='proceed')
