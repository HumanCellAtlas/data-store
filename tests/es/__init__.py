import logging

from dss.util.es import ElasticsearchClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elasticsearch_delete_index(index_name: str):
    try:
        es_client = ElasticsearchClient.get(logger)
        es_client.indices.delete(index=index_name, ignore=[404])
    except Exception as e:
        logger.warning("Error occurred while removing Elasticsearch index:%s Exception: %s", index_name, e)


# This prevents open socket errors after the test is over.
def close_elasticsearch_connections(es_client):
    for conn in es_client.transport.connection_pool.connections:
        conn.pool.close()
