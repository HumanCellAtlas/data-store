import logging

from elasticsearch import Elasticsearch


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Check if the Elasticsearch service is running,
# and if not, raise and exception with instructions to start it.
def check_start_elasticsearch_service():
    try:
        es_client = Elasticsearch()
        es_info = es_client.info()
        logger.debug("The Elasticsearch service is running.")
        logger.debug("Elasticsearch info: %s", es_info)
        close_elasticsearch_connections(es_client)
    except Exception:
        raise Exception("The Elasticsearch service does not appear to be running on this system, "
                        "yet it is required for this test. Please start it by running: elasticsearch")


def elasticsearch_delete_index(index_name: str):
    try:
        es_client = Elasticsearch()
        es_client.indices.delete(index=index_name, ignore=[404])
        close_elasticsearch_connections(es_client)  # Prevents end-of-test complaints about open sockets
    except Exception as e:
        logger.warning("Error occurred while removing Elasticsearch index:%s Exception: %s", index_name, e)


# This prevents open socket errors after the test is over.
def close_elasticsearch_connections(es_client):
    for conn in es_client.transport.connection_pool.connections:
        conn.pool.close()
