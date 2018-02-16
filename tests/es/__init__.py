import logging
import socket
import subprocess
import time
from typing import List

import os
import tempfile

from dss.config import BucketConfig, Config
from dss.index.es import ElasticsearchClient
from dss.util import networking

logger = logging.getLogger(__name__)


def elasticsearch_delete_index(index_name: str):
    # ensure the indexes are test index.
    assert Config._CURRENT_CONFIG == BucketConfig.TEST
    assert Config.test_index_suffix.value
    assert index_name.endswith(Config.test_index_suffix.value)

    try:
        es_client = ElasticsearchClient.get()
        es_client.indices.delete(index=index_name, ignore=[404])
    except Exception as e:
        logger.warning("Error occurred while removing Elasticsearch index:%s Exception: %s", index_name, e)


# This prevents open socket errors after the test is over.
def close_elasticsearch_connections(es_client):
    for conn in es_client.transport.connection_pool.connections:
        conn.pool.close()


def clear_indexes(index_names: List[str], doctypes: List[str]):
    """
    Erases all of the documents in indexes with any of the doctypes provided. This can only be used in TEST
    configuration with IndexSuffix.name set. Only indexes with the same IndexSuffix.name can be erased.
    """
    # ensure the indexes are test index.
    assert Config._CURRENT_CONFIG == BucketConfig.TEST
    assert Config.test_index_suffix.value
    for index_name in index_names:
        assert index_name.endswith(Config.test_index_suffix.value)

    es_client = ElasticsearchClient.get()
    if es_client.indices.exists(index_names):
        es_client.delete_by_query(index=index_names,
                                  body={'query': {'match_all': {}}},
                                  doc_type=doctypes,
                                  refresh=True,
                                  conflicts='proceed')


class ElasticsearchServer:
    def __init__(self, timeout: float=60, delay: float=10) -> None:
        elasticsearch_binary = os.getenv("DSS_TEST_ES_PATH", "elasticsearch")
        tempdir = tempfile.TemporaryDirectory()

        # Set Elasticsearch's initial and max heap to 1.6 GiB, 40% of what's available on Travis, according to
        # guidance from https://www.elastic.co/guide/en/elasticsearch/reference/current/heap-size.html
        env = dict(os.environ, ES_JAVA_OPTIONS="-Xms1638m -Xmx1638m")

        # Work around https://github.com/travis-ci/travis-ci/issues/8408
        if '_JAVA_OPTIONS' in env:  # no coverage
            logger.warning("_JAVA_OPTIONS is set. This may override the options just set via ES_JAVA_OPTIONS.")

        port = networking.unused_tcp_port()
        transport_port = networking.unused_tcp_port()

        args = [elasticsearch_binary,
                "-E", f"http.port={port}",
                "-E", f"transport.tcp.port={transport_port}",
                "-E", f"path.data={tempdir.name}",
                "-E", "logger.org.elasticsearch=" + ("info" if Config.debug_level() > 0 else "warn")]
        logger.info("Running %r with environment %r", args, env)
        proc = subprocess.Popen(args, env=env)

        def check():
            status = proc.poll()
            if status is not None:
                tempdir.cleanup()
                raise ChildProcessError('ES process died with status {status}')

        deadline = time.time() + timeout
        while True:
            check()
            time.sleep(delay)
            check()
            logger.info('Attempting to connect to ES instance at 127.0.0.1:%i', port)
            try:
                sock = socket.create_connection(("127.0.0.1", port), 1)
            except (ConnectionRefusedError, socket.timeout):
                logger.debug('Failed connecting to ES instance at 127.0.0.1:%i', port, exc_info=True)
                if time.time() + delay > deadline:
                    proc.kill()
                    tempdir.cleanup()
                    raise
            else:
                sock.close()
                check()
                self.port = port
                self.proc = proc
                self.tempdir = tempdir
                break

    def shutdown(self) -> None:
        self.proc.kill()
        self.proc.wait()
        self.tempdir.cleanup()
