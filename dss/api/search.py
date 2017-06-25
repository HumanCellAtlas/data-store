import json
import os

from elasticsearch_dsl import Search
from flask import request

from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE
from .. import get_logger
from ..util import connect_elasticsearch


def list():
    es = connect_elasticsearch(os.getenv("DSS_ES_ENDPOINT"), get_logger())  # TODO Use a connection manager
    get_logger().debug("Searching for: %s", request.values["query"])

    query = json.loads(request.values["query"])
    s = Search(using=es, index=DSS_ELASTICSEARCH_INDEX_NAME, doc_type=DSS_ELASTICSEARCH_DOC_TYPE)\
        .query("match", **query)
    response = s.execute()
    # TODO Format output
    results = [{"id": hit.meta.id, "score": hit.meta.score} for hit in response]
    return {"query": query, "results": results}
