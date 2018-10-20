#!/usr/bin/env python

import os
from dss.index.es import ElasticsearchClient
from elasticsearch_dsl import Search

os.environ['DSS_ES_ENDPOINT'] = "search-dss-index-integration-vx3ftz6xjooc2vr3ayd7qeyh3m.us-east-1.es.amazonaws.com"
#os.environ['DSS_ES_ENDPOINT'] = "search-dss-index-dev-wc7kz2vuj7mpfjbxnp4ftplpuu.us-east-1.es.amazonaws.com"
es_client = ElasticsearchClient.get()

def find_subscriptions(replica="aws", owner=None):
    search_obj = Search(using=es_client,
                        index=f"dss-integration-{replica}-subscriptions",
                        doc_type="subscription")

    if owner is None:
        search = search_obj.query({"match_all": {}})
    else:
        search = search_obj.query({'bool': {'must': [{'term': {'owner': owner}}]}})

    for hit in search.scan():
        print(hit.meta.id, hit.to_dict().get('owner', None))

for replica in ["aws", "gcp"]:
    print()
    print(f"All {replica} subscriptions:")
    find_subscriptions(replica)

    print()
    print(f"My {replica} subscriptions:")
    find_subscriptions(replica, "bhannafi@ucsc.edu")
