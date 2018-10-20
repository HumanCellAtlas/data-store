#!/usr/bin/env python
"""
"""
import os
import hca
import json
import argparse
from concurrent.futures import ThreadPoolExecutor
#
#set -euo pipefail
#
#replica=$1
#list=$2
#
#for uuid in $(cat $list); do
#    hca dss post-search --replica $replica --es-query="{\"query\":{\"match\":{\"uuid\":\"$uuid\"}}}"
#done

swag_url = "https://dss.data.humancellatlas.org/v1/swagger.json"
dss_client = hca.dss.SwaggerClient(swagger_url=swag_url)

def query(replica, key_or_uuid):
    if "/" in key_or_uuid:
        uuid = key_or_uuid.split("/")[1]
    else:
        uuid = key_or_uuid

    if "." in uuid:
        uuid = uuid.split(".")[0]

    q = {
        'query': {
            'match': {
                'uuid': uuid
            }
        }
    }

    resp = dss_client.post_search(
        replica=replica,
        es_query=q,
#        output_format="raw",
    )

    print(resp)
    if resp['total_hits'] < 1:
        print(f"missing {key_or_uuid}")

def query_many(replica, objs):
    with ThreadPoolExecutor(10) as executor:
        executor.map(lambda o: query(replica, o), objs)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replica", choices=["aws", "gcp"])
    parser.add_argument("key_or_file")
    parser.add_argument("-d", "--stage", default="dev", choices=["dev", "integration", "staging", "prod"])
    args = parser.parse_args()

    if os.path.isfile(args.key_or_file):
        with open(args.key_or_file, "r") as fh:
            keys = fh.read().split()
        query_many(args.replica, keys)
    else:
        query(args.replica, args.key_or_file)
        pass
