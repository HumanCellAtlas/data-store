#!/usr/bin/env python
"""
This script will tombstone the bundles in `bundles_to_tombstone`
Usage:
    - edit `stage` to target deployment.
    - edit `bundles_to_tombstone` to contain the bundles to tombstone.
    - edit `tombstone_reason` with explanation for tombstoning
    - Place the edited script in a GitHub gist
    - Post gist link to Slack in #dcp-ops asking for review and verification that these bundles should be tonbstoned
    - If given the go-ahead, execute script
    - Monitor output. Announce errors in #dcp-ops, ask for help in #data-store-eng
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from hca.dss import DSSClient
from hca.util.exceptions import SwaggerAPIException

stage = ""
tombstone_reason = ""
assert stage
assert tombstone_reason

if stage == "prod":
    dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
else:
    dss_client = DSSClient(swagger_url=f"https://dss.{stage}.data.humancellatlas.org/v1/swagger.json")

def tombstone_bundle(fqid):
    uuid, version = fqid.split(".", 1)
    bundle_status = _bundle_status(uuid, version)
    if "FOUND" == bundle_status:
        resp = dss_client.delete_bundle(
            replica="aws",
            uuid=uuid,
            version=version,
            reason=tombstone_reason,
        )
        for _ in range(30):
            try:
                bundle_status = _bundle_status(uuid, version)
                if "TOMBSTONED" == bundle_status:
                    break
            except (KeyError, IndexError, AssertionError):
                time.sleep(1)
        else:
            raise Exception(f"Unable to verity tombstone {uuid}")
    return bundle_status, fqid

def _bundle_status(uuid, version):
    bundle_query_with_version = {'query':{'bool':{'must':[{'match':{'uuid':uuid}},{'match':{'version':version}},]}}}
    resp = dss_client.post_search(replica="aws", es_query=bundle_query_with_version, output_format="raw")
    if 0 == resp['total_hits']:
        return "NOT_FOUND"
    elif 'admin_deleted' in resp['results'][0]['metadata']:
        return "TOMBSTONED"
    return "FOUND"

bundles_to_tombstone = [
    # bundles fqid list
]

with ThreadPoolExecutor(max_workers=10) as e:
    futures = list()
    for fqid in bundles_to_tombstone:
        futures.append(e.submit(tombstone_bundle, fqid))
    for f in as_completed(futures):
        try:
            bundle_status, fqid = f.result()
            print(bundle_status, fqid)
        except Exception as e:
            sys.stderr.write(str(e))
