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
import time
from hca.dss import import DSSClient

stage = ""
tombstone_reason = ""
assert stage
assert tombstone_reason

if stage == "prod":
    dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
else:
    dss_client = DSSClient(swagger_url=f"https://dss.{stage}.data.humancellatlas.org/v1/swagger.json")

def tombstone_bundle(uuid, version):
    print(f"tombstoning {uuid} {version}")
    resp = dss_client.delete_bundle(
        replica="aws",
        uuid=uuid,
        version=version,
        reason=tombstone_reason,
    )
    for _ in range(30):
        try:
            print(f"verifying tombstone {uuid} {version}")
            bundle_query_with_version = {'query':{'bool':{'must':[{'match':{'uuid':uuid}},{'match':{'version':version}},]}}}
            resp = dss_client.post_search(replica="aws", es_query=bundle_query_with_version, output_format="raw")
            assert resp['results'][0]['metadata']['admin_deleted']
            break
        except (KeyError, IndexError, AssertionError):
            time.sleep(1)
    else:
        raise Exception(f"Unable to verity tombstone {uuid}")

bundles_to_tombstone = [
    # Place bundles list here
]

for fqid in bundles_to_tombstone:
    uuid, version = fqid.split(".", 1)
    tombstone_bundle(uuid, version)
