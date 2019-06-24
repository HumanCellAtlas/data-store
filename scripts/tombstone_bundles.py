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

stage = "dev"
tombstone_reason = "testing"
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
    "3bfec149-c92d-4769-85a0-044b7b22979e.2019-06-24T141732.340450Z",
    "67e984df-5207-4666-bfe8-8dbadbbaff10.2019-06-24T135130.200679Z",
    "746cdb29-eae5-4f9e-b5dd-4c09e952a774.2019-06-24T142206.926728Z",
    "cf16d40c-11e6-41ca-9302-84a19bd0f549.2019-06-24T143214.239611Z",
    "d40135d5-c7ec-4508-8b33-1575985c614a.2019-06-24T143625.506479Z",
    "285ecfaa-b7f5-4c97-819e-cb9dec8fa584.2019-06-24T144152.785863Z",
    "1c26ea90-a2f2-447b-88ca-708ca1e9fbb7.2019-06-24T144606.217829Z",
    "b27c1f20-ad63-4bfb-a861-1d0a15d7f7cb.2019-06-24T145741.849241Z",
    "e97afb85-827c-4f3d-a650-acdf46cb4a70.2019-06-24T145750.816643Z",
    "c58495b9-aff6-4a84-896a-dfef6d33ba4a.2019-06-24T145805.291749Z",
    "bdce2a6d-e46b-4b81-8752-98e170a2c465.2019-06-24T145813.718122Z",
    "48e8ef92-94b9-4a8a-b3ab-abf6f9dee976.2019-06-24T145822.229287Z",
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
