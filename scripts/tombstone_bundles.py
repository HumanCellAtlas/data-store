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
from traceback import format_exc
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
    bundle_found = _bundle_found(uuid, version)
    bundle_already_tombstoned = _bundle_tombstoned(uuid, version)
    if bundle_found and not bundle_already_tombstoned:
        resp = dss_client.delete_bundle(
            replica="aws",
            uuid=uuid,
            version=version,
            reason=tombstone_reason,
        )
        for _ in range(30):
            if _bundle_tombstoned(uuid, version):
                break
            else:
                time.sleep(1)
        else:
            raise Exception(f"Unable to verity tombstone {uuid}")
    return bundle_found, bundle_already_tombstoned, fqid

def _bundle_found(uuid, version):
    try:
        resp = dss_client.get_bundle(replica="aws", uuid=uuid, version=version)
        return True
    except SwaggerAPIException as e:
        if 404 == e.code:
            return False
        else:
            raise

def _bundle_tombstoned(uuid, version):
    bundle_query_with_version = {"query":{"bool":{"must":[{"match":{"uuid":uuid}},{"match":{"version":version}},]}}}
    resp = dss_client.post_search(replica="aws", es_query=bundle_query_with_version, output_format="raw")
    if 0 == resp['total_hits']:
        return False
    elif 'admin_deleted' in resp['results'][0]['metadata']:
        return True
    else:
        return False

bundles_to_tombstone = [
    # bundles fqid list
]

with ThreadPoolExecutor(max_workers=10) as e:
    futures = list()
    for fqid in bundles_to_tombstone:
        futures.append(e.submit(tombstone_bundle, fqid))
    for f in as_completed(futures):
        try:
            bundle_found, bundle_already_tombstoned, fqid = f.result()
            if bundle_already_tombstoned:
                bundle_status = "ALLREADY_TOMBSTONED"            
            elif not bundle_found:
                bundle_status = "NOT_FOUND"
            else:
                bundle_status = "TOMBSTONED"            
            print(bundle_status, fqid)
        except SwaggerAPIException as e:
            if 403 == e.code: # bail out on the first credentials denial
                print(e)
                sys.exit(1)
        except Exception as e:
            sys.stderr.write(format_exc())
