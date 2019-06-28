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
import json
from random import randint
from traceback import format_exc
from concurrent.futures import ThreadPoolExecutor, as_completed

from hca.dss import DSSClient
from hca.util.exceptions import SwaggerAPIException

stage = ""
tombstone_reason = ""
assert stage
assert tombstone_reason

BATCH_SIZE = 1000

def _get_dss_client():
    if stage == "prod":
        dss_client = DSSClient(swagger_url="https://dss.data.humancellatlas.org/v1/swagger.json")
    else:
        dss_client = DSSClient(swagger_url=f"https://dss.{stage}.data.humancellatlas.org/v1/swagger.json")
    return dss_client
dss_client = _get_dss_client()

def tombstone_bundle(fqid):
    uuid, version = fqid.split(".", 1)
    bundle_found = bundle_is_found(uuid, version)
    bundle_already_tombstoned = bundle_is_tombstoned(uuid, version)
    if bundle_found and not bundle_already_tombstoned:
        resp = dss_client.delete_bundle(
            replica="aws",
            uuid=uuid,
            version=version,
            reason=tombstone_reason,
        )
        for _ in range(20):
            if bundle_is_tombstoned(uuid, version):
                break
            else:
                time.sleep(randint(1,3))
        else:
            raise Exception(f"Unable to verity tombstone {uuid}")
    return bundle_found, bundle_already_tombstoned, fqid

def bundle_is_found(uuid, version):
    try:
        resp = dss_client.get_bundle(replica="aws", uuid=uuid, version=version)
        return True
    except SwaggerAPIException as e:
        if 404 == e.code:
            return False
        else:
            raise

def bundle_is_tombstoned(uuid, version):
    bundle_query_with_version = {"query":{"bool":{"must":[{"match":{"uuid":uuid}},{"match":{"version":version}},]}}}
    resp = dss_client.post_search(replica="aws", es_query=bundle_query_with_version, output_format="raw")
    if 0 == resp['total_hits']:
        return False
    elif 'admin_deleted' in resp['results'][0]['metadata']:
        return True
    else:
        return False

def bundle_status_string(bundle_found, bundle_already_tombstoned):
    if bundle_already_tombstoned:
        return "ALLREADY_TOMBSTONED"
    elif not bundle_found:
        return "NOT_FOUND"
    else:
        return "TOMBSTONED"

if __name__ == "__main__":
    bundles_to_tombstone = [
        # bundles fqid list
    ]
    
    for i in range(0, len(bundles_to_tombstone), BATCH_SIZE):
        dss_client = _get_dss_client()
        fqids_batch = bundles_to_tombstone[i:i+BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=20) as e:
            futures = [e.submit(tombstone_bundle, fqid) for fqid in fqids_batch]
            for f in as_completed(futures):
                try:
                    bundle_found, bundle_already_tombstoned, fqid = f.result()
                except SwaggerAPIException as e:
                    if 403 == e.code: # bail out on the first credentials denial
                        sys.stderr.write(format_exc())
                        sys.exit(1)
                except Exception:
                    sys.stderr.write(format_exc())
                else:
                    status_string = bundle_status_string(bundle_found, bundle_already_tombstoned)
                    print(status_string, fqid)
                    sys.stdout.flush()
