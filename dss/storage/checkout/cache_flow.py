import os
import json
import typing
from dss.config import Replica
from dss.storage.hcablobstore import FileMetadata



def _cache_net():
    with open("checkout_cache.json", "r") as file:
        temp = json.load(file)
        file.close()
        return temp


def lookup_cache(file_metadata: dict):
    for file_type in _cache_net():
        if file_type['type'] == file_metadata[FileMetadata.CONTENT_TYPE]:
            if ["max_size"] >= file_metadata[FileMetadata.SIZE]:
                return True
        else:
            return False
