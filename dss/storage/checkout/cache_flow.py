import json
from dss.storage.hcablobstore import FileMetadata


def _cache_net():
    with open("checkout_cache_criteria.json", "r") as file:
        temp = json.load(file)
    return temp


def lookup_cache(file_metadata: dict):
    for file_type in _cache_net():
        if file_type['type'] == file_metadata[FileMetadata.CONTENT_TYPE]:
            if file_type['max_size'] >= file_metadata[FileMetadata.SIZE]:
                return True
    return False
