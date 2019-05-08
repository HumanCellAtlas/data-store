#!/usr/bin/env python
"""
Updates the dynamoDB table that tracks collections.

To update the table run (slow):
    scripts/update_collection_db.py

CAUTION: Doing a hard-reset will break some collections usage during the time it is updating.
To run a hard reset on the table (delete table and repopulate from bucket):
    scripts/update_collection_db.py hard-reset

Tries to be efficient, since this potentially opens 1000's of files one by one and takes a long time.
"""
import os
import sys
import json
import time
import argparse
import textwrap
from cloud_blobstore import BlobNotFoundError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, Replica
from dss.storage.identifiers import TOMBSTONE_SUFFIX, COLLECTION_PREFIX, CollectionFQID
from dss.collections import owner_lookup
from dss.dynamodb import DynamoDBItemNotFound


Config.set_config(BucketConfig.NORMAL)


def heredoc(template, indent=''):
    template = textwrap.dedent(template)
    return template.replace('\n', '\n' + indent) + '\n'


class CollectionDatabaseTools(object):
    def __init__(self, replica='aws'):
        self.replica = replica
        self.bucket = Replica[replica].bucket
        self.handle = Config.get_blobstore_handle(Replica[replica])
        self.bucket_name = f'{Replica[replica].storage_schema}://{self.bucket}'
        self.tombstone_cache = dict()
        self.tombstone_cache_max_len = 100000

        self.total_database_collection_items = 0
        self.total_bucket_collection_items = 0
        self.total_tombstoned_bucket_collection_items = 0

    def _is_uuid_tombstoned(self, uuid: str):
        if len(self.tombstone_cache) >= self.tombstone_cache_max_len:
            self.tombstone_cache.popitem()

        if uuid not in self.tombstone_cache:
            try:
                self.tombstone_cache[uuid] = self.handle.get(self.bucket,
                                                             key=f'{COLLECTION_PREFIX}/{uuid}.{TOMBSTONE_SUFFIX}')
            except BlobNotFoundError:
                self.tombstone_cache[uuid] = None
        return self.tombstone_cache[uuid]

    def _collections_in_database_but_not_in_bucket(self):
        """
        Determines collection items in the table that:
        1. No longer exist in the bucket.
        2. Are tombstoned in the bucket.
        3. Have an owner that doesn't match the owner found in the bucket's collection file.

        Returns an iterable tuple of strings: (owner, collection_fqid) representing the item's key pair.

        The returned keys can then be removed from the collections dynamodb table.
        """
        for owner, collection_fqid in owner_lookup.get_all_collection_keys():
            self.total_database_collection_items += 1
            collection = CollectionFQID.from_key(f'{COLLECTION_PREFIX}/{collection_fqid}')
            try:
                collection_owner = json.loads(self.handle.get(self.bucket, collection.to_key()))['owner']

                assert not self._is_uuid_tombstoned(collection.uuid)
                assert collection_owner == owner

            except BlobNotFoundError:
                yield owner, collection_fqid

            except AssertionError:
                yield owner, collection_fqid

    def _collections_in_bucket_but_not_in_database(self):
        """
        Returns any (owner, collection_fqid) present in the bucket but not in the collections table.

        Returns an iterable tuple of strings: (owner, collection_fqid) representing the item's key pair.

        The returned keys can then be added to the collections dynamodb table.
        """
        for collection_key in self.handle.list(self.bucket, prefix=f'{COLLECTION_PREFIX}/'):
            self.total_bucket_collection_items += 1
            collection_fqid = CollectionFQID.from_key(collection_key)
            if not self._is_uuid_tombstoned(collection_fqid.uuid):
                try:
                    collection = json.loads(self.handle.get(self.bucket, collection_key))
                    try:
                        owner_lookup.get_collection(owner=collection['owner'], collection_fqid=str(collection_fqid))
                    except DynamoDBItemNotFound:
                        yield collection['owner'], str(collection_fqid)
                except BlobNotFoundError:
                    pass  # if deleted from bucket while being listed
                except KeyError:
                    pass  # unexpected response
            else:
                self.total_tombstoned_bucket_collection_items += 1

    def remove_collections_from_database(self, all: bool=False):
        if all:
            collections = owner_lookup.get_all_collection_keys()
            text = 'ALL'
        else:
            collections = self._collections_in_database_but_not_in_bucket()
            text = 'INVALID'

        print(f'\nRemoving {text} user-collection associations from database: {owner_lookup.collection_db_table}\n')
        removed = 0
        for owner, collection_fqid in collections:
            print(f"Removing {owner}'s collection: {collection_fqid}")
            owner_lookup.delete_collection(owner=owner, collection_fqid=collection_fqid)
            removed += 1
        print(f'{removed} collection items removed from: {owner_lookup.collection_db_table}')
        return removed

    def add_missing_collections_to_database(self):
        print(f'\nAdding missing user-collection associations to database from: {self.bucket_name}\n')
        added = 0
        for owner, collection_fqid in self._collections_in_bucket_but_not_in_database():
            print(f"Adding {owner}'s collection: {collection_fqid}")
            owner_lookup.put_collection(owner=owner, collection_fqid=collection_fqid)
            added += 1
        print(f'{added} collection items added from: {self.bucket_name}')
        return added


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description=heredoc("""
        Updates the dynamoDB table that tracks collections.  It's fairly slow, though it tries to be efficient.
        
        The current bottleneck in speed is that in order to repopulate from a bucket, each file needs to be 
        opened one by one to determine ownership.

        To update the table run:
            scripts/update_collection_db.py
        
        CAUTION: See the "--hard-reset" option below for the full purge option.
        """), formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--hard-reset', dest="hard_reset", default=False, required=False, action='store_true',
                        help='CAUTION: Breaks some collections usage during the time it is updating.\n'
                             'This deletes the entire table and then repopulates from the bucket files.')
    o = parser.parse_args(argv)
    start = time.time()
    c = CollectionDatabaseTools(replica='aws')

    if o.hard_reset:
        removed = c.remove_collections_from_database(all=True)
    else:
        removed = c.remove_collections_from_database()

    print(f'Removal took: {time.time() - start} seconds.')

    added = c.add_missing_collections_to_database()

    print(f'Database had: {c.total_database_collection_items} items.')
    print(f'Bucket had  : {c.total_bucket_collection_items} items.')
    print(f'Of which    : {c.total_tombstoned_bucket_collection_items} items were tombstoned.')
    print(f'From bucket: {c.bucket_name} to dynamodb table: {owner_lookup.collection_db_table}')
    print(f'{removed} collection items were removed.')
    print(f'{added} collection items were added.')
    print(f'Collections Database Updated Successfully in {time.time() - start} seconds.')


if __name__ == '__main__':
    main()
