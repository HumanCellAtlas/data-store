"""
Updates the dynamoDB table that tracks collections.

To only update the table with new uuids from the bucket:
    scripts/update_collection_db.py

To reset the table (delete table and repopulate from bucket) run:
    scripts/update_collection_db.py hard-reset
"""
import os
import sys
import json
from collections import defaultdict

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, Replica
from dss.storage.identifiers import TOMBSTONE_SUFFIX, COLLECTION_PREFIX
from dss.collections import owner_lookup


Config.set_config(BucketConfig.NORMAL)


class CollectionDatabaseTools(object):
    def __init__(self, replica='aws'):
        self.replica = replica
        self.bucket = Replica[replica].bucket
        self.handle = Config.get_blobstore_handle(Replica[replica])

        # all collections in the bucket
        raw_keys = self.handle.list(self.bucket, prefix=COLLECTION_PREFIX)
        # filter for tombstoned collections
        tombstone_uuids = [i[len(f'{COLLECTION_PREFIX}/'):-len(f'.{TOMBSTONE_SUFFIX}')] for i in
                           self.handle.list(self.bucket, prefix=COLLECTION_PREFIX)
                           if i.endswith(f'.{TOMBSTONE_SUFFIX}')]

        self.valid_bucket_uuids = set()
        self.all_bucket_uuids = defaultdict(list)
        for uuid, version in [key[len(f'{COLLECTION_PREFIX}/'):].split('.', 1)
                              for key in raw_keys if key != f'{COLLECTION_PREFIX}/']:
            self.all_bucket_uuids[uuid].append(version)
            if uuid not in tombstone_uuids:
                self.valid_bucket_uuids.add(uuid)

        # TODO: Iterate?
        # Probably only necessary once the collections db reaches 10s of millions
        self.all_database_uuids = set([uuid.split('.', 1)[0] for _, uuid in owner_lookup.get_all_collection_keys()])
        self.uuids_not_in_db = self.valid_bucket_uuids - self.all_database_uuids

        bucket_name = f'{Replica[replica].storage_schema}://{self.bucket}'
        print(f'Found {len(self.valid_bucket_uuids)} valid collections in {bucket_name}.')
        print(f'Found {len(tombstone_uuids)} tombstoned collections in {bucket_name}.')
        print(f'Found {len(self.all_database_uuids)} collections in db table: {owner_lookup.collection_db_table}.')

    def _read_collection_bucket_files_to_database(self, uuids):
        print(f'\nAdding {len(uuids)} user-collection associations to database.\n')
        counter = 0
        for uuid in uuids:
            print(f'{round(counter * 100 / len(uuids), 1)}% Added.')
            for version in self.all_bucket_uuids[uuid]:
                key = f'{COLLECTION_PREFIX}/{uuid}.{version}'
                collection = json.loads(self.handle.get(self.bucket, key))
                owner_lookup.put_collection(owner=collection['owner'], versioned_uuid=f'{uuid}.{version}')
            counter += 1

    @staticmethod
    def _delete_collection_bucket_files_from_database():
        print(f'\nRemoving all user-collection associations from database.\n')
        for owner, versioned_uuid in owner_lookup.get_all_collection_keys():
            print(f"Deleting {owner}'s collection: {versioned_uuid}")
            owner_lookup.delete_collection(owner=owner, versioned_uuid=versioned_uuid)

    def collection_database_update(self):
        """
        Any collection uuids in the replica bucket not already in dynamoDB will be recorded along with their owner.

        Tries to be efficient, since this potentially opens 1000's of files one by one and takes a long time.

        For a hard reset, use collection_database_hard_reset(), which will clear the database and repopulate all
        collections from their bucket files from scratch.

        This will also not remove any entries in the database, since the current behavior is a GET request that
        does not see the file will delete the entry from the database since the file is considered truth.

        TODO: Handle repopulating read-only access once implemented?  Can it be done?
        """
        self._read_collection_bucket_files_to_database(uuids=self.uuids_not_in_db)

    def collection_database_hard_reset(self):
        """
        Clears the database and repopulates all collections from their bucket files from scratch.

        TODO: Is it safe to assume that this will load completely from only one replica?  Or use both?
        TODO: Handle repopulating read-only access once implemented?  Can it be done?
        """
        self._delete_collection_bucket_files_from_database()
        self._read_collection_bucket_files_to_database(uuids=self.valid_bucket_uuids)


def main():
    c = CollectionDatabaseTools(replica='aws')

    if len(sys.argv) > 1:
        assert sys.argv[1] == 'hard-reset', f'Invalid argument: {sys.argv[1]}'
        c.collection_database_hard_reset()
    else:
        c.collection_database_update()


if __name__ == '__main__':
    main()
