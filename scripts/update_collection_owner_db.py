import os
import sys
import json

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config, Replica
from dss.collections import get_all_collection_uuids, put_collection, delete_collection

Config.set_config(BucketConfig.NORMAL)


class CollectionDatabaseTools(object):
    def __init__(self, replica='aws'):
        self.replica = replica
        self.bucket = Replica[replica].bucket
        self.handle = Config.get_blobstore_handle(Replica[replica])

        raw_keys = self.handle.list(self.bucket, prefix='collections')
        tombstone_uuids = [i[len('collections/'):-len('.dead')] for i in
                           self.handle.list(self.bucket, prefix='collections') if i.endswith('.dead')]

        self.key_set = {}
        # only one key per uuid, since different versions should have the same owner and we don't need to open them all
        for uuid, version in [key[len('collections/'):].split('.', 1) for key in raw_keys]:
            # also filter for tombstones
            if uuid not in self.key_set and uuid not in tombstone_uuids:
                self.key_set[uuid] = version

        self.all_bucket_uuids = set(self.key_set.keys())
        self.all_database_uuids = set(get_all_collection_uuids())
        self.uuids_not_in_db = self.all_bucket_uuids.symmetric_difference(self.all_database_uuids)

        bucket_name = f'{Replica[replica].storage_schema}://{self.bucket}'
        print(f'Found {str(len(self.all_bucket_uuids))} collections in {bucket_name}.')
        print(f'Found {str(len(tombstone_uuids))} tombstoned collections in {bucket_name}.')
        print(f'Found {str(len(self.all_database_uuids))} collections in database.')

    def _read_collection_bucket_files_to_database(self, uuids):
        print(f'Adding {str(len(uuids))} user-collection associations to database.\n')
        counter = 0
        for uuid in uuids:
            print(f'{str(round(counter * 100 / len(self.uuids_not_in_db), 1))}% Added.')
            key = f'collections/{uuid}.{self.key_set[uuid]}'
            collection = json.loads(self.handle.get(self.bucket, key))
            put_collection(owner=collection['owner'], uuid=uuid, permission_level='owner')
            counter += 1

    def _delete_collection_bucket_files_from_database(self, uuids):
        print(f'Removing {str(len(uuids))} user-collection associations to database.\n')
        counter = 0
        for uuid in self.all_bucket_uuids:
            print(f'{str(round(counter * 100 / len(self.uuids_not_in_db), 1))}% Deleted.')
            key = f'collections/{uuid}.{self.key_set[uuid]}'
            collection = json.loads(self.handle.get(self.bucket, key))
            delete_collection(owner=collection['owner'], uuid=uuid)
            counter += 1

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
        self._delete_collection_bucket_files_from_database(uuids=self.all_database_uuids)
        self._read_collection_bucket_files_to_database(uuids=self.all_bucket_uuids)


if __name__ == '__main__':
    c = CollectionDatabaseTools(replica='aws')
    # TODO: Add args to switch between the two.
    # c.collection_database_hard_reset()
    c.read_collection_bucket_files_to_database()
