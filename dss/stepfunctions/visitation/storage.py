from abc import ABC, abstractmethod
from collections import defaultdict
import logging
import string
from typing import List, Mapping, MutableMapping, NamedTuple, Optional, Sequence, Tuple, Type, Hashable, Set

from cloud_blobstore import BlobStore, PagedIter

from dss.config import Config, Replica
from dss.util import require
from dss.util.iterators import zipalign
from dss.stepfunctions.visitation import Visitation, WalkerStatus
from dss.vendored.frozendict import frozendict

logger = logging.getLogger(__name__)


# noinspection PyAttributeOutsideInit
class StorageVisitation(Visitation):

    # A central pattern in this class is to use covariant arrays that have one element per replica. For example,
    # `self.buckets[i]` is the custom bucket to be used for the replica whose name is `self.replicas[i]`. Similarly,
    # `self.work_result['missing'][i]` is the number of missing keys in the bucket for the replica whose name is
    # `self.replicas[i]`. While this is not especially readable, it is efficient and fits best with the convention
    # established by the super class which encourages state to be a dict with static keys. Instead of arrays I
    # experimented with dicts keyed on replica name but that turned out to be unwieldy. The covariant array approach
    # allows for the use of zip() to merge two such arrays and iterate over the result. The most readable and
    # efficient pattern would be to use a single dict that maps replica names to objects or dicts representing all
    # replica-specific items, but again, that approach didn't seem to fit well with the superclass.

    shutdown_time = 10

    state_spec = dict(replicas=None,
                      buckets=None,
                      prefix='',
                      folder=None,
                      quick=None,
                      work_result=dict(present=None,
                                       missing=None,
                                       inconsistent=None,
                                       bad_checksum=None,
                                       bad_native_checksum=None))

    prefix_chars = list(set(string.hexdigits.lower()))

    def job_initialize(self):
        folders = folder_classes.keys()
        require(self.folder in folders, f"Folder {self.folder} is not one of {folders}")
        if self._number_of_workers <= 16:
            self.work_ids = [self.prefix + a for a in self.prefix_chars]
        else:
            self.work_ids = [self.prefix + b + a for a in self.prefix_chars for b in self.prefix_chars]
        n = len(self.replicas)
        self.work_result = {k: [0] * n for k in self.work_result.keys()}

    walker_state_spec = dict(tokens=None, row=None)

    def walker_initialize(self) -> None:
        n = len(self.replicas)
        self.row: Tuple[Optional[str], ...] = (None,) * n
        self.tokens = [None] * n

    def walker_walk(self) -> None:
        folder = None if self.quick else folder_classes[self.folder](self)
        columns = []
        handles = []
        for replica, bucket, key, token in zip(self.replicas, self.buckets, self.row, self.tokens):
            replica = Replica[replica]
            if bucket is None:
                bucket = replica.bucket
            elif bucket != replica.bucket:
                logger.warning(f'Checking bucket {bucket} instead of default {replica.bucket} for replica {replica}.')
            handle = Config.get_blobstore_handle(replica)
            handles.append(handle)
            column: PagedIter = handle.list_v2(bucket,
                                               prefix=f'{self.folder}/{self.work_id}',
                                               token=token,
                                               start_after_key=key)
            columns.append(column)

        diff = zipalign(columns=((key for key, metadata in column) for column in columns), row=self.row)
        while self.shutdown_time < self.remaining_runtime():
            try:
                row = next(diff)
            except StopIteration:
                logger.info("Finished checking replicas.")
                self._status = WalkerStatus.finished.name
                break
            else:
                values: Optional[MutableMapping[Hashable, Set[str]]] = None if folder is None else defaultdict(set)
                for i, key in enumerate(row.norm()):
                    replica = self.replicas[i]
                    if key is None:
                        logger.warning(f"Replica {replica} is missing {row.min}")
                        self.work_result['missing'][i] += 1
                    else:
                        logger.debug(f"Replica {replica} contains {key}")
                        self.work_result['present'][i] += 1
                        if values is not None:
                            values[folder.inspect_key(i, handles[i], key)].add(replica)
                if values is not None:
                    assert len(values) > 0
                    if len(values) > 1:
                        logger.warning('Inconsistency detected for key %s: %r', row.min, values)
                        # The `values` dict maps each distinct value occurring in one or more replicas at the current
                        #  key to a set of replicas where that value occured. Ideally there should be one entry in
                        # that mapping, indicating that all replicas hold the same value for the current key. If
                        # there are more entries we'll assume that the entry with the most replicas represents the
                        # ground truth and all other entries represent inconsistent replicas.
                        consistent_replicas = max(values.values(), key=len)
                        for i, replica in enumerate(self.replicas):
                            if replica not in consistent_replicas:
                                self.work_result['inconsistent'][i] += 1
                self.row = row.values
        else:
            self.tokens = [column.token for column in columns]
            logger.debug('Not enough time left in lambda execution, exiting.')

    def walker_finalize(self):
        self.tokens = None
        self.row = None
        logger.info(f'Work result: {self.work_result}')

    def _aggregate(self, work_results: Sequence[Mapping[str, List[int]]]) -> MutableMapping[str, List[int]]:
        aggregate: MutableMapping[str, List[int]] = {}
        for work_result in work_results:
            for name, deltas in work_result.items():
                try:
                    counters = aggregate[name]
                except KeyError:
                    aggregate[name] = list(deltas)
                else:
                    for i, delta in enumerate(deltas):
                        counters[i] += delta
        return aggregate


class KeyInfo(NamedTuple):
    """
    Encapsulates everything we can know about a key in a replica's storage bucket.
    """
    bucket: str
    key: str
    user_metadata: Mapping[str, str]
    size: int
    content_type: str
    cloud_checksum: str


class StorageFolder(ABC):
    """
    Abstracts the specifics of verifying and comparing keys in a particular folder of a replica's storage bucket.
    """

    def __init__(self, visitation: StorageVisitation) -> None:
        super().__init__()
        self.visitation = visitation

    @abstractmethod
    def inspect_key(self, replica: int, handle: BlobStore, key: str) -> Hashable:
        """
        Inspect the object at the given key in the given bucket and return an approximatation of that object that is
        suitable for a comparison between replicas.

        :param replica: the replica to inspect (an index into self.visitation.replicas and self.visitation.buckets)
        :param handle: the BlobStore instance for interacting with the replica
        :param key: the key to inspect
        :return: a hashable approximation of the key, its metadata tags and content
        """
        raise NotImplementedError()

    def _key_info(self, replica, handle, key):
        bucket = self.visitation.buckets[replica]
        return KeyInfo(bucket=bucket,
                       key=key,
                       user_metadata=frozendict(handle.get_user_metadata(bucket, key) or {}),
                       size=handle.get_size(bucket, key),
                       content_type=handle.get_content_type(bucket, key),
                       cloud_checksum=handle.get_cloud_checksum(bucket, key))


class Bundles(StorageFolder):
    def inspect_key(self, replica: int, handle: BlobStore, key: str) -> Hashable:
        # Compare the bundle manifest
        return handle.get(self.visitation.buckets[replica], key)


class Files(StorageFolder):
    def inspect_key(self, replica: int, handle: BlobStore, key: str) -> Hashable:
        # Compare the file metadata, not the file contents. The content is stored as an object in the /blobs folder.
        return handle.get(self.visitation.buckets[replica], key)


class Blobs(StorageFolder):
    # The hash algorithms, in the order the corresponding hash digest appears in each blob name
    hash_algos = ("sha256", "sha1", "s3_etag", "crc32c")

    def inspect_key(self, replica: int, handle: BlobStore, key: str) -> Hashable:
        info = self._key_info(replica, handle, key)
        name_checksums = set(map(str.lower, info.key.split('/')[-1].split('.')))
        _checksums = (info.user_metadata.get('hca-dss-' + algo) for algo in self.hash_algos)
        checksums = set(c.lower() for c in _checksums if c is not None)
        if checksums != name_checksums:
            logger.warning('Name of blob %s conflicts with its checksum tags (%r != %r).',
                           info.key, name_checksums, checksums)
            self.visitation.work_result['bad_checksum'][replica] += 1
        if info.cloud_checksum not in checksums:
            logger.warning('Native checksum of blob %s conflicts with its tags (%s not in %r).',
                           info.key, info.cloud_checksum, checksums)
            self.visitation.work_result['bad_native_checksum'][replica] += 1
        return info


folder_classes: Mapping[str, Type[StorageFolder]] = dict(bundles=Bundles, files=Files, blobs=Blobs)
