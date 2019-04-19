import io
import os
import bz2
import json
import inspect

from cloud_blobstore import BlobNotFoundError

from dss import Config, Replica


class _JSONCache:
    path_template = "/tmp/cache-test-{}"

    def __init__(self, *, cache_argument):
        self.cache_argument = cache_argument

    def _write(self, key, data):
        raise NotImplementedError()

    def _read(self, key):
        raise NotImplementedError()

    def _del(self, key):
        raise NotImplementedError()

    def _make_key(self, args, kwargs):
        if self.cache_argument in self.parms:
            i = [p for p in self.parms].index(self.cache_argument)
            key = args[i]
        else:
            key = kwargs[self.cache_argument]
        return key

    def __call__(self, func):
        self.parms = inspect.signature(func).parameters
        def wrapped(*args, **kwargs):
            key = self._make_key(args, kwargs)
            data = self._read(key)
            if data is None:
                data = func(*args, **kwargs)
                self._write(key, bz2.compress(json.dumps(data).encode("utf-8")))
            return json.loads(bz2.decompress(data).decode("utf-8"))
        return wrapped

class JSONCacheToFiles(_JSONCache):
    def _make_key(self, args, kwargs):
        key = super()._make_key(args, kwargs)
        return key.replace("/", ".")

    def _write(self, key, data):
        with open(self.path_template.format(key), "wb") as fh:
            fh.write(data)

    def _read(self, key):
        path = self.path_template.format(key)
        if os.path.isfile(path):
            with open(path, "rb") as fh:
                return fh.read()
        else:
            return None

class JSONCacheToS3(_JSONCache):
    def __init__(self, *, cache_argument, bucket, prefix):
        super().__init__(cache_argument=cache_argument)
        self.bucket = bucket
        self.prefix = prefix
        self.handle = Config.get_blobstore_handle(Replica.aws)

    def _make_key(self, *args, **kwargs):
        key = super()._make_key(*args, **kwargs)
        return f"{self.prefix}{key}"

    def _write(self, key, data):
        with io.BytesIO(data) as fh:
            self.handle.upload_file_handle(self.bucket, key, fh)

    def _read(self, key):
        try:
            return self.handle.get(self.bucket, key)
        except BlobNotFoundError:
            return None
