import os
import json
import typing

from dss.dynamodb import get_item, put_item, delete_item


class AsyncStateItem:
    """
    Store and recover json-serializable state into dynamodb
    Subclasses of AsyncStateItem are instantiated with AsyncStateItem.get
    for example:
        class MyAsyncSubclass(AsyncStateItem):
            pass
        foo = MyAsyncSubclass.put("test_key", {})
        bar = AsyncStateItem.get("test_key")
        type(foo) == MyAsyncSubclass  # True
        type(bar) == MyAsyncSubclass  # True
    """
    table = f"dss-async-state-{os.environ['DSS_DEPLOYMENT_STAGE']}"

    def __init__(self, key: str, body: dict) -> None:
        self.key = key
        if not body.get('_type'):
            body['_type'] = type(self).__name__
        self.body = body

    def _put(self) -> typing.Any:
        return put_item(table=self.table, value=json.dumps(self.body), hash_key=self.key)

    @classmethod
    def put(cls, key: str, body: dict = None) -> typing.Any:
        item = cls(key, body if body else dict())
        item._put()
        return item

    @classmethod
    def get(cls, key: str) -> typing.Any:
        item = get_item(table=cls.table, hash_key=key)
        if item:
            body = json.loads(item)
            item_class = _all_subclasses(cls)[body['_type']]
            return item_class(key, body)

    @classmethod
    def delete(cls, key: str) -> None:
        delete_item(table=cls.table, hash_key=key)

    def delete_item(self):
        AsyncStateItem.delete(self.key)


class AsyncStateError(AsyncStateItem, Exception):
    """
    Store an error state into dynamodb
    Errors may be recovered and raised remotely. Example:
        Class MyAsyncError(AsyncStateError):
            pass
        MyAsyncError().put("my_key", "error message")

        possible_error = AsyncStateItem.get("my_key")
        if isinstance(possible_error, MyAsyncError):
            raise possible_error
    """
    @property
    def message(self) -> dict:
        return self.body['message']

    @classmethod
    def put(cls, key: str, message: str):  # type: ignore
        item = cls(key, {"message": message})
        item._put()
        return item


def _all_subclasses(cls):
    classes = {c.__name__: c
               for c in cls.__subclasses__()}
    for c in classes.copy().values():
        classes.update(_all_subclasses(c))
    classes[cls.__name__] = cls
    return classes
