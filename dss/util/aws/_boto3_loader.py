import typing

class Loader:
    cache = dict(resource={}, client={})  # type: typing.Dict[str, dict]
    def __init__(self, factory):
        self.factory = factory

    def __getattr__(self, attr):
        if attr == "__all__":
            return list(self.cache[self.factory])
        if attr == "__path__" or attr == "__loader__":
            return None
        if attr not in self.cache[self.factory]:
            if self.factory == "client" and attr in self.cache["resource"]:
                self.cache["client"][attr] = self.cache["resource"][attr].meta.client
            else:
                import boto3
                factory = getattr(boto3, self.factory)
                self.cache[self.factory][attr] = factory(attr)
        return self.cache[self.factory][attr]
