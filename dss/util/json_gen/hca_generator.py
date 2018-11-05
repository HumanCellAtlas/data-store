import json
import random

from jsonschema import RefResolver

from dss.util.s3urlcache import S3UrlCache
from dss.util.json_gen.generator import JsonGenerator


class HCAJsonGenerator(object):
    """
    Used to generate random JSON from a from a list of URLs containing JSON schemas.
    """
    def __init__(self, schema_urls):
        """
        :param schema_urls: a list of JSON schema URLs.
        """
        self.schemas = dict()
        for url in schema_urls:
            name = url.split('/')[-1]
            self.schemas[name] = {'$ref': url, 'id': url}
        self.cache = S3UrlCache()
        self.resolver = self.resolver_factory()  # The resolver used to dereference JSON '$ref'.
        self._json_gen = JsonGenerator(resolver=self.resolver)

    def generate(self, name: str = None) -> str:
        """
        Chooses a random JSON schema from self.schemas and generates JSON data.
        :param name: the name of a JSON schema to generate. If None, then a random schema is chosen.
        :return: serialized JSON.
        """

        if name is None:
            name = random.choice(list(self.schemas.keys()))
            schema = self.schemas[name]
        else:
            assert name in self.schemas.keys()
            schema = self.schemas[name]
        self.resolve_references(schema)
        generated_json = {name: self._json_gen.generate_json(schema)}
        return json.dumps(generated_json)

    def resolve_references(self, schema: dict) -> dict:
        """
        Inlines all `$ref`s in the JSON-schema. The schema is directly modified.
        Example:
            contents of http://test.com/this.json = {'id': 'test file'}

            schema = {'$ref': 'http://test.com/this.json'}
            self.resolve_reference(schema) == {'id': 'test file'}

        :param schema: the JSON schema to use.
        :return: the schema with `$ref`'s inline.
        """
        ref_url = schema.pop('$ref', '')
        if ref_url:
            identifier, ref = self.resolver.resolve(ref_url)
            schema.update(ref)
            schema['id'] = identifier

        for value in schema.values():
            if isinstance(value, dict):
                self.resolve_references(value)
            elif isinstance(value, list):
                for i in value:
                    if isinstance(i, dict):
                        self.resolve_references(i)
        return schema

    def resolver_factory(self) -> RefResolver:
        """
        Creates a refResolver with a persistent cache
        :return: RefResolver
        """
        def request_json(url):
            return json.loads(self.cache.resolve(url).decode("utf-8"))

        resolver = RefResolver('', '', handlers={'http': request_json, 'https': request_json})
        return resolver
