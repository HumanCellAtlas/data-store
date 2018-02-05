import os
import random
import tempfile
from jsonschema import RefResolver
import json
import subprocess
from dss.storage.validator import S3UrlCache


def resolve_references(schema: dict, resolver: RefResolver) -> dict:
    """
    Inlines all `$ref`s in the JSON-schema. The schema is directly modified.
    Example:
        contents of http://test.com/this.json = {'id': 'test file'}

        schema = {'$ref': 'http://test.com/this.json'}
        resolve_reference(schema, resolver) == {'id': 'test file'}

    :param schema: the JSON schema to use.
    :param resolver: used for resolving the remote and local references
    :return: the schema with `$ref`'s inline.
    """
    ref_url = schema.pop('$ref', '')
    if ref_url:
        identifier, ref = resolver.resolve(ref_url)
        schema.update(ref)
        schema['id'] = identifier

    for value in schema.values():
        if isinstance(value, dict):
            resolve_references(value, resolver)
        elif isinstance(value, list):
            for i in value:
                if isinstance(i, dict):
                    resolve_references(i, resolver)
    return schema


def fake_json(schema: dict) -> dict:
    """
    Generates fake JSON from a given schema.
    """
    with tempfile.TemporaryDirectory() as src_dir:
        schema_file_name = f"{src_dir}/schema.json"
        with open(schema_file_name, 'w') as temp_jsf:
            json.dump(schema, temp_jsf)
        data_file_name = f"{src_dir}/temp.json"
        subprocess.call(["generate-json", schema_file_name, data_file_name])
        with open(data_file_name, 'r') as temp_json:
            return json.load(temp_json)


def resolver_factory(schema: dict=None) -> RefResolver:
    """
    Creates a refResolver with a persistent cache
    :param schema: the root schema used by RefResolver. If not supplied, no root schema is used.
    :return: RefResolver
    """
    cache = S3UrlCache()

    def request_json(url):
        return json.loads(cache.resolve(url).decode("utf-8"))

    if schema:
        resolver = RefResolver.from_schema(schema, handlers={'http': request_json, 'https': request_json})
    else:
        resolver = RefResolver('', '', handlers={'http': request_json, 'https': request_json})
    return resolver


def json_generator(schema: dict, resolver: RefResolver = None) -> dict:
    """
    Generate fake JSON file using a valid JSON schema. This function requires both
    https://github.com/json-schema-faker/json-schema-faker and https://github.com/oprogramador/json-schema-faker-cli
    to be installed.

    :param schema: a dictionary representing the JSON schema to produce fake data from.
    :param resolver: the resolver to use to dereference JSON references.
    :return: fake JSON data based on the schema.
    """
    if not resolver:
        resolver = resolver_factory(schema)
    temp_schema = resolve_references(schema, resolver)
    return fake_json(temp_schema)


class JsonFaker(object):
    """
    Used to generate random JSON from a from a list of URLs containing JSON schemas.
    """
    def __init__(self, schema_urls):
        """
        :param schema_URLs: a list of JSON schema URLs.
        """
        self.schema_urls = schema_urls
        self.resolver = resolver_factory()

    def generate(self) -> str:
        """
        Chooses a random JSON schema from self.path and generates JSON data.
        :return: serialized JSON.
        """
        schema_url = random.choice(self.schema_urls)
        identifier, schema = self.resolver.resolve(schema_url)
        generated_json = json_generator(schema, resolver=self.resolver)
        generated_json.update['id'] = identifier
        return json.dumps(generated_json)
