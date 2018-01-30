import os
import random
import tempfile
from jsonschema import RefResolver
import json
import subprocess
import logging
from dss.storage.validator import S3UrlCache

logger = logging.getLogger(__name__)


def resolve_references(schema: dict, resolver: RefResolver) -> dict:
    """
    Inlines all `$ref`s in the json-schema. The schema is directly modified.
    :param schema: the json schema to use.
    :param resolver: used for resolving the remote and local references
    :return: the schema with `$ref`'s inline.
    """
    ref = schema.pop('$ref', None)
    if ref:
        ref = resolver.resolve(ref)[1]
        schema.update(ref)

    keys = [key for key in schema.keys() if isinstance(schema[key], (list, dict))]
    for key in keys:
        if isinstance(schema[key], dict):
            resolve_references(schema[key], resolver)
        else:
            for i in schema[key]:
                if isinstance(i, dict):
                    resolve_references(i, resolver)
    return schema


def faker_json(schema: dict) -> dict:
    """Generates fake json from a dictionary of json"""
    with tempfile.TemporaryDirectory() as src_dir:
        schema_file_name = f"{src_dir}/schema.json"
        with open(schema_file_name, 'w') as temp_jsf:
            json.dump(schema, temp_jsf)
        data_file_name = f"{src_dir}/temp.json"
        subprocess.call(["generate-json", schema_file_name, data_file_name])
        with open(data_file_name, 'r') as temp_json:
            return json.load(temp_json)


def s3resolver_factory(schema: dict=None):
    """
    Creates a refResolver with an S3UrlCache
    :param schema: the referrer schema used by RefResolver
    :return: RefResolver
    """
    cache = S3UrlCache(logger)

    def request_json(url):
        return json.loads(cache.resolve(url).decode("utf-8"))

    if schema:
        resolver = RefResolver.from_schema(schema, handlers={'http': request_json, 'https': request_json})
    else:
        resolver = RefResolver('', '', handlers={'http': request_json, 'https': request_json})
    return resolver


def json_generator(schema: dict, resolver: RefResolver = None) -> dict:
    """
    Generate fake json file using a valid json schema. This function requires both
    https://github.com/json-schema-faker/json-schema-faker and https://github.com/oprogramador/json-schema-faker-cli
    to be installed.

    :param schema: a dictionary representing the json schema to produce fake data from.
    :param resolver: the resolver to use to dereference json references.
    :return: fake json data based on the schema.
    """
    if not resolver:
        resolver = s3resolver_factory(schema)
    temp_schema = resolve_references(schema, resolver)
    return faker_json(temp_schema)


class JsonFaker(object):
    """Used to generate random json from a from a folder containing json schemas."""
    def __init__(self, path):
        """
        :param path: the file path to a folder containing *.json schemas.
        """
        self.schema_files = [schema for schema in os.listdir(path) if schema.endswith('.json')]
        self.path = path

    def generate(self) -> str:
        """
        Chooses a random json schema from self.path and generates json data.
        :return: Json date in string form.
        """
        schema_file = random.choice(self.schema_files)
        with open(f"{self.path}/{schema_file}", 'r') as json_file:
            schema = json.load(json_file)
        generated_json = json_generator(schema)
        return json.dumps(generated_json)
