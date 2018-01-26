import random
from faker import Faker
from jsonschema import RefResolver
import unittest


UNBOUND_MIN_ITEMS = 0
UNBOUND_MAX_ITEMS = 16

UNBOUND_MIN_INT = -32000
UNBOUND_MAX_INT = 32000

UNBOUND_MIN_STRING = 0
UNBOUND_MAX_STRING = 128

UNBOUND_MIN_OBJECTS = 0
UNBOUND_MAX_OBJECTS = 16


class JsonDataGenerator(object):
    '''Generate a random json document based of the provided schema.'''
    _default_format_generators = {
        'date-time': 'iso8601',
        'date': 'date',
        'time': 'time',
        'email': 'email'
    }

    def __init__(self, schema: dict, resolver: RefResolver=None, formats: dict=None, faker: Faker=None):
        '''
        :param schema: the json schema to generate data from.
        :param resolver: used to resolved '$ref' within the schema.
        :param formats: replaces default_format_generators for determining the type of strings to generate. Must be a
        dict with keys associated with json string formats, and items as strings associated with a faker provider.
        :param faker: a faker object for producing fake data.
        '''
        self.schema = schema
        self.faker = faker if faker else Faker()
        self.formats = formats if formats else self._default_format_generators
        for value in self.formats.values():
            if not getattr(self.faker, value):
                raise KeyError(f"{value} provider not an attribute of Faker.")
        self.resolver = resolver if resolver else RefResolver.from_schema(schema)
        self.path = []

    def generate_json(self, schema=None) -> dict:
        if schema is None:
            schema = self.schema
        impostor = self._gen_json(schema)
        return impostor

    def _gen_json(self, schema):
        scope = schema.get(u"id")
        if scope:
            self.resolver.push_scope(scope)
        try:
            # should be inside an object now other wise there should be a ref.
            ref = schema.get(u"$ref")
            json_type = schema.get(u"type",
                                   "object")  # this is temporary until core is removed.
            if ref is not None:
                impostor = self._ref(ref)
            else:
                impostor = self.generators.get(json_type)(self, schema)

        finally:
            if scope:
                self.resolver.pop_scope()
        return impostor

    def _ref(self, r):
        resolve = getattr(self.resolver, "resolve", None)
        if resolve is None:
            with self.resolver.resolving(r) as resolved:
                impostor = self._gen_json(resolved)
        else:
            scope, resolved = self.resolver.resolve(r)
            self.resolver.push_scope(scope)
            try:
                impostor = self._gen_json(resolved)
            finally:
                self.resolver.pop_scope()
        return impostor

    def _common(self, schema: dict):
        fake = schema.get('fake')
        impostor = schema.get('const')
        enums = schema.get('enum')
        if fake:
            impostor = getattr(self.faker, fake)()
        elif not impostor and enums:
            impostor = random.choice(enums)
        return impostor

    def _object(self, schema: dict) -> dict:
        # object_attributes = ['maxProperties', 'minProperties', 'required', 'properties', 'patternProperties',
        #                      'additionalProperties', 'dependencies', 'propertyNames']]

        maximum = schema.get('maxProperties', UNBOUND_MAX_OBJECTS)
        minimum = schema.get('minProperties', UNBOUND_MIN_OBJECTS)
        required = schema.get('required', [])
        properties = schema.get('properties')
        if properties:
            properties = list(schema['properties'].keys())
        make_properties = random.randrange(minimum, maximum)
        impostor = {}
        # create required
        for j_object in required:
            self.path.append(j_object)
            impostor[j_object] = self._gen_json(schema['properties'][j_object])
            properties.remove(j_object)
            self.path.pop()
        # create properties within minProperties and maxProperties or properties is empty

        if len(impostor) < make_properties:
            options = []
            if properties:
                options.append('pr')
            pattern_properties = schema.get('patternProperties')
            if pattern_properties:
                options.append('pa')
            additional_properties = schema.get('additionlProperties')
            if additional_properties:
                options.append('ad')

            while len(impostor) < make_properties:
                #  make a property if there are properties to make
                if options:
                    choice = random.choice(options)
                else:
                    break  # in case there are no other options

                if choice == 'pr':  # make properties if any remain
                    j_object = random.choice(properties)
                    properties.remove(j_object)
                    impostor[j_object] = self._gen_json(schema['properties'][j_object])
                    if not properties:
                        options.remove('pr')
                elif choice == 'pa':  # make a patternProperty if supported
                    #  TODO support patternProperties
                    options.remove('pr')
                    pattern = random.choice(pattern_properties)
                    name = 'TBD'  # TODO create a string from pattern
                    impostor[name] = self._gen_json(schema['patternProperties'][pattern])
                elif choice == 'ad':  # make an additionalProperties if supported
                    #  TODO support additionalProperties
                    options.remove('pr')
        return impostor

    def _number(self, schema: dict) -> int:
        impostor = self._common(schema)
        if not impostor:
            maximum = schema.get('maximum', UNBOUND_MAX_INT)
            if schema.get('exclusiveMaximum', False):
                maximum = maximum - 1
            minimum = schema.get('minimum', UNBOUND_MIN_INT)
            if schema.get('exclusiveMinimum', False):
                minimum = minimum + 1
            impostor = self.faker.random_int(minimum, maximum)
            multiple_of = schema.get('multipleOf', 1)
            impostor = impostor // multiple_of * multiple_of
        return impostor

    def _string(self, schema: dict) -> str:
        impostor = self._common(schema)
        if not impostor:
            format = schema.get('format')
            if format:
                impostor = getattr(self.faker, self.formats[format])()
            elif False:
                pass  # TODO add pattern generated strings
            else:
                maximum = schema.get('maxLength', UNBOUND_MAX_STRING)
                minimum = schema.get('minLength', UNBOUND_MIN_STRING)
                impostor = self.faker.pystr(minimum, maximum)
        return impostor

    def _boolean(self, schema: dict) -> bool:
        impostor = self._common(schema)
        if not impostor:
            impostor = self.faker.pybool()
        return impostor

    def _array(self, schema: dict) -> list:
        items = schema.get('items')
        contains = schema.get('contains')
        if schema.get('uniqueItems'):
            pass  # TODO handle unique.
        impostor = []
        if isinstance(items, dict):
            if contains:
                impostor.append(self._gen_json(contains))
            minimum = schema.get('minItems', UNBOUND_MIN_ITEMS)
            maximum = schema.get('maxItems', minimum + UNBOUND_MAX_ITEMS)
            length = random.randrange(minimum, maximum)
            while len(impostor) < length:
                impostor.append(self._gen_json(items))
        elif isinstance(items, list):
            for item in items:
                if item == contains:
                    impostor.append(self._gen_json(contains))
                else:
                    impostor.append(self._gen_json(item))

        additional_items = schema.get('additionalItems')
        if isinstance(additional_items, list):
            pass  # TODO: create x items of type items by minimum and maximum
        return impostor

    generators = {
        'number': _number,
        'integer': _number,
        'string': _string,
        'object': _object,
        'array': _array,
        'boolean': _boolean
    }


class TestJsonDataGenerator(unittest.TestCase):

    type_mapping = {'string': str, 'object': dict, 'array': list, 'integer': int, "number": (int, float)}
    schema_analysis = {
        "title": "analysis",
        "required": [
            "timestamp_start_utc",
            "timestamp_stop_utc",
            "computational_method",
            "input_bundles",
            "reference_bundle",
            "analysis_id",
            "analysis_run_type",
            "metadata_schema",
            "tasks",
            "inputs",
            "outputs",
            "core"
        ],
        "additionalProperties": True,
        "definitions": {
            "task": {
                "additionalProperties": False,
                "required": [
                    "name",
                    "start_time",
                    "stop_time",
                    "disk_size",
                    "docker_image",
                    "cpus",
                    "memory",
                    "zone"
                ],
                "type": "object",
                "properties": {
                    "disk_size": {
                        "type": "string"
                    },
                    "name": {
                        "type": "string"
                    },
                    "zone": {
                        "type": "string"
                    },
                    "log_err": {
                        "type": "string"
                    },
                    "start_time": {
                        "type": "string",
                        "format": "date-time"
                    },
                    "cpus": {
                        "type": "integer"
                    },
                    "log_out": {
                        "type": "string"
                    },
                    "stop_time": {
                        "type": "string",
                        "format": "date-time"
                    },
                    "memory": {
                        "type": "string"
                    },
                    "docker_image": {
                        "type": "string"
                    }
                }
            },
            "parameter": {
                "additionalProperties": False,
                "required": [
                    "name",
                    "value"
                ],
                "type": "object",
                "properties": {
                    "checksum": {
                        "type": "string"
                    },
                    "name": {
                        "type": "string"
                    },
                    "value": {
                        "type": "string"
                    }
                }
            },
            "file": {
                "additionalProperties": False,
                "required": [
                    "name",
                    "file_path",
                    "format"
                ],
                "type": "object",
                "properties": {
                    "checksum": {
                        "type": "string"
                    },
                    "file_path": {
                        "type": "string"
                    },
                    "name": {
                        "type": "string"
                    },
                    "format": {
                        "type": "string"
                    }
                }
            }
        },
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {
            "inputs": {
                "items": {
                    "$ref": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/"
                            "analysis.json#/definitions/parameter"
                },
                "type": "array",
                "description": "Input parameters used in the pipeline run, these can be files or string values "
                               "(settings)."
            },
            "reference_bundle": {
                "type": "string",
                "description": "Bundle containing the reference used in running the pipeline."
            },
            "tasks": {
                "items": {
                    "$ref": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/"
                            "analysis.json#/definitions/task"
                },
                "type": "array",
                "description": "Descriptions of tasks in the workflow."
            },
            "description": {
                "type": "string",
                "description": "A general description of the analysis."
            },
            "timestamp_stop_utc": {
                "type": "string",
                "description": "Terminal stop time of the full pipeline.",
                "format": "date-time"
            },
            "input_bundles": {
                "items": {
                    "type": "string"
                },
                "type": "array",
                "description": "The input bundles used in this analysis run."
            },
            "outputs": {
                "items": {
                    "$ref": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/"
                            "analysis.json#/definitions/file"
                },
                "type": "array",
                "description": "Output generated by the pipeline run."
            },
            "name": {
                "type": "string",
                "description": "A short, descriptive name for the analysis that need not be unique."
            },
            "computational_method": {
                "type": "string",
                "description": "A URI to a versioned workflow and versioned execution environment in a GA4GH-compliant "
                               "repository."
            },
            "timestamp_start_utc": {
                "type": "string",
                "description": "Initial start time of the full pipeline.",
                "format": "date-time"
            },
            "core": {
                "description": "Type and schema for this object.",
                "$ref": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/core.json"
            },
            "analysis_run_type": {
                "enum": [
                    "run",
                    "copy-forward"
                ],
                "type": "string",
                "description": "Indicator of whether the analysis actually ran or was just copied forward as an "
                               "optimization."
            },
            "metadata_schema": {
                "type": "string",
                "description": "The version of the metadata schemas used for the json files."
            },
            "analysis_id": {
                "type": "string",
                "description": "A unique ID for this analysis."
            }
        }
    }

    simple_integer = {'type': 'integer', 'enum': [123]}
    simple_string = {'type': 'string', 'enum': ['ac']}
    simple_bool = {'type': 'bool'}
    simple_array = {'type': 'array', 'items': simple_string, 'maxItems': 3}
    simple_array2 = {'type': 'array', 'items': [simple_string, simple_integer, simple_string], 'maxItems': 3}
    simple_object = {'type': 'object', 'properties': {'thing_1': simple_integer, 'thing_2': simple_string}}

    def setUp(self):
        self.json_gen = JsonDataGenerator(self.schema_analysis)

    def test_number(self):
        repeat = 10
        _number = self.json_gen._number

        self._test_common(_number, 'number', [1, 2, 3.3], 999)
        self._test_common(_number, 'integer', [1, 2, 3.3], 999)

        with self.subTest("does not exceed maximum"):
            maximum = 50
            schema = {'type': 'integer', 'maximum': maximum}
            for i in range(repeat):
                self.assertLessEqual(_number(schema), maximum)

        with self.subTest("does not exceed minimum"):
            minimum = 10
            schema = {'type': 'integer', 'minimum': minimum}
            for i in range(repeat):
                self.assertGreaterEqual(_number(schema), maximum)

        with self.subTest("does not exceed minimum or maximum"):
            maximum = 10
            minimum = 1
            schema = {'type': 'integer', 'minimum': minimum, 'maximum': maximum}
            for i in range(repeat):
                value = _number(schema)
                self.assertLessEqual(value, maximum)
                self.assertGreaterEqual(value, minimum)

        with self.subTest("is a multiple of"):
            multiple_of = 5
            schema = {'type': 'integer', 'multipleOf': multiple_of}
            for i in range(repeat):
                self.assertFalse(_number(schema) % multiple_of)

        with self.subTest("is a multiple of and within boundaries"):
            multiple_of = 5
            maximum = 50
            minimum = 10
            schema = {'type': 'integer', 'minimum': minimum, 'maximum': maximum, 'multipleOf': multiple_of}
            for i in range(repeat):
                value = _number(schema)
                self.assertLessEqual(value, maximum)
                self.assertGreaterEqual(value, minimum)
                self.assertFalse(value % multiple_of)

        with self.subTest("does not exceed exclusive range"):
            maximum = 3
            minimum = 1
            schema = {'type': 'integer', 'minimum': minimum, 'maximum': maximum,
                      'exclusiveMinimum': True, 'exclusiveMaximum': True}
            for i in range(repeat):
                value = _number(schema)
                self.assertEqual(value, 2)

    def test_string(self):
        minimum = 10
        maximum = 50
        repeat = 25
        _string = self.json_gen._string
        self._test_common(_string, 'string', ['abcd', 'ferwvtg', 'c2452642@$%^@  grg56y7'], 'hello')

        with self.subTest("does not exceed minLength"):
            schema = {'type': 'string', 'minLength': minimum}
            for i in range(repeat):
                self.assertGreaterEqual(len(_string(schema)), minimum)

        with self.subTest("does not exceed maxLength"):
            schema = {'type': 'string', 'maxLength': maximum}
            for i in range(repeat):
                self.assertLessEqual(len(_string(schema)), maximum)

        with self.subTest("does not exceed maxLength or minLength"):
            schema = {'type': 'string', 'maxLength': maximum, 'minLength': minimum}
            for i in range(repeat):
                value = _string(schema)
                msg = f"{value} exceed range({minimum},{maximum})"
                self.assertLessEqual(len(value), maximum, msg=msg)
                self.assertGreaterEqual(len(value), minimum, msg=msg)

        with self.subTest("matches pattern"):
            schema = {'type': 'string', 'pattern': ".*"}

        with self.subTest("matches pattern and does not exceed minLength or maxLength"):
            schema = {'type': 'string', 'pattern': ".*"}

    def test_array(self):
        repeat = 25
        _array = self.json_gen._array
        self._test_common(_array, 'array')

        with self.subTest("with items as schema"):
            schema = {'type': 'array', 'items': self.simple_string}
            value = _array(schema)
            self.assertIn('ac', value)

        with self.subTest("with items as array of schema"):
            schema = {'type': 'array', 'items': [self.simple_integer, self.simple_string]}
            value = _array(schema)
            self.assertEqual([123, 'ac'], value)

        with self.subTest("with max items"):
            maximum = 3
            schema = {'type': 'array', 'maxItems': maximum, 'items': self.simple_string}
            for i in range(repeat):
                self.assertLessEqual(len(_array(schema)), maximum)

        with self.subTest("with min items"):
            minimum = 1
            schema = {'type': 'array', 'minItems': minimum, 'items': self.simple_string}
            for i in range(repeat):
                self.assertGreaterEqual(len(_array(schema)), minimum)

        with self.subTest("with max and max items"):
            maximum = 3
            minimum = 1
            schema = {'type': 'array', 'minItems': minimum, 'maxItems': maximum, 'items': self.simple_string}
            for i in range(repeat):
                self.assertLessEqual(len(_array(schema)), maximum)
                self.assertGreaterEqual(len(_array(schema)), minimum)

        with self.subTest("TODO with additional items"):
            pass

        with self.subTest("TODO with contains"):
            pass

        with self.subTest("TODO with unique items"):
            pass

    def test_object(self):
        repeat = 25
        _object = self.json_gen._object
        properties = {
            'thing1': self.simple_string,
            'thing2': self.simple_string,
            'thing3': self.simple_string,
            'thing4': self.simple_string
        }

        with self.subTest("with properties"):
            schema = {'type': 'object', 'properties': {'thing1': self.simple_string}}
            value = _object(schema)
            self.assertTrue(value.get('thing1'))

        with self.subTest("with required properties"):
            schema = {'type': 'object', 'required': properties.keys(), 'properties': properties}
            value = _object(schema)
            self.assertTrue(value.get('thing1'))
            self.assertTrue(value.get('thing2'))
            self.assertTrue(value.get('thing3'))
            self.assertTrue(value.get('thing4'))

        with self.subTest("with max items"):
            maximum = 3
            schema = {'type': 'object', 'maxProperties': maximum,
                      'properties': properties
                      }
            for i in range(repeat):
                self.assertLessEqual(len(_object(schema)), maximum)

        with self.subTest("with min items"):
            minimum = 1
            schema = {'type': 'object', 'minProperties': minimum,
                      'properties': properties
                      }
            for i in range(repeat):
                self.assertGreaterEqual(len(_object(schema)), minimum)

        with self.subTest("with max and max items"):
            maximum = 3
            minimum = 1
            schema = {'type': 'object', 'minProperties': minimum, 'maxProperties': maximum, 'properties': properties}
            for i in range(repeat):
                self.assertLessEqual(len(_object(schema)), maximum)
                self.assertGreaterEqual(len(_object(schema)), minimum)

        with self.subTest("TODO with additionalProperties"):
            pass

        with self.subTest("TODO with dependencies"):
            pass

        with self.subTest("TODO with patternProperties"):
            pass

        with self.subTest("TODO with propertyNames"):
            pass

    def _test_common(self, func, jtype: str, enums: list=None, const=None):
        with self.subTest(f"with only 'type: {jtype}' in schema"):
            self.assertIsInstance(func({'type': jtype}), self.type_mapping.get(jtype))

        if const and enums:
            with self.subTest("test constant take precedence over enum"):
                self.assertEqual(func({'type': jtype, 'enum': enums, 'const': const}), const)

        if enums:
            with self.subTest("test enum value are used"):
                for i in range(len(enums)):
                    self.assertIn(func({'type': jtype, 'enum': enums}), enums)

    def test_basic(self):
        generate_json = self.json_gen.generate_json
        for i in range(100):
            with self.subTest(i):
                generate_json()


if __name__ == "__main__":
    unittest.main()
