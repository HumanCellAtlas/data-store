#!/usr/bin/env python

import json
import os
import sys
import unittest
from typing import Callable, Any, Dict, Union, Tuple

from faker import Faker
from jsonschema import Draft4Validator

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import testmode
from dss.util.json_gen.generator import JsonGenerator, JsonProvider

type_mapping = {'string': str,
                'object': dict,
                'array': list,
                'integer': int,
                'number': (int, float)}  # type: Dict[str, Union[Any, Tuple[Any, ...]]]
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
simple_float = {'type': 'number', 'enum': [3.14]}
simple_string = {'type': 'string', 'enum': ['ac']}
simple_bool = {'type': 'boolean'}
simple_array = {'type': 'array', 'items': simple_string, 'maxItems': 3}
simple_array2 = {'type': 'array', 'items': [simple_string, simple_integer, simple_string], 'maxItems': 3}
simple_object = {'type': 'object', 'properties': {'thing_1': simple_integer, 'thing_2': simple_string}}


class Base(unittest.TestCase):
    repeat = 1000
    def setUp(self):
        self.json_gen = JsonGenerator()

    def _test_common(self, func: Callable[[dict], Any], jtype: str, enums: list=None, const: Any=None):
        with self.subTest(f"with only 'type: {jtype}' in schema"):
            self.assertTrue(isinstance(func({'type': jtype}), type_mapping[jtype]),
                            msg=f"{func.__name__} did not return type {type_mapping[jtype]}.")

        if const and enums:
            with self.subTest("test constant take precedence over enum"):
                self.assertEqual(func({'type': jtype, 'enum': enums, 'const': const}), const)

        if enums:
            with self.subTest("test enum value are used"):
                for i in range(len(enums)):
                    self.assertIn(func({'type': jtype, 'enum': enums}), enums)


@testmode.standalone
class TestNumber(Base):
    numbers = [10.0, 0.3, 0.0, -0.1, -10.0, 1e-09]
    multiple_ofs = [0.1, 1.01, 1.5, 1.99, 2, 3]
    invalid_multiple_ofs = [0, -2]

    def test_common(self):
        self._test_common(self.json_gen._number, 'number', self.numbers, 0.999)

    def test_inclusive_range(self):
        for minimum in self.numbers:
            with self.subTest(f"does not exceed minimum of {minimum}"):
                schema = {'type': 'number', 'minimum': minimum}
                for i in range(self.repeat):
                    self.assertGreaterEqual(self.json_gen._number(schema), minimum)

        for maximum in self.numbers:
            with self.subTest(f"does not exceed maximum of {maximum}"):
                schema = {'type': 'number', 'maximum': maximum}
                for i in range(self.repeat):
                    self.assertLessEqual(self.json_gen._number(schema), maximum)

        with self.subTest("minimum == maximum"):
            maximum = 1.0
            minimum = 1.0
            schema = {'type': 'number', 'minimum': minimum, 'maximum': maximum}
            for i in range(self.repeat):
                value = self.json_gen._number(schema)
                self.assertEqual(value, 1.0)

        with self.subTest("does not exceed minimum or maximum"):
            maximum = 1.0
            minimum = -1.0
            schema = {'type': 'number', 'minimum': minimum, 'maximum': maximum}
            for i in range(self.repeat):
                value = self.json_gen._number(schema)
                self.assertLessEqual(value, maximum)
                self.assertGreaterEqual(value, minimum)

    def test_multiple_of(self):
        for multiple_of in self.multiple_ofs:
            with self.subTest(f"is a multiple of {multiple_of}"):
                schema = {'type': 'number', 'multipleOf': multiple_of}
                for i in range(self.repeat):
                    value = self.json_gen._number(schema)
                    self.assertTrue(round(value / multiple_of, 3).is_integer(), msg=f"value:{value} is not a "
                                                                                    f"multipleOf:{multiple_of}.")
        for multiple_of in self.invalid_multiple_ofs:
            with self.subTest(f"is a multiple of {multiple_of}"):
                schema = {'type': 'number', 'multipleOf': multiple_of}
                with self.assertRaises(ValueError):
                    self.json_gen._number(schema)

    def test_multiple_of_in_inclusive_range(self):
        maximum = 10.0
        minimum = 0.0
        for multiple_of in self.multiple_ofs:
            with self.subTest(f"is a multiple of {multiple_of} and within boundaries"):
                schema = {'type': 'number', 'minimum': minimum, 'maximum': maximum, 'multipleOf': multiple_of}
                for i in range(self.repeat):
                    value = self.json_gen._number(schema)
                    v = round(value / multiple_of, 3)
                    self.assertLessEqual(value, maximum)
                    self.assertGreaterEqual(value, minimum)
                    self.assertTrue(v.is_integer(), msg=f"value:{value} is not a "
                                                        f"multipleOf:{multiple_of}.")

    def test_exclusive_range(self):
        with self.subTest("does not exceed exclusive range"):
            maximum = 3.0
            minimum = 1.0
            schema = {'type': 'number', 'exclusiveMinimum': minimum, 'exclusiveMaximum': maximum}
            for i in range(self.repeat):
                value = self.json_gen._number(schema)
                self.assertLess(value, maximum)
                self.assertGreater(value, minimum)

        with self.subTest("WIP minimum == maximum"):
            if False:
                maximum = 1.0
                minimum = 1.0
                schema = {'type': 'number', 'exclusiveMinimum': minimum, 'exclusiveMaximum': maximum}
                for i in range(self.repeat):
                    value = self.json_gen._number(schema)
                    self.assertEqual(value, 1.0)


@testmode.standalone
class TestInteger(Base):

    def test_common(self):
        self._test_common(self.json_gen._integer, 'integer', [1, 2], 999)

    def test_inclusive_range(self):
        minimum = 1
        with self.subTest(f"does not exceed minimum of {minimum}"):
            schema = {'type': 'integer', 'minimum': minimum}
            for i in range(self.repeat):
                self.assertGreaterEqual(self.json_gen._integer(schema), minimum)

        maximum = 10
        with self.subTest(f"does not exceed maximum of {maximum}"):

            schema = {'type': 'integer', 'maximum': maximum}
            for i in range(self.repeat):
                self.assertLessEqual(self.json_gen._integer(schema), maximum)

        with self.subTest("does not exceed minimum or maximum"):
            schema = {'type': 'integer', 'minimum': minimum, 'maximum': maximum}
            for i in range(self.repeat):
                value = self.json_gen._integer(schema)
                self.assertLessEqual(value, maximum)
                self.assertGreaterEqual(value, minimum)

        with self.subTest("minimum == maximum"):
            maximum = 1
            schema = {'type': 'integer', 'minimum': minimum, 'maximum': maximum}
            for i in range(self.repeat):
                value = self.json_gen._integer(schema)
                self.assertEqual(value, 1)

    def test_multiple_of(self):
        multiple_ofs = [2, 3, 5, 7]
        for multiple_of in multiple_ofs:
            with self.subTest(f"is a multiple of {multiple_of}"):
                schema = {'type': 'integer', 'multipleOf': multiple_of}
                for i in range(self.repeat):
                    self.assertFalse(self.json_gen._integer(schema) % multiple_of)

        invalid_multiple_ofs = [0, -2]
        for multiple_of in invalid_multiple_ofs:
            with self.subTest(f"is a multiple of {multiple_of}"):
                schema = {'type': 'number', 'multipleOf': multiple_of}
                with self.assertRaises(ValueError):
                    self.json_gen._integer(schema)

    def test_multiple_of_in_range(self):
        """is a multiple of and within boundaries"""
        multiple_of = 5
        maximum = 25
        minimum = 10
        schema = {'type': 'integer', 'minimum': minimum, 'maximum': maximum, 'multipleOf': multiple_of}
        for i in range(self.repeat):
            value = self.json_gen._integer(schema)
            self.assertLessEqual(value, maximum)
            self.assertGreaterEqual(value, minimum)
            self.assertFalse(value % multiple_of)

    def test_exclusive_range(self):
        """does not exceed exclusive range"""
        maximum = 3
        minimum = 1
        schema = {'type': 'integer', 'exclusiveMinimum': minimum, 'exclusiveMaximum': maximum}
        for i in range(self.repeat):
            value = self.json_gen._integer(schema)
            self.assertEqual(value, 2)


@testmode.standalone
class TestString(Base):
    minimum = 10
    maximum = 50
    regexs = {'version': "^[0-9]{2}\.[A-Za-z]{4}\.[0-9a-z]{3}$",
              'phone#': "^(\\([0-9]{3}\\))?[0-9]{3}-[0-9]{4}$",
              'email': "[A-Za-z][0-9A-Za-z.]*@[A-Za-z][0-9A-Za-z]{,4}\.[A-Za-z][0-9A-Za-z]{,4}"
              }

    def test_common(self):
        self._test_common(self.json_gen._string, 'string', ['abcd', 'ferwvtg', 'c2452642@$%^@  grg56y7'], 'hello')

    def test_minLength(self):
        """does not exceed minLength"""
        schema = {'type': 'string', 'minLength': self.minimum}
        for i in range(self.repeat):
            self.assertGreaterEqual(len(self.json_gen._string(schema)), self.minimum)

    def test_maxLength(self):
        """does not exceed maxLength"""
        schema = {'type': 'string', 'maxLength': self.maximum}
        for i in range(self.repeat):
            self.assertLessEqual(len(self.json_gen._string(schema)), self.maximum)

    def test_range(self):
        """does not exceed maxLength or minLength"""
        schema = {'type': 'string', 'maxLength': self.maximum, 'minLength': self.minimum}
        for i in range(self.repeat):
            value = self.json_gen._string(schema)
            msg = f"{value} exceed range({self.minimum},{self.maximum})"
            self.assertLessEqual(len(value), self.maximum, msg=msg)
            self.assertGreaterEqual(len(value), self.minimum, msg=msg)

    def test_minimum_is_maximum(self):
        """minLength == maxLength"""
        maximum = 1
        minimum = 1
        schema = {'type': 'string', 'minLength': minimum, 'maxLength': maximum}
        for i in range(self.repeat):
            value = self.json_gen._string(schema)
            self.assertEqual(len(value), 1)

    def test_pattern(self):
        for name, regex in self.regexs.items():
            with self.subTest(f"matches {name} pattern "):
                schema = {'type': 'string', 'pattern': regex}
                for i in range(self.repeat):
                    value = self.json_gen._string(schema)
                    self.assertRegex(value, regex)

    def test_pattern_within_range(self):
        """WIP matches pattern and does not exceed minLength or maxLength"""
        if False:
            maximum = 15
            minimum = 5
            regex = self.regexs['email']
            schema = {'type': 'string', 'minLength': minimum, 'maxLength': maximum, 'pattern': regex}
            for i in range(self.repeat):
                value = self.json_gen._string(schema)
                self.assertRegex(value, regex)
                self.assertLessEqual(len(value), maximum)
                self.assertGreaterEqual(len(value), minimum)


@testmode.standalone
class TestArray(Base):

    def test_common(self):
        self._test_common(self.json_gen._array, 'array')

    def test_enums(self):
        schema = {'type': 'array', 'enum': [1, 2], 'minItems': 10, 'items': simple_integer}
        value = self.json_gen._array(schema)
        self.assertIn(1, value)
        self.assertIn(2, value)

    def test_unique_enums(self):
        schema = {'type': 'array', 'enum': [1, 2, 3], 'uniqueItems': True, 'minItems': 3,
                  'items': simple_integer}
        value = self.json_gen._array(schema)
        self.assertIn(1, value)
        self.assertIn(2, value)

    def test_constants(self):
        pass

    def test_items_is_schema(self):
        schema = {'type': 'array', 'items': simple_string}
        value = self.json_gen._array(schema)
        self.assertIn('ac', value)

    def test_items_is_array(self):
        schema = {'type': 'array', 'items': [simple_integer, simple_string]}
        value = self.json_gen._array(schema)
        self.assertEqual([123, 'ac'], value)

    def test_in_range(self):
        with self.subTest("with maxItems"):
            maximum = 3
            schema = {'type': 'array', 'maxItems': maximum, 'items': simple_string}
            for i in range(self.repeat):
                self.assertLessEqual(len(self.json_gen._array(schema)), maximum)

        with self.subTest("with minItems"):
            minimum = 1
            schema = {'type': 'array', 'minItems': minimum, 'items': simple_string}
            for i in range(self.repeat):
                self.assertGreaterEqual(len(self.json_gen._array(schema)), minimum)

        with self.subTest("with minItems and maxItems"):
            maximum = 3
            minimum = 1
            schema = {'type': 'array', 'minItems': minimum, 'maxItems': maximum, 'items': simple_string}
            for i in range(self.repeat):
                self.assertLessEqual(len(self.json_gen._array(schema)), maximum)
                self.assertGreaterEqual(len(self.json_gen._array(schema)), minimum)

        with self.subTest("minItems == maxItems"):
            maximum = 1
            minimum = 1
            schema = {'type': 'array', 'minItems': minimum, 'maxItems': maximum, 'items': simple_string}
            for i in range(self.repeat):
                value = self.json_gen._array(schema)
                self.assertEqual(len(value), 1)

    def test_additionalItems(self):
        with self.subTest("items as list with additionalItems"):
            minimum = 4
            maximum = 4
            schema = {'type': 'array', 'maxItems': maximum, 'minItems': minimum,
                      'items': [simple_string, simple_string],
                      'additionalItems': simple_integer}
            self.assertListEqual(self.json_gen._array(schema), ['ac', 'ac', 123, 123])

        with self.subTest("additionalItems is ignored when items is schema"):
            minimum = 3
            maximum = 3
            schema = {'type': 'array', 'maxItems': maximum, 'minItems': minimum, 'items': simple_string,
                      'additional_items': simple_integer}
            self.assertListEqual(self.json_gen._array(schema), ['ac', 'ac', 'ac'])

    def test_contains(self):
        with self.subTest("TODO items as schema with contains"):
            pass

        with self.subTest("TODO items as array of schema with contains"):
            pass

    def test_unique(self):
        with self.subTest("TODO items as schema with unique items"):
            pass

        with self.subTest("TODO items as array of schema with unique items"):
            pass


@testmode.standalone
class TestObject(Base):
    properties = {
        'thing1': simple_string,
        'thing2': simple_integer,
        'thing3': simple_array,
        'thing4': simple_array2,
        'thing5': simple_float,
        'thing6': simple_bool
    }

    def test_properties(self):
        schema = {'type': 'object', 'properties': {'thing1': simple_string}}
        value = self.json_gen._object(schema)
        self.assertTrue(value.get('thing1'))

    def test_required(self):
        schema = {'type': 'object', 'required': self.properties.keys(), 'properties': self.properties}
        value = self.json_gen._object(schema)
        self.assertIsInstance(value.get('thing1'), str)
        self.assertIsInstance(value.get('thing2'), int)
        self.assertIsInstance(value.get('thing3'), list)
        self.assertIsInstance(value.get('thing4'), list)
        self.assertIsInstance(value.get('thing5'), float)
        self.assertIsInstance(value.get('thing6'), bool)

    def test_range(self):
        with self.subTest("with maxProperties"):
            maximum = 3
            schema = {'type': 'object', 'maxProperties': maximum,
                      'properties': self.properties
                      }
            for i in range(self.repeat):
                self.assertLessEqual(len(self.json_gen._object(schema)), maximum)

        with self.subTest("with minProperties"):
            minimum = 1
            schema = {'type': 'object', 'minProperties': minimum,
                      'properties': self.properties
                      }
            for i in range(self.repeat):
                self.assertGreaterEqual(len(self.json_gen._object(schema)), minimum)

        with self.subTest("with minProperties and maxProperties"):
            maximum = 3
            minimum = 1
            schema = {'type': 'object', 'minProperties': minimum, 'maxProperties': maximum,
                      'properties': self.properties}
            for i in range(self.repeat):
                self.assertLessEqual(len(self.json_gen._object(schema)), maximum)
                self.assertGreaterEqual(len(self.json_gen._object(schema)), minimum)

        with self.subTest("minLength == maxProperties"):
            maximum = 1
            minimum = 1
            schema = {'type': 'object', 'minProperties': minimum, 'maxProperties': maximum,
                      'properties': self.properties}
            for i in range(self.repeat):
                value = self.json_gen._object(schema)
                self.assertEqual(len(value), 1)

    def test_additionalProperties(self):
        with self.subTest("True"):
            maximum = 4
            minimum = 4
            schema = {'type': 'object', 'additionalProperties': True,
                      'minProperties': minimum, 'maxProperties': maximum}
            value = self.json_gen._object(schema)
            self.assertEqual(len(value), 4)

        with self.subTest("False"):
            schema = {'type': 'object', 'properties': {'thing1': simple_string}, 'additionalProperties': False}
            value = self.json_gen._object(schema)
            self.assertTrue(value.get('thing1'))
            self.assertEqual(len(value), 1)

    def test_patternProperties(self):
        with self.subTest("with patternProperties"):
            regex = '[^.]+'
            schema = {'type': 'object', 'patternProperties': {regex: simple_integer}}
            value = self.json_gen._object(schema)
            for key, item in value.items():
                self.assertRegex(key, regex)
                self.assertEqual(item, 123)

    def test_dependencies(self):
        pass

    def test_propertyNames(self):
        pass


@testmode.standalone
class TestJsonGenerator(Base):

    def test_generate(self):
        generate_json = self.json_gen.generate_json
        for i in range(self.repeat):
            with self.subTest(i):
                generate_json(schema_analysis)


if __name__ == "__main__":
    unittest.main()
