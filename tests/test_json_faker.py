import unittest

from dss import Config, BucketConfig
from tests.scalability.json_faker import JsonFaker
from tests.infra import testmode
import json

@testmode.standalone
class TestJsonFaker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Config.set_config(BucketConfig.TEST)

    def setUp(self):
        self.path = "tests/fixtures/json_schemas"
        self.faker = JsonFaker(self.path)

    def test_locals(self):
        self.assertListEqual(self.faker.schema_files, ['analysis.json', 'assay.json', 'project.json', 'sample.json'])
        self.assertEqual(self.faker.path, self.path)

    def test_generation(self):
        fake_json = self.faker.generate()
        self.assertIsInstance(fake_json, str)
        self.assertIsInstance(json.loads(fake_json), dict)

if __name__ == "__main__":
    unittest.main()
