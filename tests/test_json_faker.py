import unittest

from dss import Config, BucketConfig
from tests.scalability.json_faker import JsonFaker
from tests.infra import testmode
import json

schema_url = [
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/assay.json",
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/project.json",
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/sample.json",
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/analysis.json"
]

@testmode.standalone
class TestJsonFaker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Config.set_config(BucketConfig.TEST)

    def setUp(self):
        self.faker = JsonFaker(schema_url)

    def test_locals(self):
        self.assertListEqual(self.faker.schema_urls, schema_url)

    def test_generation(self):
        fake_json = self.faker.generate()
        self.assertIsInstance(fake_json, str)
        self.assertIsInstance(json.loads(fake_json), dict)

if __name__ == "__main__":
    unittest.main()
