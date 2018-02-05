import unittest
from jsonschema.validators import Draft4Validator
from jsonschema.exceptions import ValidationError
from dss import Config, BucketConfig
from tests.scalability.json_faker import JsonFaker
from tests.infra import testmode
import json

schema_urls = [
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/analysis_bundle.json",
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/assay_bundle.json",
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/project_bundle.json",
    "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.0/json_schema/sample_bundle.json",
]


@testmode.standalone
class TestJsonFaker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Config.set_config(BucketConfig.TEST)

    def setUp(self):
        self.faker = JsonFaker(schema_urls)

    def test_locals(self):
        for url in schema_urls:
            name = url.split('/')[-1]
            self.assertEqual(self.faker.schemas[name], {'$ref': url, 'id': url})

    def test_generation(self):
        for name in self.faker.schemas.keys():
            with self.subTest(name):
                validator = Draft4Validator(self.faker.schemas[name])
                fake_json = self.faker.generate(name)
                fake_json = json.loads(fake_json)
                try:
                    validator.validate(fake_json[name])
                except ValidationError as ex:
                    self.fail(ex)


if __name__ == "__main__":
    unittest.main()
