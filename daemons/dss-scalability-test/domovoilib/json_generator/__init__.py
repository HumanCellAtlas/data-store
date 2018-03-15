import json
import random
import string

from tests.json_gen.hca_generator import HCAJsonGenerator


def id_generator(size=30, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


schema_urls = [
    "https://schema.humancellatlas.org/bundle/5.1.0/project",
    "https://schema.humancellatlas.org/bundle/5.1.0/submission",
    "https://schema.humancellatlas.org/bundle/5.1.0/ingest_audit",
    "https://schema.humancellatlas.org/bundle/5.1.0/protocol"
]

json_faker = HCAJsonGenerator(schema_urls)


def generate_sample() -> str:
    return json_faker.generate()
