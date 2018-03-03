import json
import random
import string

def id_generator(size=30, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def generate_sample() -> str:
    json_sample_doc = {
        "core": {
            "type": "sample_bundle",
            "schema_url": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/sample_bundle.json",
            "schema_version": "4.6.1"
        },
        "samples": [{
            "content": {
                "core": {
                    "type": "sample",
                    "schema_url": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/sample.json",
                    "schema_version": "4.6.1"
                },
                "name": id_generator(),
                "specimen_from_organism": {
                    "body_part": {
                        "text": "glioblastoma"
                    },
                    "organ": {
                        "text": "brain"
                    }
                },
                "ncbi_taxon_id": 9606,
                "derived_from": id_generator(),
                "sample_id": id_generator()
            },
            "derivation_protocols": [{
                "pdf": id_generator(),
                "protocol_id": "Q3_DEMO-protocol",
                "type": {
                    "text": "single cell sequencing"
                },
                "core": {
                    "type": "protocol",
                    "schema_url": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/protocol.json",
                    "schema_version": "4.6.1"
                }
            }],
            "hca_ingest": {
                "accession": "",
                "submissionDate": "2017-12-15T16:30:56.471Z",
                "updateDate": "2017-12-15T16:31:37.937Z",
                "document_id": "fdd17f7d-a967-4b5e-a3fa-b9afc8b8d07b"
            },
            "derived_from": "76983296-83fa-48e8-ae8f-f9b0e6742acd"
        }, {
            "content": {
                "core": {
                    "type": "sample",
                    "schema_url": "https://raw.githubusercontent.com/HumanCellAtlas/metadata-schema/4.6.1/json_schema/sample.json",
                    "schema_version": "4.6.1"
                },
                "name": id_generator(),
                "genus_species": {
                    "text": "Homo sapiens",
                    "ontology": "NCBITaxon:9606"
                },
                "ncbi_taxon_id": 9606,
                "sample_id": "SAMN04303778",
                "donor": {
                    "is_living": True
                }
            },
            "derivation_protocols": [],
            "hca_ingest": {
                "accession": "",
                "submissionDate": "2017-12-15T16:30:56.454Z",
                "updateDate": "2017-12-15T16:31:38.757Z",
                "document_id": "76983296-83fa-48e8-ae8f-f9b0e6742acd"
            }
        }]
    }
    return json.dumps(json_sample_doc)
