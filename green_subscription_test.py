#!/usr/bin/env python

import jmespath

from hca.dss import DSSClient

client = DSSClient(swagger_url="https://dss.integration.data.humancellatlas.org/v1/swagger.json")
replica = "aws"

es_10X_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "files.library_preparation_protocol_json.library_construction_method.ontology_label": "10X v2 sequencing"
                    }
                },
                {
                    "match": {
                        "files.library_preparation_protocol_json.end_bias": "3 prime tag"
                    }
                },
                {
                    "match": {
                        "files.library_preparation_protocol_json.nucleic_acid_source": "single cell"
                    }
                },
                {
                    "match": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": 9606
                    }
                }
            ],
            "must_not": [
                {
                    "match": {
                        "files.sequencing_protocol_json.sequencing_approach.ontology_label": "CITE-seq"
                    }
                },
                {
                    "match": {
                        "files.analysis_process_json.process_type.text": "analysis"
                    }
                },
                {
                    "range": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": {
                            "lt": 9606
                        }
                    }
                },
                {
                    "range": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": {
                            "gt": 9606
                        }
                    }
                }
            ]
        }
    }
}
    
jmespath_10X_query = (
    "(files.library_preparation_protocol_json[].library_construction_method[].ontology_label | contains(@, `10X v2 sequencing`))"
    "&& (files.library_preparation_protocol_json[].end_bias | contains(@, `3 prime tag`))"
    "&& (files.library_preparation_protocol_json[].nucleic_acid_source | contains(@, `single cell`))"
    "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
    "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
    "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id | not_null(@, `[]`) | !contains(@, `analysis`)"
)

es_SS2_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "files.library_preparation_protocol_json.library_construction_method.ontology": "EFO:0008931"
                    }
                },
                {
                    "match": {
                        "files.sequencing_protocol_json.paired_end": True
                    }
                },
                {
                    "match": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": 9606
                    }
                }
            ],
            "should": [
                {
                    "match": {
                        "files.dissociation_protocol_json.dissociation_method.ontology": "EFO:0009108"
                    }
                },
                {
                    "match": {
                        "files.dissociation_protocol_json.dissociation_method.text": "mouth pipette"
                    }
                }
            ],
            "must_not": [
                {
                    "match": {
                        "files.analysis_process_json.process_type.text": "analysis"
                    }
                },
                {
                    "range": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": {
                            "lt": 9606
                        }
                    }
                },
                {
                    "range": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": {
                            "gt": 9606
                        }
                    }
                }
            ]
        }
    }
}
    
jmespath_SS2_query = (
    "(files.library_preparation_protocol_json[].library_construction_method[].ontology | contains(@, `EFO:0008931`))"
    "&& (files.sequencing_protocol_json[].paired_end | [0])"
    "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
    "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
    "&& files.analysis_process_json[].process_type.text | not_null(@, `[]`) | !contains(@, `analysis`)"
)

def test_query_match(es_query, jmespath_query):
    count = 0
    for hit in client.post_search.iterate(replica=replica, output_format="raw", es_query=es_query):
        did_match = jmespath.search(jmespath_query, hit['metadata'])
        if not did_match:
            print("JMESPath query did not match for bundle", hit['bundle_fqid'])
        count += 1
    print(count, "bundles tested")

test_query_match(es_10X_query, jmespath_10X_query)
test_query_match(es_SS2_query, jmespath_SS2_query)
