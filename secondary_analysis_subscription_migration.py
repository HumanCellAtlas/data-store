#!/usr/bin/env python

from hca.dss import DSSClient


dss_client = DSSClient(swagger_url="https://dss.integration.data.humancellatlas.org/v1/swagger.json")


def put_subscription(jmespath_query, callback_url, replica="gcp"):
    dss_client.put_subscription(
        replica=replica,
        callback_url=callback_url,
        jmespath_query=jmespath_query
    )


secondary_analysis_subscription_migration = dict()

secondary_analysis_subscription_migration['89bfca93-b877-48ef-995d-c69435e83950'] = {
    'owner': "bluebox-subscription-manager@broad-dsde-mint-test.iam.gserviceaccount.com",
    'callback_url': "https://pipelines.integration.data.humancellatlas.org/notifications",
    'es_query': { 
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
    },
    'jmespath_query': (
        "(files.library_preparation_protocol_json[].library_construction_method[].ontology_label | contains(@, `10X v2 sequencing`))"
        "&& (files.library_preparation_protocol_json[].end_bias | contains(@, `3 prime tag`))"
        "&& (files.library_preparation_protocol_json[].nucleic_acid_source | contains(@, `single cell`))"
        "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
        "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
        "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id | not_null(@, `[]`) | !contains(@, `analysis`)"
    )
}

secondary_analysis_subscription_migration['d1b8fc71-3753-43a5-b173-2f292da8154f'] = {
    'owner': "bluebox-subscription-manager@broad-dsde-mint-test.iam.gserviceaccount.com",
    'callback_url': "https://pipelines.integration.data.humancellatlas.org/notifications",
    'es_query': {
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
    },
    'jmespath_query': (
        "(files.library_preparation_protocol_json[].library_construction_method[].ontology | contains(@, `EFO:0008931`))"
        "&& (files.sequencing_protocol_json[].paired_end | [0])"
        "&& files.donor_organism_json[].biomaterial_core.ncbi_taxon_id[] | (min(@) == `9606` && max(@) == `9606`)"
        "&& files.sequencing_protocol_json[].sequencing_approach.ontology_label | not_null(@, `[]`) | !contains(@, `CITE-seq`)"
        "&& files.analysis_process_json[].process_type.text | not_null(@, `[]`) | !contains(@, `analysis`)"
    )   
}

for uuid, mig in secondary_analysis_subscription_migration.items():
    put_subscription(mig['jmespath_query'], f"https://2zxizmwkmd.execute-api.us-east-1.amazonaws.com/api/event/dss-integration-green-{uuid}/")
