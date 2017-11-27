"""Sample queries for testing elastic search"""

smartseq2_paired_ends_v3_query = \
    {
        'query': {
            'bool': {
                'must': [{
                    'match': {
                        "files.sample_json.donor.species.text": "Homo sapiens"
                    }
                }, {
                    'match': {
                        "files.assay_json.single_cell.cell_handling": "Fluidigm C1"
                    }
                }, {
                    'match': {
                        "files.sample_json.ncbi_biosample": "SAMN04303778"
                    }
                }]
            }
        }
    }

smartseq2_paired_ends_v2_or_v3_query = \
    {
        'query': {
            'bool': {
                'must': [
                    {'match': {'files.sample_json.ncbi_biosample': "SAMN04303778"}},
                    {
                        'bool': {
                            'should': [
                                {'match': {'files.assay_json.single_cell.method': "Fluidigm C1"}},
                                {'match': {'files.assay_json.single_cell.cell_handling': "Fluidigm C1"}}
                            ],
                            'minimum_should_match': 1
                        }
                    },
                    {
                        'bool': {
                            'should': [
                                {'match': {'files.sample_json.donor.species': "Homo sapiens"}},
                                {'match': {'files.sample_json.donor.species.text': "Homo sapiens"}}
                            ],
                            'minimum_should_match': 1
                        }
                    }
                ]
            }
        }
    }
