"""Sample queries for testing elastic search"""

smartseq2_paired_ends_query = \
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
